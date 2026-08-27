from __future__ import annotations

import json
import mimetypes
import re
import secrets
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol
from urllib import error as urllib_error
from urllib import request as urllib_request


class TelegramAPIError(RuntimeError):
    """A redacted Telegram Bot API or file-transfer failure."""


class OutboundImage(Protocol):
    filename: str
    content: bytes
    media_type: str


class _MultipartBody:
    """Replayable multipart iterator that does not load a local file into memory."""

    def __init__(self, path: Path, prefix: bytes, suffix: bytes) -> None:
        self.path = path
        self.prefix = prefix
        self.suffix = suffix
        self.content_length = len(prefix) + path.stat().st_size + len(suffix)

    def __iter__(self) -> Iterator[bytes]:
        yield self.prefix
        with self.path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                yield chunk
        yield self.suffix


class TelegramBotAPI:
    """Dependency-free Bot API transport with bounded, redacted file handling.

    Authorization, update persistence, command policy, and assistant execution are
    deliberately supplied by the host workflow rather than this transport.
    """

    def __init__(
        self,
        token: str,
        *,
        api_origin: str = "https://api.telegram.org",
        base_url: str | None = None,
        opener: Callable[..., Any] | None = None,
        timeout: float = 35.0,
        upload_timeout: float = 600.0,
        max_upload_bytes: int = 50_000_000,
        user_agent: str = "afc-telegram-assistant/0.2",
    ) -> None:
        if not token:
            raise ValueError("Telegram token is required")
        if max_upload_bytes < 1:
            raise ValueError("Telegram max_upload_bytes must be positive")
        origin = (base_url or api_origin).rstrip("/")
        self._token = token
        self._opener = opener or urllib_request.urlopen
        self._endpoint = f"{origin}/bot{token}/"
        self._file_endpoint = f"{origin}/file/bot{token}/"
        self.timeout = timeout
        self.upload_timeout = upload_timeout
        self.max_upload_bytes = max_upload_bytes
        self.user_agent = user_agent

    def call(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        values = dict(payload or {})
        upload = values.pop("_upload", None)
        if upload is not None:
            if not isinstance(upload, Mapping):
                raise TelegramAPIError("Telegram upload descriptor must be an object")
            body, content_type = self._streaming_multipart(values, upload)
            request_timeout = self.upload_timeout if timeout is None else timeout
            content_length = body.content_length
        else:
            body = json.dumps(values, ensure_ascii=False).encode("utf-8")
            content_type = "application/json"
            request_timeout = self.timeout if timeout is None else timeout
            content_length = len(body)
        return self._open_api_request(
            method,
            body,
            content_type=content_type,
            content_length=content_length,
            timeout=request_timeout,
        )

    def request(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout: float = 15,
    ) -> Any:
        """Compatibility name used by the original ProductGraph gateway."""
        return self.call(method, payload, timeout=timeout)

    def get_me(self) -> Mapping[str, Any]:
        result = self.request("getMe")
        if not isinstance(result, dict):
            raise TelegramAPIError("Telegram API getMe returned an invalid result")
        return result

    def get_webhook_info(self) -> Mapping[str, Any]:
        result = self.request("getWebhookInfo")
        if not isinstance(result, dict):
            raise TelegramAPIError("Telegram API getWebhookInfo returned an invalid result")
        return result

    def delete_webhook(self) -> None:
        self.request("deleteWebhook", {"drop_pending_updates": False})

    def get_updates(
        self,
        offset: int | None = None,
        timeout: int = 25,
        *,
        allowed_updates: Sequence[str] = ("message", "callback_query"),
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": timeout,
            "allowed_updates": list(allowed_updates),
        }
        if limit is not None:
            payload["limit"] = limit
        if offset is not None:
            payload["offset"] = offset
        result = self.call("getUpdates", payload, timeout=timeout + 10)
        if not isinstance(result, list):
            raise TelegramAPIError("Telegram API getUpdates returned an invalid result")
        return [item for item in result if isinstance(item, dict)]

    def get_file(self, file_id: str) -> Mapping[str, Any]:
        result = self.request("getFile", {"file_id": file_id})
        if not isinstance(result, dict) or not isinstance(result.get("file_path"), str):
            raise TelegramAPIError("Telegram API getFile returned an invalid result")
        return result

    def download_file(
        self,
        file_path: str,
        *,
        max_bytes: int = 10 * 1024 * 1024,
    ) -> bytes:
        path = Path(file_path)
        if (
            not file_path
            or path.is_absolute()
            or ".." in path.parts
            or not re.fullmatch(r"[A-Za-z0-9_./-]+", file_path)
        ):
            raise TelegramAPIError("Telegram API returned an unsafe file path")
        request = urllib_request.Request(
            self._file_endpoint + file_path,
            headers={"User-Agent": self.user_agent},
            method="GET",
        )
        try:
            with self._opener(request, timeout=self.timeout) as response:
                content = response.read(max_bytes + 1)
        except urllib_error.HTTPError as exc:
            raise TelegramAPIError(
                f"Telegram file download returned HTTP {exc.code}"
            ) from None
        except (urllib_error.URLError, TimeoutError, OSError) as exc:
            safe = _redact(str(getattr(exc, "reason", exc)), self._token)
            raise TelegramAPIError(
                f"Telegram file download is unavailable: {safe}"
            ) from None
        if len(content) > max_bytes:
            raise TelegramAPIError(
                f"Telegram file exceeds the configured {max_bytes}-byte limit"
            )
        return content

    def send_chat_action(self, chat_id: int, action: str = "typing") -> None:
        self.request("sendChatAction", {"chat_id": chat_id, "action": action})

    def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        reply_markup: Mapping[str, Any] | None = None,
    ) -> list[Mapping[str, Any]]:
        sent: list[Mapping[str, Any]] = []
        for index, chunk in enumerate(split_text(text)):
            payload: dict[str, Any] = {
                "chat_id": chat_id,
                "text": chunk,
                "link_preview_options": {"is_disabled": True},
            }
            if reply_to_message_id is not None:
                payload["reply_parameters"] = {
                    "message_id": reply_to_message_id,
                    "allow_sending_without_reply": True,
                }
            if reply_markup is not None and index == 0:
                payload["reply_markup"] = dict(reply_markup)
            result = self.request("sendMessage", payload)
            if isinstance(result, dict):
                sent.append(result)
        return sent

    def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        reply_markup: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any] | None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text[:4096],
            "link_preview_options": {"is_disabled": True},
        }
        if reply_markup is not None:
            payload["reply_markup"] = dict(reply_markup)
        result = self.request("editMessageText", payload)
        return result if isinstance(result, dict) else None

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> None:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text[:200]
        self.request("answerCallbackQuery", payload)

    def send_document(
        self,
        chat_id: int,
        filename: str,
        content: bytes,
        *,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        media_type: str = "text/markdown; charset=utf-8",
    ) -> Mapping[str, Any] | None:
        fields = _media_fields(chat_id, caption, reply_to_message_id)
        result = self._multipart_request(
            "sendDocument",
            fields,
            [("document", filename, content, media_type)],
        )
        return result if isinstance(result, dict) else None

    def send_photo(
        self,
        chat_id: int,
        filename: str,
        content: bytes,
        media_type: str,
        *,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
    ) -> Mapping[str, Any] | None:
        fields = _media_fields(chat_id, caption, reply_to_message_id)
        result = self._multipart_request(
            "sendPhoto",
            fields,
            [("photo", filename, content, media_type)],
        )
        return result if isinstance(result, dict) else None

    def send_media_group(
        self,
        chat_id: int,
        images: Sequence[OutboundImage],
        *,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
    ) -> list[Mapping[str, Any]]:
        if not 2 <= len(images) <= 10:
            raise TelegramAPIError("Telegram media groups require between 2 and 10 images")
        media: list[dict[str, str]] = []
        files: list[tuple[str, str, bytes, str]] = []
        for index, image in enumerate(images):
            field = f"image{index}"
            item = {"type": "photo", "media": f"attach://{field}"}
            if index == 0 and caption:
                item["caption"] = caption[:1024]
            media.append(item)
            files.append((field, image.filename, image.content, image.media_type))
        fields = _media_fields(chat_id, None, reply_to_message_id)
        fields["media"] = json.dumps(media, ensure_ascii=False)
        result = self._multipart_request("sendMediaGroup", fields, files)
        if not isinstance(result, list):
            raise TelegramAPIError("Telegram API sendMediaGroup returned an invalid result")
        return [item for item in result if isinstance(item, dict)]

    def _streaming_multipart(
        self,
        payload: Mapping[str, Any],
        upload: Mapping[str, Any],
    ) -> tuple[_MultipartBody, str]:
        field = str(upload.get("field") or "")
        path = Path(str(upload.get("path") or ""))
        if not field or not path.is_file():
            raise TelegramAPIError(f"Upload file does not exist: {path}")
        size = path.stat().st_size
        if size > self.max_upload_bytes:
            raise TelegramAPIError(
                f"Upload file is {size} bytes; configured Telegram limit is "
                f"{self.max_upload_bytes} bytes"
            )
        boundary = f"----afc-{secrets.token_hex(12)}"
        pieces = _multipart_fields(boundary, payload)
        mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        filename = _safe_filename(path.name)
        pieces.extend(
            [
                f"--{boundary}\r\n".encode(),
                (
                    f'Content-Disposition: form-data; name="{field}"; '
                    f'filename="{filename}"\r\n'
                ).encode(),
                f"Content-Type: {mime}\r\n\r\n".encode(),
            ]
        )
        suffix = b"\r\n" + f"--{boundary}--\r\n".encode()
        return (
            _MultipartBody(path, b"".join(pieces), suffix),
            f"multipart/form-data; boundary={boundary}",
        )

    def _multipart_request(
        self,
        method: str,
        fields: Mapping[str, Any],
        files: Sequence[tuple[str, str, bytes, str]],
    ) -> Any:
        boundary = f"----afc-{secrets.token_hex(12)}"
        pieces = _multipart_fields(boundary, fields)
        for field, filename, content, media_type in files:
            if len(content) > self.max_upload_bytes:
                raise TelegramAPIError(
                    f"Upload file is {len(content)} bytes; configured Telegram limit is "
                    f"{self.max_upload_bytes} bytes"
                )
            pieces.extend(
                [
                    f"--{boundary}\r\n".encode(),
                    (
                        f'Content-Disposition: form-data; name="{field}"; '
                        f'filename="{_safe_filename(filename)}"\r\n'
                    ).encode(),
                    f"Content-Type: {media_type}\r\n\r\n".encode(),
                    content,
                    b"\r\n",
                ]
            )
        pieces.append(f"--{boundary}--\r\n".encode())
        body = b"".join(pieces)
        return self._open_api_request(
            method,
            body,
            content_type=f"multipart/form-data; boundary={boundary}",
            content_length=len(body),
            timeout=self.upload_timeout,
        )

    def _open_api_request(
        self,
        method: str,
        body: bytes | Iterable[bytes],
        *,
        content_type: str,
        content_length: int,
        timeout: float,
    ) -> Any:
        request = urllib_request.Request(
            self._endpoint + method,
            data=body,
            headers={
                "Content-Type": content_type,
                "Content-Length": str(content_length),
                "User-Agent": self.user_agent,
            },
            method="POST",
        )
        try:
            with self._opener(request, timeout=timeout) as response:
                raw = response.read()
        except urllib_error.HTTPError as exc:
            description = _redact(_error_description(exc.read()), self._token)
            raise TelegramAPIError(
                f"Telegram API {method} returned HTTP {exc.code}: {description}"
            ) from None
        except (urllib_error.URLError, TimeoutError, OSError) as exc:
            safe = _redact(str(getattr(exc, "reason", exc)), self._token)
            raise TelegramAPIError(
                f"Telegram API {method} is unavailable: {safe}"
            ) from None
        try:
            envelope = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise TelegramAPIError(
                f"Telegram API {method} returned invalid JSON"
            ) from exc
        if not isinstance(envelope, dict) or envelope.get("ok") is not True:
            description = _redact(
                str(envelope.get("description", "request failed")), self._token
            )
            raise TelegramAPIError(
                f"Telegram API {method} rejected the request: {description}"
            )
        return envelope.get("result")


def split_text(text: str, limit: int = 3800) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        boundary = remaining.rfind("\n", 0, limit + 1)
        if boundary < limit // 2:
            boundary = remaining.rfind(" ", 0, limit + 1)
        if boundary < limit // 2:
            boundary = limit
        chunks.append(remaining[:boundary].rstrip())
        remaining = remaining[boundary:].lstrip()
    return chunks


def _media_fields(
    chat_id: int,
    caption: str | None,
    reply_to_message_id: int | None,
) -> dict[str, str]:
    fields = {"chat_id": str(chat_id)}
    if caption:
        fields["caption"] = caption[:1024]
    if reply_to_message_id is not None:
        fields["reply_parameters"] = json.dumps(
            {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        )
    return fields


def _multipart_fields(boundary: str, fields: Mapping[str, Any]) -> list[bytes]:
    pieces: list[bytes] = []
    for name, value in fields.items():
        encoded = (
            json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else str(value)
        ).encode("utf-8")
        pieces.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(),
                encoded,
                b"\r\n",
            ]
        )
    return pieces


def _safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", value) or "attachment"


def _error_description(raw: bytes) -> str:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return "request failed"
    if isinstance(value, dict):
        return str(value.get("description") or "request failed")
    return "request failed"


def _redact(value: str, secret: str) -> str:
    return value.replace(secret, "[REDACTED]") if secret else value
