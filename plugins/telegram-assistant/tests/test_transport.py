from __future__ import annotations

import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from typing import Any

from afc_telegram_assistant import TelegramAPIError, TelegramBotAPI, split_text


class _Response:
    def __init__(self, value: object) -> None:
        self.payload = json.dumps({"ok": True, "result": value}).encode("utf-8")

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, amount: int = -1) -> bytes:
        return self.payload if amount < 0 else self.payload[:amount]


class TelegramTransportTests(unittest.TestCase):
    def test_json_request_never_places_token_in_error(self) -> None:
        token = "123456:test-secret"

        def fail(*_: Any, **__: Any) -> None:
            raise urllib.error.URLError(f"offline {token}")

        api = TelegramBotAPI(token, opener=fail)
        with self.assertRaises(TelegramAPIError) as caught:
            api.call("getMe")
        self.assertNotIn(token, str(caught.exception))
        self.assertIn("[REDACTED]", str(caught.exception))

    def test_get_updates_uses_bounded_long_poll_payload(self) -> None:
        captured: list[tuple[str, bytes, float]] = []

        def open_request(request: Any, *, timeout: float) -> _Response:
            captured.append((request.full_url, request.data, timeout))
            return _Response([{"update_id": 3}, "invalid"])

        api = TelegramBotAPI("123456:test", opener=open_request)
        updates = api.get_updates(offset=3, timeout=7, limit=20)

        self.assertEqual(updates, [{"update_id": 3}])
        payload = json.loads(captured[0][1])
        self.assertEqual(payload["offset"], 3)
        self.assertEqual(payload["limit"], 20)
        self.assertEqual(captured[0][2], 17)

    def test_download_rejects_unsafe_paths_before_network_access(self) -> None:
        api = TelegramBotAPI("123456:test", opener=lambda *_args, **_kwargs: None)
        with self.assertRaisesRegex(TelegramAPIError, "unsafe file path"):
            api.download_file("../token", max_bytes=100)

    def test_streaming_upload_is_size_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "video.mp4"
            path.write_bytes(b"12345")
            api = TelegramBotAPI("123456:test", max_upload_bytes=4)
            with self.assertRaisesRegex(TelegramAPIError, "configured Telegram limit"):
                api.call(
                    "sendVideo",
                    {"chat_id": 1, "_upload": {"field": "video", "path": str(path)}},
                )

    def test_split_text_preserves_content_across_chunks(self) -> None:
        value = "alpha beta gamma"
        chunks = split_text(value, limit=8)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(" ".join(chunks), value)


if __name__ == "__main__":
    unittest.main()
