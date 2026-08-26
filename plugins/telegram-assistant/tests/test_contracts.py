from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from afc_telegram_assistant import AssistantAttachment, AssistantTurnResult


class AssistantContractTests(unittest.TestCase):
    def test_turn_result_requires_and_preserves_runtime_session_identity(self) -> None:
        result = AssistantTurnResult(
            runtime_session_id="session-1",
            response="Done",
            trace_path="/tmp/trace.json",
            runtime_id="test-runtime",
        )
        self.assertEqual(result.thread_id, "session-1")
        self.assertEqual(result.runtime_id, "test-runtime")
        with self.assertRaises(ValueError):
            AssistantTurnResult(response="missing identity")

    def test_attachment_prompt_record_is_bounded_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            attachment = AssistantAttachment(
                id="attachment-1",
                kind="image",
                original_name="reference.png",
                media_type="image/png",
                size_bytes=8,
                sha256="0" * 64,
                path=Path(directory) / "reference.png",
            )
            record = attachment.prompt_record()
        self.assertEqual(record["kind"], "image")
        self.assertNotIn("path", record)
        self.assertNotIn(str(attachment.path), str(record))


if __name__ == "__main__":
    unittest.main()
