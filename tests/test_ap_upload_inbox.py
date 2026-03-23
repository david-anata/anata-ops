import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ap_upload_inbox
from scripts import run_scheduled_audit


class ApUploadInboxTests(unittest.TestCase):
    def test_store_upload_updates_latest_and_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = ap_upload_inbox.store_upload(root, "../ExportedTransactions (03.23.26).csv", b"a,b\n1,2\n")
            self.assertTrue(metadata["original_filename"].endswith(".csv"))
            self.assertNotIn("/", metadata["original_filename"])
            self.assertTrue((root / "latest.csv").exists())
            self.assertEqual((root / "latest.csv").read_text(), "a,b\n1,2\n")
            saved_metadata = json.loads((root / "latest.json").read_text())
            self.assertEqual(saved_metadata["stored_filename"], metadata["stored_filename"])
            self.assertTrue((root / "archive" / metadata["stored_filename"]).exists())

    def test_request_token_prefers_query_then_header(self):
        self.assertEqual(
            ap_upload_inbox.request_token({"QUERY_STRING": "token=query-secret"}),
            "query-secret",
        )
        self.assertEqual(
            ap_upload_inbox.request_token({"QUERY_STRING": "", "HTTP_AUTHORIZATION": "Bearer header-secret"}),
            "header-secret",
        )

    def test_token_validation_allows_open_mode_and_protected_mode(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            self.assertTrue(ap_upload_inbox.token_is_valid(""))
        with mock.patch.dict(os.environ, {"AP_UPLOAD_TOKEN": "expected"}, clear=False):
            self.assertTrue(ap_upload_inbox.token_is_valid("expected"))
            self.assertFalse(ap_upload_inbox.token_is_valid("wrong"))

    def test_download_suffix_ignores_query_parameters(self):
        suffix = run_scheduled_audit.download_suffix("https://anata-ops-ap-inbox.onrender.com/latest.csv?token=secret")
        self.assertEqual(suffix, ".csv")


if __name__ == "__main__":
    unittest.main()
