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

    def test_signed_session_round_trip_validates(self):
        with mock.patch.dict(
            os.environ,
            {
                "AP_ADMIN_USERNAME": "apadmin",
                "AP_ADMIN_PASSWORD": "secret-pass",
                "AP_SESSION_SECRET": "session-secret",
            },
            clear=False,
        ):
            token = ap_upload_inbox.sign_session("apadmin", 4_102_444_800)
            self.assertTrue(ap_upload_inbox.verify_session(token))

    def test_request_is_admin_authenticated_uses_cookie(self):
        with mock.patch.dict(
            os.environ,
            {
                "AP_ADMIN_USERNAME": "apadmin",
                "AP_ADMIN_PASSWORD": "secret-pass",
                "AP_SESSION_SECRET": "session-secret",
            },
            clear=False,
        ):
            token = ap_upload_inbox.sign_session("apadmin", 4_102_444_800)
            environ = {"HTTP_COOKIE": f"{ap_upload_inbox.SESSION_COOKIE_NAME}={token}"}
            self.assertTrue(ap_upload_inbox.request_is_admin_authenticated(environ))

    def test_download_suffix_ignores_query_parameters(self):
        suffix = run_scheduled_audit.download_suffix("https://anata-ops-ap-inbox.onrender.com/latest.csv?token=secret")
        self.assertEqual(suffix, ".csv")

    def test_build_archive_analysis_detects_new_vendor_and_growth(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ap_upload_inbox.ensure_storage(root)
            previous = "reference,date,vendor,amount,account,memo\np1,2026-03-10,QuickBooks,50.00,Bank,Old plan\np2,2026-03-10,Canva,20.00,Bank,Design\n"
            current = "reference,date,vendor,amount,account,memo\nc1,2026-03-17,QuickBooks,120.00,Bank,Expanded plan\nc2,2026-03-17,Snowflake,75.00,Bank,New vendor\n"
            (root / "archive" / "20260310T120000Z_transactions.csv").write_text(previous)
            metadata = ap_upload_inbox.store_upload(root, "transactions.csv", current.encode("utf-8"))
            analysis = ap_upload_inbox.build_archive_analysis(root, metadata, {"known_vendor_keys": set(), "clickup": {}, "qbo": {}})
            self.assertTrue(analysis["available"])
            self.assertEqual(analysis["current_transaction_count"], 2)
            self.assertIn("Snowflake", {item["vendor"] for item in analysis["new_charges"]})
            self.assertEqual(analysis["spend_growth"][0]["vendor"], "QuickBooks")

    def test_build_archive_analysis_suppresses_growth_without_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = ap_upload_inbox.store_upload(
                root,
                "transactions.csv",
                b"reference,date,vendor,amount,account,memo\nc1,2026-03-17,QuickBooks,120.00,Bank,Expanded plan\n",
            )
            analysis = ap_upload_inbox.build_archive_analysis(root, metadata, {"known_vendor_keys": set(), "clickup": {}, "qbo": {}})
            self.assertTrue(analysis["available"])
            self.assertFalse(analysis["baseline_ready"])
            self.assertEqual(analysis["new_charges"][0]["classification"], "NEW_UNMAPPED_VENDOR")
            self.assertEqual(analysis["savings_opportunities"], [])

    def test_build_archive_analysis_suppresses_known_vendor_without_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = ap_upload_inbox.store_upload(
                root,
                "transactions.csv",
                b"reference,date,vendor,amount,account,memo\nc1,2026-03-17,QuickBooks,120.00,Bank,Expanded plan\n",
            )
            analysis = ap_upload_inbox.build_archive_analysis(
                root,
                metadata,
                {
                    "known_vendor_keys": {ap_upload_inbox.ap_audit.normalize_key("QuickBooks")},
                    "clickup": {"connected": True},
                    "qbo": {"connected": False},
                },
            )
            self.assertEqual(analysis["new_charges"], [])


if __name__ == "__main__":
    unittest.main()
