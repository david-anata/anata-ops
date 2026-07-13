import io
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import ap_upload_inbox


def call_app(path, *, environ_overrides=None, local_bypass=False):
    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "SERVER_NAME": "testserver",
        "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(b""),
        "wsgi.errors": io.StringIO(),
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "CONTENT_LENGTH": "0",
    }
    if environ_overrides:
        environ.update(environ_overrides)

    captured = {}

    def start_response(status, headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = dict(headers)

    bypass_value = "true" if local_bypass else ""
    with mock.patch.dict(os.environ, {"ANATA_ALLOW_UNAUTHENTICATED_LOCAL": bypass_value}, clear=False):
        body = b"".join(ap_upload_inbox.app(environ, start_response))
    return captured["status"], captured["headers"], body


class FinanceDashboardTests(unittest.TestCase):
    def test_root_page_still_renders_when_qbo_rule_enrichment_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {"AP_UPLOAD_STORAGE_DIR": str(Path(tmpdir))}
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                ap_upload_inbox.qbo_client,
                "enrich_rules_with_qbo",
                side_effect=RuntimeError("QBO token refresh failed"),
            ):
                status, headers, body = call_app("/", local_bypass=True)

        self.assertEqual(status, "200 OK")
        rendered = body.decode("utf-8")
        self.assertIn("Cash And Bills", rendered)
        self.assertIn("Cash Unavailable", rendered)

    def test_build_bank_snapshot_uses_latest_balance_on_same_day(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = ap_upload_inbox.store_upload(
                root,
                "bank.csv",
                (
                    "date,description,amount,balance\n"
                    "2026-07-13,Opening balance,0,1000.00\n"
                    "2026-07-13,Vendor A,-200.00,800.00\n"
                    "2026-07-12,Vendor B,-50.00,1050.00\n"
                ).encode("utf-8"),
            )

            snapshot = ap_upload_inbox.build_bank_snapshot(root, metadata)

            self.assertTrue(snapshot["available"])
            self.assertEqual(snapshot["current_cash"], 800.0)
            self.assertEqual(snapshot["recent_outflows"][0]["description"], "Vendor A")

    def test_build_forecast_snapshot_only_subtracts_bills_within_30_days(self):
        bills_snapshot = {
            "available": True,
            "as_of_date": date(2026, 7, 13),
            "items": [
                {"vendor": "Rent", "remaining_balance": 3000.0, "due_date": date(2026, 7, 15)},
                {"vendor": "Insurance", "remaining_balance": 500.0, "due_date": date(2026, 8, 10)},
                {"vendor": "Annual Fee", "remaining_balance": 1200.0, "due_date": date(2026, 8, 20)},
            ],
        }

        snapshot = ap_upload_inbox.build_forecast_snapshot(10000.0, bills_snapshot, date(2026, 7, 13))

        self.assertTrue(snapshot["available"])
        self.assertEqual([point["balance"] for point in snapshot["points"]], [10000.0, 7000.0, 6500.0])
        self.assertEqual(snapshot["low_point"], {"date": date(2026, 8, 10), "balance": 6500.0})

    def test_root_page_renders_one_page_finance_contract(self):
        finance_snapshot = {
            "bank": {
                "available": True,
                "current_cash": 12500.0,
                "uploaded_at": "2026-07-13T12:00:00Z",
                "recent_outflows": [
                    {
                        "date": date(2026, 7, 13),
                        "description": "Vendor A",
                        "amount": -200.0,
                        "balance": 12500.0,
                    }
                ],
            },
            "bills": {
                "available": True,
                "items": [
                    {
                        "level": "HIGH",
                        "vendor": "Rent",
                        "remaining_balance": 3000.0,
                        "due_date": date(2026, 7, 15),
                        "ap_state": "Scheduled",
                        "days_until_due": 2,
                    }
                ],
                "due_in_14_days": 3000.0,
                "overdue_total": 0.0,
                "overdue_count": 0,
            },
            "forecast": {
                "available": True,
                "points": [
                    {"label": "Today", "date": date(2026, 7, 13), "balance": 12500.0},
                    {"label": "Rent", "date": date(2026, 7, 15), "balance": 9500.0},
                ],
                "low_point": {"date": date(2026, 7, 15), "balance": 9500.0},
            },
            "systems": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {"AP_UPLOAD_STORAGE_DIR": str(Path(tmpdir))}
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                ap_upload_inbox, "build_finance_page_snapshot", return_value=finance_snapshot
            ):
                status, headers, body = call_app("/", local_bypass=True)

        self.assertEqual(status, "200 OK")
        rendered = body.decode("utf-8")
        self.assertIn("Cash And Bills", rendered)
        self.assertIn("Cash In Bank", rendered)
        self.assertIn("Next Bills Due", rendered)
        self.assertIn("Projected Cash Balance", rendered)
        self.assertIn("Recent Posted Outflows", rendered)
        self.assertIn("Trust Panel", rendered)
        self.assertIn("$12,500.00", rendered)
        self.assertIn("Rent", rendered)
