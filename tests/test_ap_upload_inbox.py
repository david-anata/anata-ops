import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ap_upload_inbox
import hubspot_sales
import support_agent
from scripts import run_scheduled_audit


ADMIN_ENV = {
    "AP_ADMIN_USERNAME": "apadmin",
    "AP_ADMIN_PASSWORD": "secret-pass",
    "AP_SESSION_SECRET": "session-secret",
}


def admin_cookie_environ():
    token = ap_upload_inbox.sign_session(ADMIN_ENV["AP_ADMIN_USERNAME"], 4_102_444_800)
    return {"HTTP_COOKIE": f"{ap_upload_inbox.SESSION_COOKIE_NAME}={token}"}


def call_app(path, *, method="GET", body=b"", content_type="", accept="", environ_overrides=None, local_bypass=False):
    path_info, _, query_string = path.partition("?")
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path_info,
        "QUERY_STRING": query_string,
        "SERVER_NAME": "testserver",
        "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(body),
        "wsgi.errors": io.StringIO(),
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "CONTENT_LENGTH": str(len(body)),
    }
    if content_type:
        environ["CONTENT_TYPE"] = content_type
    if accept:
        environ["HTTP_ACCEPT"] = accept
    if environ_overrides:
        environ.update(environ_overrides)

    captured = {}

    def start_response(status, headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = dict(headers)

    bypass_value = "true" if local_bypass else ""
    with mock.patch.dict(os.environ, {"ANATA_ALLOW_UNAUTHENTICATED_LOCAL": bypass_value}, clear=False):
        result = b"".join(ap_upload_inbox.app(environ, start_response))
    return captured["status"], captured["headers"], result


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

    def test_token_validation_fails_closed_when_unconfigured(self):
        with mock.patch.dict(os.environ, {"AP_UPLOAD_TOKEN": ""}, clear=False):
            self.assertFalse(ap_upload_inbox.token_is_valid(""))
            self.assertFalse(ap_upload_inbox.token_is_valid("anything"))
        with mock.patch.dict(os.environ, {"AP_UPLOAD_TOKEN": "expected"}, clear=False):
            self.assertTrue(ap_upload_inbox.token_is_valid("expected"))
            self.assertFalse(ap_upload_inbox.token_is_valid("wrong"))

    def test_admin_readonly_token_allows_get_smoke_checks_without_admin_login(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = {
                "ANATA_ADMIN_READONLY_TOKEN": "readonly-secret",
                "AP_ADMIN_USERNAME": "",
                "AP_ADMIN_PASSWORD": "",
                "AP_SESSION_SECRET": "",
                "AP_UPLOAD_STORAGE_DIR": str(root / "uploads"),
                "WEBSITE_OPS_DIR": str(root / "website-ops"),
                "SUPPORT_AGENT_REPORTS_DIR": str(root / "support-reports"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                status, headers, body = call_app(
                    "/website-ops/queue",
                    environ_overrides={"HTTP_AUTHORIZATION": "Bearer readonly-secret"},
                )
            self.assertEqual(status, "200 OK")
            self.assertIn(b"Open Work Queue", body)

    def test_admin_readonly_token_does_not_authorize_post_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = {
                **ADMIN_ENV,
                "ANATA_ADMIN_READONLY_TOKEN": "readonly-secret",
                "AP_UPLOAD_STORAGE_DIR": str(root / "uploads"),
                "WEBSITE_OPS_DIR": str(root / "website-ops"),
                "SUPPORT_AGENT_REPORTS_DIR": str(root / "support-reports"),
            }
            body = b"category=UX&summary=Smoke"
            with mock.patch.dict(os.environ, env, clear=False):
                status, headers, response_body = call_app(
                    "/website-ops/feedback",
                    method="POST",
                    body=body,
                    content_type="application/x-www-form-urlencoded",
                    environ_overrides={"HTTP_AUTHORIZATION": "Bearer readonly-secret"},
                )
            self.assertEqual(status, "303 See Other")
            self.assertEqual(headers["Location"], "/?status=unauthorized")

    def test_signed_session_round_trip_validates(self):
        with mock.patch.dict(
            os.environ,
            ADMIN_ENV,
            clear=False,
        ):
            token = ap_upload_inbox.sign_session("apadmin", 4_102_444_800)
            self.assertTrue(ap_upload_inbox.verify_session(token))

    def test_request_is_admin_authenticated_uses_cookie(self):
        with mock.patch.dict(
            os.environ,
            ADMIN_ENV,
            clear=False,
        ):
            self.assertTrue(ap_upload_inbox.request_is_admin_authenticated(admin_cookie_environ()))

    def test_admin_auth_fails_closed_without_credentials_unless_dev_bypass_is_explicit(self):
        missing_env = {
            "AP_ADMIN_USERNAME": "",
            "AP_ADMIN_PASSWORD": "",
            "AP_SESSION_SECRET": "",
            "ANATA_ALLOW_UNAUTHENTICATED_LOCAL": "",
        }
        with mock.patch.dict(os.environ, missing_env, clear=False):
            self.assertFalse(ap_upload_inbox.request_is_admin_authenticated({}))
            self.assertFalse(ap_upload_inbox.verify_session(""))
        with mock.patch.dict(os.environ, {**missing_env, "ANATA_ALLOW_UNAUTHENTICATED_LOCAL": "true"}, clear=False):
            self.assertTrue(ap_upload_inbox.request_is_admin_authenticated({}))

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

    def test_website_ops_report_routes_render_library_and_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ops_root = Path(tmpdir) / "website-ops"
            report_dir = ops_root / "reports" / "daily"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / "2026-03-25-demo-report.md"
            report_path.write_text(
                "# Demo Report\n\nDate: 2026-03-25\nScope: production\nMethod: manual review\n\n## Executive Summary\n\n- One issue\n",
            )
            with mock.patch.dict(os.environ, {**ADMIN_ENV, "WEBSITE_OPS_DIR": str(ops_root)}, clear=False):
                auth = admin_cookie_environ()
                status, headers, body = call_app("/website-ops/reports/", environ_overrides=auth)
                self.assertEqual(status, "200 OK")
                rendered = body.decode("utf-8")
                self.assertIn("Report Library", rendered)
                self.assertIn("Daily", rendered)

                status, headers, body = call_app("/website-ops/reports/daily/2026-03-25-demo-report", environ_overrides=auth)
                self.assertEqual(status, "200 OK")
                rendered = body.decode("utf-8")
                self.assertIn("Demo Report", rendered)
                self.assertIn("<h1>Demo Report</h1>", rendered)
                self.assertIn("<li>One issue</li>", rendered)

                status, headers, body = call_app("/website-ops/reports/latest", environ_overrides=auth)
                self.assertEqual(status, "303 See Other")
                self.assertEqual(headers["Location"], "/website-ops/reports/daily/2026-03-25-demo-report")

    def test_website_ops_feedback_submission_writes_structured_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ops_root = Path(tmpdir) / "website-ops"
            payload = json.dumps(
                {
                    "category": "SEO",
                    "priority": "High",
                    "page_url": "https://example.com/services/fulfillment/",
                    "summary": "Fix the fulfillment hero heading",
                    "details": "The page H1 is too weak.",
                    "desired_outcome": "Clearer commercial intent",
                    "recommended_fix": "Rewrite the hero H1",
                    "reporter_name": "Test User",
                    "reporter_email": "test@example.com",
                }
            ).encode("utf-8")
            with mock.patch.dict(os.environ, {**ADMIN_ENV, "WEBSITE_OPS_DIR": str(ops_root)}, clear=False):
                auth = admin_cookie_environ()
                status, headers, body = call_app(
                    "/website-ops/feedback",
                    method="POST",
                    body=payload,
                    content_type="application/json",
                    accept="application/json",
                    environ_overrides=auth,
                )
                self.assertEqual(status, "201 Created")
                response = json.loads(body.decode("utf-8"))
                self.assertTrue(response["ok"])
                self.assertEqual(response["record"]["category"], "SEO")
                inbox = ops_root / "feedback" / "inbox"
                files = list(inbox.glob("*.json"))
                self.assertEqual(len(files), 1)
                saved = json.loads(files[0].read_text())
                self.assertEqual(saved["summary"], "Fix the fulfillment hero heading")
                self.assertEqual(saved["page_url"], "https://example.com/services/fulfillment/")
                self.assertEqual(saved["status"], "new")

                status, headers, body = call_app("/website-ops/queue", environ_overrides=auth)
                self.assertEqual(status, "200 OK")
                rendered = body.decode("utf-8")
                self.assertIn("Open Work Queue", rendered)
                self.assertIn("Fix the fulfillment hero heading", rendered)

                saved_id = files[0].stem
                review_body = b"status=approved&reviewer_name=SEO+Lead&review_notes=Looks+good&action_type=replace_primary_heading&action_value=Sharper+Shipping+Headline&target_post_id=5540"
                status, headers, body = call_app(
                    f"/website-ops/feedback/submissions/{saved_id}/status",
                    method="POST",
                    body=review_body,
                    content_type="application/x-www-form-urlencoded",
                    accept="application/json",
                    environ_overrides=auth,
                )
                self.assertEqual(status, "200 OK")
                response = json.loads(body.decode("utf-8"))
                self.assertTrue(response["ok"])
                self.assertEqual(response["record"]["status"], "approved")
                updated = json.loads(files[0].read_text())
                self.assertEqual(updated["status"], "approved")
                self.assertEqual(updated["reviewer_name"], "SEO Lead")
                self.assertEqual(updated["action_type"], "replace_primary_heading")
                self.assertEqual(updated["action_value"], "Sharper Shipping Headline")
                self.assertEqual(updated["target_post_id"], "5540")

    def test_protected_routes_return_503_when_admin_auth_is_not_configured(self):
        missing_env = {
            "AP_ADMIN_USERNAME": "",
            "AP_ADMIN_PASSWORD": "",
            "AP_SESSION_SECRET": "",
            "ANATA_ALLOW_UNAUTHENTICATED_LOCAL": "",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {**missing_env, "WEBSITE_OPS_DIR": str(Path(tmpdir) / "website-ops")}, clear=False):
                for path in ("/", "/website-ops/queue", "/admin/sales/"):
                    status, headers, body = call_app(path)
                    self.assertEqual(status, "503 Service Unavailable")
                    rendered = body.decode("utf-8")
                    self.assertIn("Authentication is not configured", rendered)
                    self.assertIn("AP_ADMIN_USERNAME", rendered)
                    self.assertIn("AP_ADMIN_PASSWORD", rendered)

    def test_website_ops_and_admin_routes_require_admin_session(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {**ADMIN_ENV, "WEBSITE_OPS_DIR": str(Path(tmpdir) / "website-ops")}, clear=False):
                for path in ("/website-ops/queue", "/website-ops/reports/", "/website-ops/backups/", "/admin/sales/"):
                    status, headers, body = call_app(path)
                    self.assertEqual(status, "303 See Other")
                    self.assertEqual(headers["Location"], "/?status=unauthorized")

                status, headers, body = call_app(
                    "/website-ops/feedback/submissions/example/status",
                    method="POST",
                    body=b"status=approved",
                    content_type="application/x-www-form-urlencoded",
                    accept="application/json",
                )
                self.assertEqual(status, "401 Unauthorized")
                self.assertEqual(json.loads(body.decode("utf-8"))["error"], "unauthorized")

    def test_latest_csv_allows_configured_machine_token_only_and_admin_browser_access(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ap_upload_inbox.ensure_storage(root)
            ap_upload_inbox.store_upload(root, "transactions.csv", b"a,b\n1,2\n")
            env = {**ADMIN_ENV, "AP_UPLOAD_STORAGE_DIR": str(root), "AP_UPLOAD_TOKEN": "machine-secret"}
            with mock.patch.dict(os.environ, env, clear=False):
                status, headers, body = call_app("/latest.csv?token=machine-secret")
                self.assertEqual(status, "200 OK")
                self.assertEqual(body, b"a,b\n1,2\n")

                status, headers, body = call_app("/latest.csv?token=wrong")
                self.assertEqual(status, "401 Unauthorized")

                status, headers, body = call_app("/latest.csv")
                self.assertEqual(status, "303 See Other")
                self.assertEqual(headers["Location"], "/?status=unauthorized")

                status, headers, body = call_app("/latest.csv", environ_overrides=admin_cookie_environ())
                self.assertEqual(status, "200 OK")
                self.assertEqual(body, b"a,b\n1,2\n")

    def test_latest_csv_token_mode_fails_closed_when_token_is_unconfigured(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ap_upload_inbox.ensure_storage(root)
            ap_upload_inbox.store_upload(root, "transactions.csv", b"a,b\n1,2\n")
            env = {**ADMIN_ENV, "AP_UPLOAD_STORAGE_DIR": str(root), "AP_UPLOAD_TOKEN": ""}
            with mock.patch.dict(os.environ, env, clear=False):
                status, headers, body = call_app("/latest.csv?token=anything")
                self.assertEqual(status, "401 Unauthorized")

    def test_upload_post_requires_admin_session_before_parsing_body(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {**ADMIN_ENV, "AP_UPLOAD_STORAGE_DIR": str(Path(tmpdir) / "uploads")}
            with mock.patch.dict(os.environ, env, clear=False):
                status, headers, body = call_app(
                    "/upload",
                    method="POST",
                    body=b"not multipart",
                    content_type="text/plain",
                )
                self.assertEqual(status, "303 See Other")
                self.assertEqual(headers["Location"], "/?status=unauthorized")

                status, headers, body = call_app(
                    "/upload",
                    method="POST",
                    body=b"not multipart",
                    content_type="text/plain",
                    environ_overrides=admin_cookie_environ(),
                )
                self.assertEqual(status, "400 Bad Request")
                self.assertIn(b"Could not parse upload form", body)

    def test_support_agent_report_routes_render_dashboard_library_and_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_root = Path(tmpdir) / "support-runs"
            report = {
                "title": "Fulfillment Support Review",
                "generated_at": "2026-03-27T21:42:10-06:00",
                "status": "ready",
                "candidate_count": 1,
                "action_counts": {"clarifying": 1},
                "candidates": [
                    {
                        "brand_name": "Mule Deer Foundation",
                        "channel": "mule-deer-anatafulfillment",
                        "permalink": "https://anatainc.slack.com/archives/C099KMCAQ6A/p1774656761822649",
                        "question_summary": "Need PO verification for received boots.",
                        "identifiers": {"po_numbers": ["PO-108096"], "order_numbers": [], "tracking_numbers": []},
                        "evidence": {"labelogics": {"status": "pending_account_match"}},
                        "recommended_action": {
                            "reply_type": "clarifying",
                            "customer_reply": "Can you send the PO number or shipment reference so I can pull this up?",
                        },
                    }
                ],
            }
            support_agent.write_candidate_report_artifacts(report, output_dir=reports_root)
            with mock.patch.dict(os.environ, {**ADMIN_ENV, "SUPPORT_AGENT_REPORTS_DIR": str(reports_root)}, clear=False):
                auth = admin_cookie_environ()
                status, headers, body = call_app("/admin/fulfillment-cs/", environ_overrides=auth)
                self.assertEqual(status, "200 OK")
                rendered = body.decode("utf-8")
                self.assertIn("Fulfillment CS", rendered)
                self.assertIn("Candidate Preview", rendered)

                status, headers, body = call_app("/admin/fulfillment-cs/reports/", environ_overrides=auth)
                self.assertEqual(status, "200 OK")
                rendered = body.decode("utf-8")
                self.assertIn("Support Review Reports", rendered)
                self.assertIn("Fulfillment Support Review", rendered)

                status, headers, body = call_app("/admin/fulfillment-cs/reports/latest", environ_overrides=auth)
                self.assertEqual(status, "303 See Other")
                self.assertIn("/admin/fulfillment-cs/reports/support-review-", headers["Location"])

                latest_slug = Path(headers["Location"]).name
                status, headers, body = call_app(f"/admin/fulfillment-cs/reports/{latest_slug}", environ_overrides=auth)
                self.assertEqual(status, "200 OK")
                rendered = body.decode("utf-8")
                self.assertIn("Mule Deer Foundation", rendered)
                self.assertIn("Need PO verification for received boots.", rendered)

                for suffix, content_type in (
                    (".json", "application/json; charset=utf-8"),
                    (".html", "text/html; charset=utf-8"),
                    (".md", "text/markdown; charset=utf-8"),
                ):
                    status, headers, body = call_app(f"/admin/fulfillment-cs/reports/{latest_slug}{suffix}", environ_overrides=auth)
                    self.assertEqual(status, "200 OK")
                    self.assertEqual(headers["Content-Type"], content_type)
                    self.assertIn(b"Fulfillment Support Review", body)

                status, headers, body = call_app("/support-agent")
                self.assertEqual(status, "303 See Other")
                self.assertEqual(headers["Location"], "/admin/fulfillment-cs/")

    def test_sales_deal_create_form_renders_required_fields(self):
        with mock.patch.dict(os.environ, ADMIN_ENV, clear=False):
            auth = admin_cookie_environ()
            status, headers, body = call_app("/admin/sales", environ_overrides=auth)
            self.assertEqual(status, "303 See Other")
            self.assertEqual(headers["Location"], "/admin/sales/")

            status, headers, body = call_app("/admin/sales/", environ_overrides=auth)
            self.assertEqual(status, "200 OK")
            self.assertIn("Create a HubSpot deal", body.decode("utf-8"))

            status, headers, body = call_app("/admin/sales/deals/create", environ_overrides=auth)
            self.assertEqual(status, "200 OK")
            rendered = body.decode("utf-8")
            self.assertIn('action="/admin/sales/deals/create"', rendered)
            self.assertIn('method="post"', rendered)
            for field in (
                "dealname",
                "pipeline",
                "dealstage",
                "anata_service_line",
                "anata_lead_source_detail",
                "hubspot_owner_id",
                "company_id",
                "contact_id",
                "amount",
                "closedate",
                "anata_next_step",
            ):
                self.assertIn(f'name="{field}"', rendered)

    def test_sales_deal_create_post_requires_admin_session(self):
        with mock.patch.dict(
            os.environ,
            {
                "AP_ADMIN_USERNAME": "admin",
                "AP_ADMIN_PASSWORD": "secret",
                "AP_SESSION_SECRET": "session-secret",
            },
            clear=False,
        ):
            with mock.patch("hubspot_sales.create_deal") as create_deal:
                status, headers, body = call_app(
                    "/admin/sales/deals/create",
                    method="POST",
                    body=b"dealname=Acme",
                    content_type="application/x-www-form-urlencoded",
                    local_bypass=False,
                )
                self.assertEqual(status, "303 See Other")
                self.assertEqual(headers["Location"], "/?status=unauthorized")
                create_deal.assert_not_called()

    def test_sales_deal_create_validates_required_fields_before_hubspot(self):
        with mock.patch.dict(os.environ, ADMIN_ENV, clear=False):
            auth = admin_cookie_environ()
            with mock.patch("hubspot_sales.create_deal") as create_deal:
                status, headers, body = call_app(
                    "/admin/sales/deals/create",
                    method="POST",
                    body=b"dealname=Acme&pipeline=default&anata_service_line=fulfillment&anata_lead_source_detail=website&hubspot_owner_id=123&company_id=111&contact_id=222",
                    content_type="application/x-www-form-urlencoded",
                    environ_overrides=auth,
                )
                self.assertEqual(status, "400 Bad Request")
                rendered = body.decode("utf-8")
                self.assertIn("Missing required deal property: dealstage", rendered)
                create_deal.assert_not_called()

    def test_sales_deal_create_posts_to_hubspot_and_redirects(self):
        created = {
            "id": "deal-123",
            "hubspot_url": "https://app.hubspot.com/contacts/999/record/0-3/deal-123",
            "properties": {},
        }
        body = (
            b"dealname=Acme+Fulfillment+Opportunity&"
            b"pipeline=default&"
            b"dealstage=appointmentscheduled&"
            b"anata_service_line=fulfillment&"
            b"anata_lead_source_detail=website&"
            b"hubspot_owner_id=12345&"
            b"company_id=111&"
            b"contact_id=222&"
            b"amount=5000&"
            b"closedate=2026-07-31&"
            b"anata_next_step=Send+deck"
        )
        with mock.patch.dict(os.environ, ADMIN_ENV, clear=False):
            auth = admin_cookie_environ()
            with mock.patch("hubspot_sales.create_deal", return_value=created) as create_deal:
                status, headers, response_body = call_app(
                    "/admin/sales/deals/create",
                    method="POST",
                    body=body,
                    content_type="application/x-www-form-urlencoded",
                    environ_overrides=auth,
                )
        self.assertEqual(status, "303 See Other")
        self.assertEqual(headers["Location"], created["hubspot_url"])
        request = create_deal.call_args.args[0]
        self.assertIsInstance(request, hubspot_sales.DealCreateRequest)
        self.assertEqual(request.company_id, "111")
        self.assertEqual(request.contact_id, "222")
        self.assertEqual(request.properties["dealname"], "Acme Fulfillment Opportunity")
        self.assertEqual(request.properties["dealstage"], "appointmentscheduled")
        self.assertEqual(request.properties["amount"], "5000")

    def test_sales_downstream_routes_remain_guarded_until_wired(self):
        with mock.patch.dict(os.environ, ADMIN_ENV, clear=False):
            auth = admin_cookie_environ()
            for path in (
                "/admin/sales/quotes/create",
                "/admin/sales/decks/create",
            ):
                status, headers, body = call_app(path, environ_overrides=auth)
                self.assertEqual(status, "501 Not Implemented")
                rendered = body.decode("utf-8")
                self.assertIn("Commercial Flow Blocked", rendered)
                self.assertIn("No HubSpot action was performed.", rendered)
                self.assertIn(path, rendered)


if __name__ == "__main__":
    unittest.main()
