import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

from scripts import run_fulfillment_support


def base_config(root: Path) -> dict:
    return {
        "agent_name": "Support",
        "timezone": "America/Denver",
        "workspace_root": str(root),
        "paths": {
            "intake": "support/intake",
            "runs": "support/runs",
            "knowledge": "support/knowledge",
            "escalations": "support/escalations",
            "connections_db": "support/knowledge/connections.sqlite3",
            "shopify_accounts": "support/knowledge/shopify_accounts.json",
            "labelogics_accounts": "support/knowledge/labelogics_accounts.json",
        },
        "schedule": {
            "weekday_interval_hours": 2,
            "weekday_days": ["MO", "TU", "WE", "TH", "FR"],
            "weekday_hours": [8, 10, 12, 14, 16, 18],
        },
        "agent_runtime": {
            "enabled": True,
            "mode": "scheduled",
            "queue_channel": "fulfillment-ops",
            "lookback_hours": 6,
            "escalation_slack_user_ids_env": "SUPPORT_ESCALATION_SLACK_USER_IDS",
        },
        "channels": {
            "slack": {"enabled": True, "required_env": ["SLACK_API_BASE_URL", "SLACK_BOT_TOKEN", "SUPPORT_SLACK_CHANNELS"]},
            "gmail": {"enabled": False, "required_env": ["SUPPORT_GMAIL_EXPORT_PATH"]},
        },
        "systems": {
            "labelogics": {
                "enabled": True,
                "required_env": ["LABELOGICS_APP_URL", "LABELOGICS_SANDBOX_URL", "LABELOGICS_KEY", "LABELOGICS_PASSWORD"],
            },
            "shopify": {
                "enabled": False,
                "required_env": ["SHOPIFY_STORE_DOMAIN", "SHOPIFY_ADMIN_API_ACCESS_TOKEN", "SHOPIFY_API_VERSION"],
            },
        },
        "account_matching": {
            "enabled": True,
            "ignored_tokens": ["anata", "fulfillment", "store", "cs"],
            "alias_overrides": {},
        },
        "response_policy": {"default_escalation_owner_env": "SUPPORT_ESCALATION_DEFAULT_OWNER"},
    }


class RunFulfillmentSupportTests(unittest.TestCase):
    def test_main_prepares_directories_and_prints_ready_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "fulfillment_support.json"
            config_path.write_text(json.dumps(base_config(root)))
            buffer = io.StringIO()
            env = {
                "SLACK_API_BASE_URL": "https://slack.com/api",
                "SLACK_BOT_TOKEN": "xoxb-test-token",
                "SUPPORT_SLACK_CHANNELS": "customer-support",
                "LABELOGICS_APP_URL": "https://app.labelogics.com",
                "LABELOGICS_SANDBOX_URL": "https://sandbox.labelogics.com",
                "LABELOGICS_KEY": "key",
                "LABELOGICS_PASSWORD": "password",
                "SUPPORT_ESCALATION_DEFAULT_OWNER": "ops@anata.test",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                with mock.patch("sys.argv", ["run_fulfillment_support.py", "--config", str(config_path)]):
                    with redirect_stdout(buffer):
                        run_fulfillment_support.main()
            rendered = json.loads(buffer.getvalue())
            self.assertEqual(rendered["status"], "ready")
            self.assertEqual(rendered["channels"], ["slack"])
            self.assertEqual(rendered["systems"], ["labelogics"])
            self.assertEqual(rendered["channel_config"]["count"], 1)
            self.assertEqual(rendered["schedule"]["weekday_hours"], [8, 10, 12, 14, 16, 18])
            self.assertIn("connections_db", rendered)
            self.assertTrue((root / "support" / "intake").exists())
            self.assertTrue((root / "support" / "escalations").exists())

    def test_main_exits_non_zero_when_required_env_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = base_config(root)
            config["channels"] = {"gmail": {"enabled": True, "required_env": ["SUPPORT_GMAIL_EXPORT_PATH"]}}
            config["systems"] = {}
            config_path = root / "fulfillment_support.json"
            config_path.write_text(json.dumps(config))
            buffer = io.StringIO()
            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch("sys.argv", ["run_fulfillment_support.py", "--config", str(config_path), "--validate-only"]):
                    with self.assertRaises(SystemExit) as exc:
                        with redirect_stdout(buffer):
                            run_fulfillment_support.main()
            rendered = json.loads(buffer.getvalue())
            self.assertEqual(exc.exception.code, 1)
            self.assertEqual(rendered["status"], "blocked")
            self.assertEqual(rendered["missing_env"]["gmail"], ["SUPPORT_GMAIL_EXPORT_PATH"])

    def test_main_loads_env_file_before_validation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "fulfillment_support.json"
            env_path = root / ".env"
            config_path.write_text(json.dumps(base_config(root)))
            env_path.write_text(
                "\n".join(
                    [
                        "SLACK_API_BASE_URL=https://slack.com/api",
                        "SLACK_BOT_TOKEN=xoxb-test-token",
                        "SUPPORT_SLACK_CHANNELS=customer-support,fulfillment",
                        "LABELOGICS_APP_URL=https://app.labelogics.com",
                        "LABELOGICS_SANDBOX_URL=https://sandbox.labelogics.com",
                        "LABELOGICS_KEY=test-key",
                        "LABELOGICS_PASSWORD=test-password",
                    ]
                )
            )
            buffer = io.StringIO()
            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch(
                    "sys.argv",
                    ["run_fulfillment_support.py", "--config", str(config_path), "--env-file", str(env_path), "--validate-only"],
                ):
                    with redirect_stdout(buffer):
                        run_fulfillment_support.main()
            rendered = json.loads(buffer.getvalue())
            self.assertEqual(rendered["status"], "ready")
            self.assertEqual(rendered["systems"], ["labelogics"])
            self.assertEqual(rendered["channels"], ["slack"])

    def test_build_summary_includes_live_checks(self):
        config = base_config(Path("/tmp"))
        directories = {"intake": Path("/tmp/intake"), "connections_db": Path("/tmp/connections.sqlite3"), "shopify_accounts": Path("/tmp/shopify.json"), "labelogics_accounts": Path("/tmp/labelogics.json")}
        with mock.patch("scripts.run_fulfillment_support.live_checks", return_value={"slack": {"ok": True}}):
            rendered = run_fulfillment_support.build_summary(config, Path("/tmp/config.json"), directories, include_live_checks=True)
        self.assertIn("live_checks", rendered)
        self.assertTrue(rendered["live_checks"]["slack"]["ok"])

    def test_slack_live_check_skips_when_env_missing(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            rendered = run_fulfillment_support.slack_live_check()
        self.assertTrue(rendered["skipped"])

    def test_labelogics_live_check_skips_when_env_missing(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            rendered = run_fulfillment_support.labelogics_live_check()
        self.assertTrue(rendered["skipped"])

    def test_shopify_live_check_skips_when_env_missing(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            rendered = run_fulfillment_support.shopify_live_check()
        self.assertTrue(rendered["skipped"])

    def test_account_matching_summary_scores_alias_matches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shopify_path = root / "shopify_accounts.json"
            labelogics_path = root / "labelogics_accounts.json"
            shopify_path.write_text(json.dumps([{"store_domain": "akimbo-store.myshopify.com", "shop_name": "Akimbo Store", "aliases": ["akimbo"]}]))
            labelogics_path.write_text(json.dumps([{"account_id": "acct-1", "account_name": "Akimbo Fulfillment", "aliases": ["akimbo"]}]))
            config = {"account_matching": {"enabled": True, "ignored_tokens": ["anata", "fulfillment", "store"], "alias_overrides": {}}}
            directories = {"shopify_accounts": shopify_path, "labelogics_accounts": labelogics_path}
            with mock.patch.dict("os.environ", {"SUPPORT_SLACK_CHANNELS": "akimbo-anatafulfillment"}, clear=False):
                summary = run_fulfillment_support.account_matching_summary(config, directories)
            self.assertEqual(summary["shopify_accounts_loaded"], 1)
            self.assertEqual(summary["labelogics_accounts_loaded"], 1)
            self.assertEqual(summary["candidate_matches"][0]["shopify_match"]["display_name"], "Akimbo Store")
            self.assertEqual(summary["candidate_matches"][0]["labelogics_match"]["id"], "acct-1")

    def test_sync_connections_database_persists_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "connections.sqlite3"
            matching_summary = {
                "candidate_matches": [
                    {
                        "slack_channel": "akimbo-anatafulfillment",
                        "tokens": ["akimbo"],
                        "shopify_match": {"id": "akimbo-store.myshopify.com", "display_name": "Akimbo Store", "score": 35},
                        "labelogics_match": {"id": "acct-1", "display_name": "Akimbo Fulfillment", "score": 35},
                    }
                ]
            }
            result = run_fulfillment_support.sync_connections_database(
                db_path,
                matching_summary=matching_summary,
                slack_check={"visible_channels": [{"name": "akimbo-anatafulfillment", "id": "C123"}]},
                slack_records=[{"display_name": "akimbo-anatafulfillment", "normalized": ["akimbo-anatafulfillment"], "tokens": ["akimbo"]}],
                shopify_records=[{"id": "akimbo-store.myshopify.com", "display_name": "Akimbo Store", "aliases": ["akimbo"], "normalized": ["akimbo-store"], "tokens": ["akimbo"]}],
                labelogics_records=[{"id": "acct-1", "display_name": "Akimbo Fulfillment", "aliases": ["akimbo"], "normalized": ["akimbo-fulfillment"], "tokens": ["akimbo"]}],
                status="ready",
            )
            self.assertEqual(result["counts"]["connection_matches"], 1)
            connection = sqlite3.connect(db_path)
            try:
                row = connection.execute("SELECT status, shopify_store_domain, labelogics_account_id FROM connection_matches").fetchone()
            finally:
                connection.close()
            self.assertEqual(row[0], "matched")
            self.assertEqual(row[1], "akimbo-store.myshopify.com")
            self.assertEqual(row[2], "acct-1")

    def test_identifier_tokens_strips_compound_ignored_terms(self):
        rendered = run_fulfillment_support.identifier_tokens(
            "akimbo-anatafulfillment",
            ignored_tokens=["anata", "fulfillment"],
            alias_overrides={},
        )
        self.assertEqual(rendered, ["akimbo"])

    def test_schedule_window_allows_only_workday_hours(self):
        config = {"schedule": {"weekday_days": ["MO", "TU", "WE", "TH", "FR"], "weekday_hours": [8, 10, 12, 14, 16, 18]}}
        allowed = run_fulfillment_support.schedule_window_status(
            config,
            now=datetime.fromisoformat("2026-03-27T10:00:00-06:00"),
        )
        blocked = run_fulfillment_support.schedule_window_status(
            config,
            now=datetime.fromisoformat("2026-03-29T10:00:00-06:00"),
        )
        self.assertTrue(allowed["allowed"])
        self.assertFalse(blocked["allowed"])

    def test_record_case_event_suppresses_duplicates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "connections.sqlite3"
            connection = run_fulfillment_support.open_connections_db(db_path)
            try:
                run_fulfillment_support.upsert_support_case(
                    connection,
                    {
                        "case_id": "case-1",
                        "source_channel_name": "brand",
                        "source_channel_id": "C123",
                        "source_thread_ts": "1.000",
                        "latest_message_ts": "1.000",
                        "status": "new",
                    },
                )
                first = run_fulfillment_support.record_case_event(
                    connection,
                    event_key="slack_message:C123:1.000",
                    case_id="case-1",
                    event_type="intake_message",
                )
                second = run_fulfillment_support.record_case_event(
                    connection,
                    event_key="slack_message:C123:1.000",
                    case_id="case-1",
                    event_type="intake_message",
                )
            finally:
                connection.close()
            self.assertTrue(first)
            self.assertFalse(second)

    def test_decide_case_action_uses_labelogics_resolution(self):
        action = run_fulfillment_support.decide_case_action(
            {"source_channel_name": "brand"},
            question_summary="Where is my order?",
            identifiers={"order_numbers": ["1001"], "tracking_numbers": []},
            connection_match={"status": "partial", "labelogics_display_name": "Brand"},
            labelogics_evidence={"confidence": "high", "summary": "Shipping OS shows tracking 123 with status 'In Transit'."},
            shopify_evidence={"confidence": "none", "reason": "shopify_not_configured"},
        )
        self.assertEqual(action["status"], "responded")
        self.assertFalse(action["should_escalate"])
        self.assertIn("Here’s what I found", action["customer_reply"])
        self.assertEqual(action["issue_category"], "shipment_stuck")

    def test_decide_case_action_escalates_unmatched_cases(self):
        action = run_fulfillment_support.decide_case_action(
            {"source_channel_name": "brand"},
            question_summary="Where is my order?",
            identifiers={"order_numbers": ["1001"], "tracking_numbers": []},
            connection_match={"status": "unmatched"},
            labelogics_evidence={"confidence": "none", "reason": "missing_account_match"},
            shopify_evidence={"confidence": "none", "reason": "missing_store_match"},
        )
        self.assertEqual(action["status"], "waiting_human")
        self.assertTrue(action["should_escalate"])
        self.assertEqual(action["escalation_reason"], "match_missing")

    def test_decide_case_action_asks_clarifying_question_when_identifiers_missing(self):
        action = run_fulfillment_support.decide_case_action(
            {"source_channel_name": "brand"},
            question_summary="Where is my order?",
            source_text="Where is my order?",
            identifiers={"order_numbers": [], "tracking_numbers": []},
            connection_match={"status": "partial", "labelogics_display_name": "Brand"},
            labelogics_evidence={"confidence": "none", "reason": "missing_identifiers"},
            shopify_evidence={"confidence": "none", "reason": "missing_identifiers"},
        )
        self.assertEqual(action["status"], "investigating")
        self.assertFalse(action["should_escalate"])
        self.assertEqual(action["customer_reply"], "Can you send the order number so I can pull this up?")

    def test_should_ignore_internal_update_message(self):
        message = {
            "ts": "100.001",
            "user": "U123",
            "text": "@Ashley All above OOS orders are now resolved/in process of resolving with customer #108096 #109294 are PR orders and can be marked as fulfilled.",
        }
        self.assertTrue(run_fulfillment_support.should_ignore_slack_message(message, bot_user_id="BOT"))

    def test_message_from_external_team_detects_non_anata_sender(self):
        message = {"user_team": "T_OTHER", "source_team": "T_OTHER"}
        self.assertTrue(run_fulfillment_support.message_from_external_team(message, "TQFU890QH"))

    def test_low_signal_acknowledgment_is_filtered(self):
        self.assertTrue(run_fulfillment_support.is_low_signal_acknowledgment("Thank you"))
        self.assertTrue(run_fulfillment_support.is_low_signal_acknowledgment("Thank you!"))

    def test_extract_order_identifiers_ignores_slack_mentions_and_non_numeric_order_words(self):
        identifiers = run_fulfillment_support.extract_order_identifiers(
            "<@U096MRDT28Y> Order #109823 has the white tag. <@U063WD0MSPM>"
        )
        self.assertEqual(identifiers["order_numbers"], ["109823"])
        self.assertEqual(identifiers["tracking_numbers"], [])

    def test_review_candidates_returns_external_request_threads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = base_config(root)
            directories = run_fulfillment_support.resolve_directories(config, root)
            run_fulfillment_support.ensure_directories(directories)
            run_fulfillment_support.sync_connections_database(
                directories["connections_db"],
                matching_summary={
                    "candidate_matches": [
                        {
                            "slack_channel": "brand-fulfillment",
                            "tokens": ["brand"],
                            "shopify_match": {},
                            "labelogics_match": {"id": "pending:brand", "display_name": "Brand", "score": 25},
                        }
                    ]
                },
                slack_check={"visible_channels": [{"name": "brand-fulfillment", "id": "C111"}]},
                slack_records=[{"display_name": "brand-fulfillment", "normalized": ["brand-fulfillment"], "tokens": ["brand"]}],
                shopify_records=[],
                labelogics_records=[{"id": "pending:brand", "display_name": "Brand", "aliases": ["brand"], "normalized": ["brand"], "tokens": ["brand"]}],
                status="ready",
            )
            with mock.patch.dict("os.environ", {"SUPPORT_SLACK_CHANNELS": "brand-fulfillment"}, clear=False):
                with mock.patch(
                    "scripts.run_fulfillment_support.slack_live_check",
                    return_value={
                        "ok": True,
                        "user_id": "BOT",
                    "team_id": "TQFU890QH",
                    "team_url": "https://example.slack.com",
                    "visible_channels": [{"name": "brand-fulfillment", "id": "C111"}],
                },
                ):
                    with mock.patch(
                        "scripts.run_fulfillment_support.slack_channel_history",
                        return_value=[
                            {"ts": "101.000", "user": "U_EXT", "user_team": "T_OTHER", "source_team": "T_OTHER", "text": "Where is order #1001?"},
                            {"ts": "102.000", "user": "U_INT", "user_team": "TQFU890QH", "text": "resolved/in process"},
                        ],
                    ):
                        with mock.patch("scripts.run_fulfillment_support.labelogics_lookup", return_value={"confidence": "none", "reason": "placeholder_account_id"}):
                            with mock.patch("scripts.run_fulfillment_support.shopify_lookup", return_value={"confidence": "none", "reason": "missing_store_match"}):
                                result = run_fulfillment_support.review_candidates(
                                    config,
                                    directories,
                                    now=datetime.fromisoformat("2026-03-27T10:00:00-06:00"),
                                )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["candidates"][0]["channel"], "brand-fulfillment")
        self.assertEqual(result["candidates"][0]["recommended_action"]["reply_type"], "investigating")
        self.assertEqual(result["ingest"]["created_cases"], 1)

    def test_review_candidates_skips_external_non_support_chatter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = base_config(root)
            directories = run_fulfillment_support.resolve_directories(config, root)
            run_fulfillment_support.ensure_directories(directories)
            run_fulfillment_support.sync_connections_database(
                directories["connections_db"],
                matching_summary={"candidate_matches": []},
                slack_check={"visible_channels": [{"name": "brand-fulfillment", "id": "C111"}]},
                slack_records=[{"display_name": "brand-fulfillment", "normalized": ["brand-fulfillment"], "tokens": ["brand"]}],
                shopify_records=[],
                labelogics_records=[],
                status="ready",
            )
            with mock.patch.dict("os.environ", {"SUPPORT_SLACK_CHANNELS": "brand-fulfillment"}, clear=False):
                with mock.patch("scripts.run_fulfillment_support.slack_live_check", return_value={"ok": True, "user_id": "BOT", "team_url": "https://example.slack.com", "visible_channels": [{"name": "brand-fulfillment", "id": "C111"}]}):
                    with mock.patch("scripts.run_fulfillment_support.slack_channel_history", return_value=[{"ts": "101.000", "user": "U_EXT", "user_team": "T_OTHER", "source_team": "T_OTHER", "text": "FYI I just grabbed one popcorn popper for videos!"}]):
                        result = run_fulfillment_support.review_candidates(
                            config,
                            directories,
                            now=datetime.fromisoformat("2026-03-27T10:00:00-06:00"),
                        )
        self.assertEqual(result["count"], 0)

    def test_sync_case_from_message_links_duplicate_thread_to_existing_case(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "connections.sqlite3"
            connection = run_fulfillment_support.open_connections_db(db_path)
            try:
                run_fulfillment_support.upsert_support_case(
                    connection,
                    {
                        "case_id": "case-original",
                        "canonical_case_id": "case-original",
                        "source_channel_name": "brand-fulfillment",
                        "source_channel_id": "C111",
                        "source_thread_ts": "100.001",
                        "latest_message_ts": "100.001",
                        "status": "waiting_human",
                        "brand_name": "Brand",
                        "operational_object_key": "brand:order_numbers:1001",
                        "complaint_fingerprint": "where order 1001",
                    },
                )
                run_fulfillment_support.upsert_case_thread(
                    connection,
                    channel_id="C111",
                    thread_ts="100.001",
                    case_id="case-original",
                    relationship_type="new",
                    related_case_id="",
                    relationship_confidence=100,
                    last_message_ts="100.001",
                )
                synced = run_fulfillment_support.sync_case_from_message(
                    connection,
                    channel_name="brand-fulfillment",
                    channel_id="C111",
                    message={"ts": "101.000", "thread_ts": "101.000", "user": "U1", "text": "Any update on order #1001?"},
                    connection_match={"status": "partial", "labelogics_display_name": "Brand"},
                )
                thread_case = run_fulfillment_support.get_case_thread(connection, "C111", "101.000")
            finally:
                connection.close()
        self.assertEqual(synced["case_id"], "case-original")
        self.assertEqual(synced["relationship_type"], "follow_up")
        self.assertEqual(thread_case["case_id"], "case-original")

    def test_sync_case_from_message_reopens_resolved_case(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "connections.sqlite3"
            connection = run_fulfillment_support.open_connections_db(db_path)
            try:
                run_fulfillment_support.upsert_support_case(
                    connection,
                    {
                        "case_id": "case-original",
                        "canonical_case_id": "case-original",
                        "source_channel_name": "brand-fulfillment",
                        "source_channel_id": "C111",
                        "source_thread_ts": "100.001",
                        "latest_message_ts": "100.001",
                        "status": "resolved",
                        "resolved_at": "2026-03-27T10:00:00+00:00",
                        "brand_name": "Brand",
                        "operational_object_key": "brand:tracking_numbers:1Z999999999",
                        "complaint_fingerprint": "tracking 1z999999999 delivered not received",
                    },
                )
                synced = run_fulfillment_support.sync_case_from_message(
                    connection,
                    channel_name="brand-fulfillment",
                    channel_id="C111",
                    message={"ts": "101.000", "thread_ts": "101.000", "user": "U1", "text": "Package 1Z999999999 says delivered but I still don't have it."},
                    connection_match={"status": "partial", "labelogics_display_name": "Brand"},
                )
                refreshed = run_fulfillment_support.get_case(connection, "case-original")
            finally:
                connection.close()
        self.assertEqual(synced["relationship_type"], "reopened")
        self.assertEqual(refreshed["status"], "investigating")
        self.assertIsNone(refreshed["resolved_at"])

    def test_resolve_escalation_owners_routes_to_von_for_warehouse_issue(self):
        with mock.patch.dict("os.environ", {"SUPPORT_ESCALATION_VON_ID": "U_VON", "SUPPORT_ESCALATION_ASHLEY_ID": "U_ASHLEY"}, clear=False):
            owners = run_fulfillment_support.resolve_escalation_owners(
                issue_category="warehouse_execution",
                escalation_reason="shipment_stuck",
                operator_user_ids=["U_VON", "U_ASHLEY"],
            )
        self.assertEqual(owners["primary_owner"], "U_VON")

    def test_resolve_escalation_owners_routes_to_ashley_for_client_case(self):
        with mock.patch.dict("os.environ", {"SUPPORT_ESCALATION_VON_ID": "U_VON", "SUPPORT_ESCALATION_ASHLEY_ID": "U_ASHLEY"}, clear=False):
            owners = run_fulfillment_support.resolve_escalation_owners(
                issue_category="client_coordination",
                escalation_reason="insufficient_data",
                operator_user_ids=["U_VON", "U_ASHLEY"],
            )
        self.assertEqual(owners["primary_owner"], "U_ASHLEY")

    def test_format_escalation_message_includes_mentions_and_reason(self):
        rendered = run_fulfillment_support.format_escalation_message(
            {
                "case_id": "case-1",
                "source_channel_id": "C123",
                "source_thread_ts": "123.456",
                "source_channel_name": "brand-fulfillment",
            },
            question_summary="Where is my order?",
            connection_match={"labelogics_display_name": "Brand", "status": "partial"},
            labelogics_evidence={"reason": "missing_identifiers"},
            shopify_evidence={"reason": "shopify_not_configured"},
            escalation_reason="missing_identifiers",
            team_url="https://example.slack.com/",
            operator_mentions="<@U1> <@U2>",
        )
        self.assertIn("case-1", rendered)
        self.assertIn("<@U1> <@U2>", rendered)
        self.assertIn("missing_identifiers", rendered)
        self.assertIn("/archives/C123/p123456", rendered)

    def test_run_agent_creates_one_case_per_thread_and_escalates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directories = run_fulfillment_support.resolve_directories(base_config(root), root)
            run_fulfillment_support.ensure_directories(directories)
            db_path = directories["connections_db"]
            run_fulfillment_support.sync_connections_database(
                db_path,
                matching_summary={
                    "candidate_matches": [
                        {
                            "slack_channel": "brand-fulfillment",
                            "tokens": ["brand"],
                            "shopify_match": {},
                            "labelogics_match": {"id": "pending:brand", "display_name": "Brand", "score": 25},
                        }
                    ]
                },
                slack_check={"visible_channels": [{"name": "brand-fulfillment", "id": "C111"}, {"name": "fulfillment-ops", "id": "COPS"}]},
                slack_records=[{"display_name": "brand-fulfillment", "normalized": ["brand-fulfillment"], "tokens": ["brand"]}],
                shopify_records=[],
                labelogics_records=[{"id": "pending:brand", "display_name": "Brand", "aliases": ["brand"], "normalized": ["brand"], "tokens": ["brand"]}],
                status="ready",
            )
            config = base_config(root)
            history = [
                {"ts": "100.001", "thread_ts": "100.001", "user": "U123", "text": "Where is order #1001?"},
                {"ts": "100.002", "thread_ts": "100.001", "user": "U123", "text": "Any update?"},
            ]
            posted = []

            def fake_post(channel_id, text, thread_ts=""):
                posted.append({"channel_id": channel_id, "text": text, "thread_ts": thread_ts})
                return {"ok": True, "ts": f"{len(posted)}.000"}

            env = {
                "SUPPORT_SLACK_CHANNELS": "brand-fulfillment",
                "SUPPORT_ESCALATION_SLACK_USER_IDS": "U_VON,U_ASHLEY",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                with mock.patch("scripts.run_fulfillment_support.slack_live_check", return_value={"ok": True, "user_id": "BOT", "team_url": "https://example.slack.com", "visible_channels": [{"name": "brand-fulfillment", "id": "C111"}, {"name": "fulfillment-ops", "id": "COPS"}]}):
                    with mock.patch("scripts.run_fulfillment_support.slack_channel_history", return_value=history):
                        with mock.patch("scripts.run_fulfillment_support.slack_thread_messages", return_value=history):
                            with mock.patch("scripts.run_fulfillment_support.slack_post_message", side_effect=fake_post):
                                result = run_fulfillment_support.run_agent(
                                    config,
                                    directories,
                                    now=datetime.fromisoformat("2026-03-27T10:00:00-06:00"),
                                    force_run=False,
                                )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["counts"]["created_cases"], 1)
            self.assertEqual(result["counts"]["customer_replies"], 1)
            self.assertEqual(result["counts"]["escalations_posted"], 1)
            self.assertEqual(len(posted), 2)
            connection = sqlite3.connect(db_path)
            try:
                case_count = connection.execute("SELECT COUNT(*) FROM support_cases").fetchone()[0]
                status = connection.execute("SELECT status FROM support_cases").fetchone()[0]
                assignment_count = connection.execute("SELECT COUNT(*) FROM support_case_assignments").fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(case_count, 1)
            self.assertEqual(status, "waiting_human")
            self.assertEqual(assignment_count, 2)

    def test_process_case_marks_resolved_when_human_marks_thread_resolved(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "connections.sqlite3"
            connection = run_fulfillment_support.open_connections_db(db_path)
            try:
                run_fulfillment_support.upsert_support_case(
                    connection,
                    {
                        "case_id": "case-1",
                        "source_channel_name": "brand-fulfillment",
                        "source_channel_id": "C111",
                        "source_thread_ts": "100.001",
                        "latest_message_ts": "100.001",
                        "status": "waiting_human",
                    },
                )
                connection.execute(
                    """
                    INSERT INTO connection_matches (
                        slack_channel,
                        slack_tokens_json,
                        labelogics_account_id,
                        labelogics_display_name,
                        status
                    ) VALUES (?, '[]', 'pending:brand', 'Brand', 'partial')
                    """,
                    ("brand-fulfillment",),
                )
                with mock.patch(
                    "scripts.run_fulfillment_support.slack_thread_messages",
                    return_value=[
                        {"ts": "100.001", "user": "U123", "text": "Need an update"},
                        {"ts": "100.002", "user": "U456", "text": "resolved"},
                    ],
                ):
                    result = run_fulfillment_support.process_case(
                        connection,
                        case_row=run_fulfillment_support.get_case(connection, "case-1"),
                        queue_channel_name="fulfillment-ops",
                        queue_channel_id="COPS",
                        team_url="https://example.slack.com",
                        bot_user_id="BOT",
                        operator_user_ids=[],
                        resolution_markers=["resolved"],
                    )
                refreshed = run_fulfillment_support.get_case(connection, "case-1")
            finally:
                connection.close()
            self.assertEqual(result["resolved_cases"], 1)
            self.assertEqual(refreshed["status"], "resolved")


if __name__ == "__main__":
    unittest.main()
