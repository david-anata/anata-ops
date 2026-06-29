import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import support_agent


class SupportAgentTests(unittest.TestCase):
    def test_build_candidate_report_summarizes_actions(self):
        report = support_agent.build_candidate_report(
            {
                "review_candidates": {
                    "status": "ready",
                    "candidates": [
                        {
                            "channel": "brand-fulfillment",
                            "channel_id": "C111",
                            "thread_ts": "100.001",
                            "recommended_action": {"reply_type": "clarifying", "status": "investigating"},
                            "identifiers": {"order_numbers": []},
                        },
                        {
                            "channel": "brand-fulfillment",
                            "channel_id": "C111",
                            "thread_ts": "100.002",
                            "recommended_action": {"reply_type": "resolution", "status": "responded"},
                            "identifiers": {"order_numbers": ["1001"]},
                        },
                    ],
                }
            },
            generated_at=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc),
            title="Support Review",
        )
        self.assertEqual(report["schema_version"], "1.0")
        self.assertTrue(report["report_id"])
        self.assertEqual(report["candidate_count"], 2)
        self.assertEqual(report["summary"]["candidate_count"], 2)
        self.assertEqual(report["summary"]["action_counts"]["clarifying"], 1)
        self.assertEqual(report["summary"]["action_counts"]["ready_to_answer"], 1)
        self.assertEqual(report["summary"]["lifecycle_counts"]["responded"], 1)
        self.assertEqual(report["recent_candidates"][0]["case_id"][:5], "case-")

    def test_write_candidate_report_artifacts_persists_outputs(self):
        report = {
            "schema_version": "1.0",
            "report_id": "2026-03-27T18-00-00+00-00",
            "report_slug": "support-review-2026-03-27T18-00-00+00-00",
            "title": "Support Review",
            "generated_at": "2026-03-27T18:00:00+00:00",
            "status": "ready",
            "candidate_count": 1,
            "action_counts": {"clarifying": 1, "investigating": 0, "ready_to_answer": 0, "escalated": 0, "resolved": 0},
            "lifecycle_counts": {"new": 0, "investigating": 1, "responded": 0, "escalated": 0, "waiting_human": 0, "resolved": 0},
            "summary": {
                "candidate_count": 1,
                "action_counts": {"clarifying": 1, "investigating": 0, "ready_to_answer": 0, "escalated": 0, "resolved": 0},
                "lifecycle_counts": {"new": 0, "investigating": 1, "responded": 0, "escalated": 0, "waiting_human": 0, "resolved": 0},
                "brand_counts": [{"brand": "Brand", "count": 1}],
                "account_counts": [],
                "escalation_count": 0,
                "unresolved_count": 1,
            },
            "recent_candidates": [],
            "candidates": [
                {
                    "case_id": "case-1",
                    "brand": "Brand",
                    "channel_name": "brand-fulfillment",
                    "customer_thread_link": "https://example.com",
                    "question_summary": "Where is my order?",
                    "lifecycle_state": "investigating",
                    "ui_recommendation": "clarifying",
                    "draft_reply": "Can you send the order number so I can pull this up?",
                    "recommended_action": {"reply_type": "clarifying", "customer_reply": "Can you send the order number so I can pull this up?"},
                }
            ],
            "escalations": [],
            "links": {
                "self_json": "/admin/fulfillment-cs/reports/support-review-2026-03-27T18-00-00+00-00.json",
                "self_html": "/admin/fulfillment-cs/reports/support-review-2026-03-27T18-00-00+00-00",
                "reports_index": "/admin/fulfillment-cs/reports/",
                "latest": "/admin/fulfillment-cs/reports/latest",
            },
            "warnings": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = support_agent.write_candidate_report_artifacts(report, output_dir=Path(tmpdir))
            self.assertTrue(artifacts["json"].exists())
            self.assertTrue(artifacts["markdown"].exists())
            self.assertTrue(artifacts["html"].exists())
            self.assertTrue(artifacts["latest_json"].exists())
            self.assertTrue(artifacts["latest_markdown"].exists())
            self.assertTrue(artifacts["latest_html"].exists())
            self.assertTrue(artifacts["index"].exists())
            self.assertIn("Support Review", artifacts["markdown"].read_text())
            self.assertIn("<table>", artifacts["html"].read_text())
            self.assertEqual(
                json.loads(artifacts["latest_json"].read_text())["candidate_count"],
                1,
            )
            index_payload = json.loads(artifacts["index"].read_text())
            self.assertEqual(index_payload["schema_version"], "1.0")
            self.assertEqual(index_payload["latest_report_id"], "2026-03-27T18-00-00+00-00")
            self.assertEqual(index_payload["reports"][0]["candidate_count"], 1)

    def test_load_config_honors_reports_dir_env_override(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "fulfillment_support.json"
            config_path.write_text(json.dumps({"paths": {"runs": "support-agent/runs"}}))
            with mock.patch.dict("os.environ", {"SUPPORT_AGENT_REPORTS_DIR": str(Path(tmpdir) / "custom-runs")}, clear=False):
                config = support_agent.load_config(config_path)
            self.assertEqual(config.reports_dir, Path(tmpdir) / "custom-runs")

    def test_run_candidate_review_pipeline_uses_runner_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            support_config = support_agent.SupportAgentConfig(
                support_agent_root=Path(tmpdir),
                reports_dir=Path(tmpdir) / "reports",
                report_title="Support Review",
            )
            summary = {
                "status": "ready",
                "live_checks": {"slack": {"ok": True}},
                "review_candidates": {
                    "status": "ready",
                    "candidates": [
                        {"recommended_action": {"reply_type": "clarifying"}},
                    ],
                },
            }
            with mock.patch("support_agent.core.run_fulfillment_support.load_env_file"):
                with mock.patch("support_agent.core.run_fulfillment_support.read_config", return_value={"paths": {}}):
                    with mock.patch("support_agent.core.run_fulfillment_support.resolve_workspace_root", return_value=Path(tmpdir)):
                        with mock.patch(
                            "support_agent.core.run_fulfillment_support.resolve_directories",
                            return_value={
                                "intake": Path(tmpdir) / "intake",
                                "runs": Path(tmpdir) / "runs",
                                "knowledge": Path(tmpdir) / "knowledge",
                                "escalations": Path(tmpdir) / "escalations",
                                "connections_db": Path(tmpdir) / "knowledge" / "connections.sqlite3",
                                "shopify_accounts": Path(tmpdir) / "knowledge" / "shopify.json",
                                "labelogics_accounts": Path(tmpdir) / "knowledge" / "labelogics.json",
                            },
                        ):
                                with mock.patch("support_agent.core.run_fulfillment_support.ensure_directories"):
                                    with mock.patch("support_agent.core.run_fulfillment_support.build_summary", return_value=summary):
                                        with mock.patch(
                                            "support_agent.core.run_fulfillment_support.review_candidates",
                                            return_value={
                                                "status": "ready",
                                                "candidates": [
                                                    {
                                                        "channel": "brand-fulfillment",
                                                        "channel_id": "C111",
                                                        "thread_ts": "100.001",
                                                        "recommended_action": {"reply_type": "clarifying", "status": "investigating"},
                                                        "identifiers": {"order_numbers": []},
                                                    }
                                                ],
                                            },
                                        ):
                                            pipeline = support_agent.run_candidate_review_pipeline(
                                                config_path=Path(tmpdir) / "fulfillment_support.json",
                                                support_config=support_config,
                                                now=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc),
                                                persist=True,
                                            )
            self.assertEqual(pipeline["report"]["candidate_count"], 1)
            self.assertEqual(pipeline["report"]["summary"]["action_counts"]["clarifying"], 1)
            self.assertTrue(pipeline["artifacts"]["json"].exists())

    def test_build_candidate_report_falls_back_to_persisted_cases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "connections.sqlite3"
            connection = sqlite3.connect(db_path)
            try:
                connection.execute(
                    """
                    CREATE TABLE support_cases (
                        case_id TEXT PRIMARY KEY,
                        source_channel_name TEXT,
                        source_channel_id TEXT,
                        source_thread_ts TEXT,
                        status TEXT,
                        brand_name TEXT,
                        labelogics_account_id TEXT,
                        shopify_store_domain TEXT,
                        customer_question_summary TEXT,
                        customer_facing_reply TEXT,
                        latest_evidence_summary TEXT,
                        escalation_reason TEXT,
                        relationship_type TEXT,
                        related_case_id TEXT,
                        issue_category TEXT,
                        primary_owner TEXT,
                        secondary_owner TEXT,
                        waiting_on TEXT,
                        updated_at TEXT
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO support_cases (
                        case_id, source_channel_name, source_channel_id, source_thread_ts, status, brand_name,
                        labelogics_account_id, customer_question_summary, customer_facing_reply,
                        latest_evidence_summary, escalation_reason, relationship_type, issue_category,
                        primary_owner, waiting_on, updated_at
                    ) VALUES (
                        'case-1', 'brand-fulfillment', 'C111', '100.001', 'waiting_human', 'Brand',
                        'pending:brand', 'Where is order #1001?', 'I am looking into it.',
                        'Shipping OS: missing identifiers', 'insufficient_data', 'new', 'client_coordination',
                        'U_ASHLEY', 'human', '2026-03-27T18:00:00+00:00'
                    )
                    """
                )
                connection.commit()
            finally:
                connection.close()
            report = support_agent.build_candidate_report(
                {"review_candidates": {"status": "ready", "candidates": []}},
                generated_at=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc),
                connections_db_path=db_path,
            )
        self.assertEqual(report["candidate_count"], 1)
        self.assertEqual(report["candidates"][0]["case_id"], "case-1")
        self.assertEqual(report["candidates"][0]["ui_recommendation"], "escalated")


if __name__ == "__main__":
    unittest.main()
