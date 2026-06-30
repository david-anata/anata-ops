import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

import website_ops


class WebsiteOpsTests(unittest.TestCase):
    def test_inspect_html_document_parses_headings_and_flags_generic_h1(self):
        html = """<!doctype html>
<html>
  <head>
    <title>Services - Anata</title>
    <link rel="canonical" href="https://example.com/services/">
    <meta name="description" content="Commercial service page">
  </head>
  <body>
    <h1>Contact Us</h1>
    <h2>Service overview</h2>
  </body>
</html>"""
        observation = website_ops.inspect_html_document("https://example.com/services/", html)

        self.assertEqual(observation["title"], "Services - Anata")
        self.assertEqual(observation["canonical_url"], "https://example.com/services/")
        self.assertEqual(observation["h1"], ["Contact Us"])
        issue_codes = {issue["code"] for issue in observation["issues"]}
        self.assertIn("GENERIC_PRIMARY_HEADING", issue_codes)

    def test_build_daily_report_summarizes_issues_and_recommendations(self):
        observations = [
            website_ops.inspect_html_document(
                "https://example.com/services/",
                "<html><head><title>Services</title><link rel='canonical' href='https://example.com/services/'></head><body><h1>Contact Us</h1></body></html>",
            ),
            website_ops.inspect_html_document(
                "https://example.com/about/",
                "<html><head></head><body><h1>About</h1></body></html>",
            ),
        ]
        report = website_ops.build_daily_report(
            observations,
            report_date=date(2026, 3, 26),
            feedback_entries=[
                {
                    "feedback_id": "fb-1",
                    "summary": "Improve shipping hero",
                    "priority": "High",
                    "page_url": "https://example.com/services/shipping/",
                    "status": "new",
                    "submitted_at": "2026-03-26T10:00:00+00:00",
                }
            ],
        )

        self.assertEqual(report["date"], "2026-03-26")
        self.assertEqual(report["pages_reviewed"], 2)
        self.assertGreater(report["issues_found"], 0)
        self.assertEqual(report["issue_counts_by_priority"]["P1"], 2)
        self.assertIn("Replace the H1 with a topic-specific heading", "\n".join(report["recommendations"]))
        self.assertEqual(report["feedback_received"], 1)
        self.assertEqual(report["feedback_open"], 1)
        self.assertEqual(report["action_counts_by_mode"]["approval-required"], 1)
        self.assertEqual(report["feedback_action_counts_by_mode"]["manual-only"], 1)

    def test_action_registry_validates_supported_feedback_actions(self):
        definitions = website_ops.website_ops_action_definitions()
        self.assertIn("replace_primary_heading", {item["action_type"] for item in definitions})
        self.assertEqual(
            website_ops.validate_feedback_action_payload(
                {"action_type": "replace_primary_heading", "action_value": "Sharper H1"}
            ),
            [],
        )
        self.assertTrue(
            website_ops.validate_feedback_action_payload(
                {"action_type": "replace_primary_heading", "action_value": ""}
            )
        )
        self.assertTrue(
            website_ops.validate_feedback_action_payload(
                {"action_type": "unsupported_action", "action_value": "Value"}
            )
        )

    def test_write_daily_report_artifacts_persists_json_markdown_and_html(self):
        report = website_ops.build_daily_report(
            [website_ops.inspect_html_document(
                "https://example.com/",
                "<html><head><title>Home</title><link rel='canonical' href='https://example.com/'></head><body><h1>Home</h1></body></html>",
            )],
            report_date="2026-03-26",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = website_ops.write_daily_report_artifacts(report, output_dir=Path(tmpdir))
            self.assertTrue(artifacts["json"].exists())
            self.assertTrue(artifacts["markdown"].exists())
            self.assertTrue(artifacts["html"].exists())
            self.assertIn("Website Ops Daily Report", artifacts["markdown"].read_text())
            self.assertIn("<!doctype html>", artifacts["html"].read_text().lower())

    def test_run_daily_report_pipeline_uses_collector_and_writes_artifacts(self):
        config = website_ops.WebsiteOpsConfig(
            website_ops_root=Path("/tmp/website-ops-root"),
            daily_reports_dir=Path("/tmp/website-ops-root/reports/daily"),
            feedback_dir=Path("/tmp/website-ops-root/feedback"),
            user_agent="test-agent",
            timeout_seconds=5,
            report_title="Pipeline Report",
        )
        observation = website_ops.inspect_html_document(
            "https://example.com/one/",
            "<html><head><title>One</title><link rel='canonical' href='https://example.com/one/'></head><body><h1>One</h1></body></html>",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            config = website_ops.WebsiteOpsConfig(
                website_ops_root=Path(tmpdir),
                daily_reports_dir=Path(tmpdir) / "reports" / "daily",
                feedback_dir=Path(tmpdir) / "feedback",
                user_agent="test-agent",
                timeout_seconds=5,
                report_title="Pipeline Report",
            )
            with mock.patch("website_ops.core.collect_page_observations", return_value=[observation]) as collector:
                pipeline = website_ops.run_daily_report_pipeline(["https://example.com/one/"], config=config)
            collector.assert_called_once()
            self.assertIn("report", pipeline)
            self.assertTrue(pipeline["artifacts"]["json"].exists())

    def test_feedback_entries_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = website_ops.WebsiteOpsConfig(
                website_ops_root=Path(tmpdir),
                daily_reports_dir=Path(tmpdir) / "reports" / "daily",
                feedback_dir=Path(tmpdir) / "feedback",
                user_agent="test-agent",
                timeout_seconds=5,
                report_title="Feedback Report",
            )
            saved = website_ops.save_feedback_entry(
                {
                    "title": "Need H1 fix",
                    "page_url": "https://example.com/services/",
                    "summary": "Generic H1 on services page",
                    "status": "open",
                },
                config=config,
                timestamp=datetime(2026, 3, 26, 15, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(saved.exists())
            loaded = website_ops.load_feedback_entries(config=config)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["summary"], "Generic H1 on services page")


if __name__ == "__main__":
    unittest.main()
