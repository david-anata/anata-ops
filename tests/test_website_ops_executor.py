import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import website_ops
from website_ops import executor


class WebsiteOpsExecutorTests(unittest.TestCase):
    def test_update_primary_heading_rewrites_first_h1_heading(self):
        elements = [
            {
                "id": "section1",
                "elements": [
                    {
                        "id": "heading1",
                        "widgetType": "heading",
                        "settings": {"title": "Old Heading", "header_size": "h1"},
                        "elements": [],
                    }
                ],
            }
        ]
        updated, summary = executor.update_primary_heading(elements, "New Heading")
        self.assertEqual(updated[0]["elements"][0]["settings"]["title"], "New Heading")
        self.assertEqual(updated[0]["elements"][0]["settings"]["header_size"], "h1")
        self.assertEqual(summary["before_text"], "Old Heading")
        self.assertEqual(summary["after_text"], "New Heading")

    def test_execute_feedback_action_updates_page_and_verifies(self):
        feedback = {
            "feedback_id": "fb-1",
            "status": "approved",
            "action_type": "replace_primary_heading",
            "action_value": "Sharper Commercial Heading",
            "page_url": "https://example.com/services/shipping/",
            "target_post_id": "5540",
        }
        page_record = {
            "id": 5540,
            "link": "https://example.com/services/shipping/",
            "meta": {
                "_elementor_data": json.dumps(
                    [
                        {
                            "id": "abc123",
                            "widgetType": "heading",
                            "settings": {"title": "Old Heading", "header_size": "h1"},
                            "elements": [],
                        }
                    ]
                )
            },
        }
        updated_record = {"id": 5540, "link": "https://example.com/services/shipping/"}
        verified_observation = {"h1": ["Sharper Commercial Heading"]}
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.object(executor, "backup_root", return_value=Path(tmpdir)):
                with mock.patch.object(executor, "resolve_page_record", return_value=page_record):
                    with mock.patch.object(executor, "wp_request", return_value=updated_record) as wp_request:
                        with mock.patch.object(executor, "collect_page_observation", return_value=verified_observation):
                            result = executor.execute_feedback_action(
                                feedback,
                                config=website_ops.WebsiteOpsConfig(
                                    website_ops_root=Path(tmpdir),
                                    daily_reports_dir=Path(tmpdir) / "reports" / "daily",
                                    feedback_dir=Path(tmpdir) / "feedback",
                                    user_agent="test-agent",
                                    timeout_seconds=5,
                                    report_title="Test",
                                ),
                            )
        self.assertEqual(result["verification_status"], "verified")
        self.assertEqual(result["summary"]["after_text"], "Sharper Commercial Heading")
        wp_request.assert_called_once()

    def test_execute_feedback_action_rejects_invalid_registry_payload_before_wp_calls(self):
        feedback = {
            "feedback_id": "fb-2",
            "status": "approved",
            "action_type": "unsupported_action",
            "action_value": "Nope",
            "page_url": "https://example.com/services/shipping/",
        }
        with mock.patch.object(executor, "resolve_page_record") as resolve:
            with self.assertRaises(executor.ExecutionError):
                executor.execute_feedback_action(feedback)
        resolve.assert_not_called()


if __name__ == "__main__":
    unittest.main()
