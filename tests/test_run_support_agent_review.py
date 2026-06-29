import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scripts import run_support_agent_review


class RunSupportAgentReviewTests(unittest.TestCase):
    def test_main_runs_pipeline_and_prints_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "fulfillment_support.json"
            config_path.write_text("{}")
            buffer = io.StringIO()
            with mock.patch("scripts.run_support_agent_review.support_agent.load_config") as load_config:
                with mock.patch("scripts.run_support_agent_review.support_agent.run_candidate_review_pipeline") as pipeline:
                    load_config.return_value = mock.Mock(reports_dir=Path(tmpdir) / "reports")
                    pipeline.return_value = {
                        "report": {
                            "schema_version": "1.0",
                            "report_id": "2026-03-27T18-00-00-06-00",
                            "status": "ready",
                            "candidate_count": 2,
                            "action_counts": {"clarifying": 1},
                            "lifecycle_counts": {"investigating": 2},
                            "summary": {
                                "action_counts": {"clarifying": 1, "investigating": 1},
                                "lifecycle_counts": {"investigating": 2},
                            },
                        },
                        "artifacts": {"json": Path(tmpdir) / "review.json"},
                    }
                    with mock.patch("sys.argv", ["run_support_agent_review.py", "--config", str(config_path)]):
                        with redirect_stdout(buffer):
                            run_support_agent_review.main()
            rendered = json.loads(buffer.getvalue())
            self.assertEqual(rendered["schema_version"], "1.0")
            self.assertEqual(rendered["report_id"], "2026-03-27T18-00-00-06-00")
            self.assertEqual(rendered["status"], "ready")
            self.assertEqual(rendered["candidate_count"], 2)
            self.assertEqual(rendered["action_counts"]["clarifying"], 1)
            self.assertEqual(rendered["lifecycle_counts"]["investigating"], 2)


if __name__ == "__main__":
    unittest.main()
