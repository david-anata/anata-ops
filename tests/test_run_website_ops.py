import io
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from scripts import run_website_ops

ROOT = Path(__file__).resolve().parents[1]


class _HealthyPageHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"""<!doctype html><html><head><title>Healthy Page</title><link rel="canonical" href="/"></head><body><h1>Healthy Page</h1></body></html>"""
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


class _LocalHTTPServer:
    def __enter__(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _HealthyPageHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.url = f"http://{host}:{port}/"
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RunWebsiteOpsTests(unittest.TestCase):
    def test_resolve_urls_prefers_args_then_env_then_config(self):
        args = mock.Mock(urls=["https://example.com/one/"])
        self.assertEqual(
            run_website_ops.resolve_urls(args, {"urls": ["https://example.com/two/"]}),
            ["https://example.com/one/"],
        )
        args = mock.Mock(urls=[])
        with mock.patch.dict("os.environ", {"WEBSITE_OPS_URLS": "https://example.com/three/,https://example.com/four/"}, clear=False):
            self.assertEqual(
                run_website_ops.resolve_urls(args, {"urls": ["https://example.com/two/"]}),
                ["https://example.com/three/", "https://example.com/four/"],
            )
        with mock.patch.dict("os.environ", {}, clear=False):
            self.assertEqual(
                run_website_ops.resolve_urls(args, {"urls": ["https://example.com/two/"]}),
                ["https://example.com/two/"],
            )

    def test_main_runs_pipeline_and_prints_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "website_ops.json"
            config_path.write_text(json.dumps({"urls": ["https://example.com/"]}))
            summary_buffer = io.StringIO()
            with mock.patch("scripts.run_website_ops.website_ops.load_config") as load_config:
                with mock.patch("scripts.run_website_ops.website_ops.load_feedback_entries", return_value=[{"summary": "Need better H1", "status": "new"}]):
                    with mock.patch("scripts.run_website_ops.website_ops.run_daily_report_pipeline") as pipeline:
                        load_config.return_value = mock.Mock(website_ops_root=Path(tmpdir))
                        pipeline.return_value = {
                            "report": {
                                "date": "2026-03-26",
                                "status": "needs-attention",
                                "pages_reviewed": 1,
                                "issues_found": 2,
                                "feedback_received": 1,
                            },
                            "artifacts": {"markdown": Path(tmpdir) / "report.md"},
                        }
                        with mock.patch("sys.argv", ["run_website_ops.py", "--config", str(config_path)]):
                            with redirect_stdout(summary_buffer):
                                run_website_ops.main()
            rendered = json.loads(summary_buffer.getvalue())
            self.assertEqual(rendered["mode"], "daily")
            self.assertEqual(rendered["pages_reviewed"], 1)
            self.assertEqual(rendered["feedback_received"], 1)

    def test_main_executes_approved_actions_when_enabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "website_ops.json"
            config_path.write_text(json.dumps({"urls": ["https://example.com/"]}))
            summary_buffer = io.StringIO()
            feedback = [{
                "feedback_id": "fb-1",
                "status": "approved",
                "action_type": "replace_primary_heading",
                "action_value": "New Heading",
                "_path": str(Path(tmpdir) / "feedback.json"),
            }]
            with mock.patch.dict("os.environ", {"WEBSITE_OPS_EXECUTE_APPROVED": "true"}, clear=False):
                with mock.patch("scripts.run_website_ops.website_ops.load_config") as load_config:
                    with mock.patch("scripts.run_website_ops.website_ops.load_feedback_entries", side_effect=[feedback, feedback]):
                        with mock.patch("scripts.run_website_ops.website_ops.execute_feedback_action", return_value={"executed_at": "2026-03-26T00:00:00+00:00", "verification_status": "verified"}) as execute:
                            with mock.patch("scripts.run_website_ops.website_ops.update_feedback_entry") as update:
                                with mock.patch("scripts.run_website_ops.website_ops.run_daily_report_pipeline") as pipeline:
                                    load_config.return_value = mock.Mock(website_ops_root=Path(tmpdir))
                                    pipeline.return_value = {
                                        "report": {
                                            "date": "2026-03-26",
                                            "status": "healthy",
                                            "pages_reviewed": 1,
                                            "issues_found": 0,
                                            "feedback_received": 1,
                                        },
                                        "artifacts": {},
                                    }
                                    with mock.patch("sys.argv", ["run_website_ops.py", "--config", str(config_path)]):
                                        with redirect_stdout(summary_buffer):
                                            run_website_ops.main()
            execute.assert_called_once()
            update.assert_called_once()

    def test_documented_cli_invocation_runs_as_script(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "website_ops.json"
            config_path.write_text(json.dumps({"urls": []}))
            with _LocalHTTPServer() as server:
                result = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "scripts" / "run_website_ops.py"),
                        "--mode",
                        "daily",
                        "--config",
                        str(config_path),
                        "--urls",
                        server.url,
                        "--dry-run",
                    ],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            self.assertEqual(result.returncode, 0, result.stderr)
            rendered = json.loads(result.stdout)
            self.assertEqual(rendered["mode"], "daily")
            self.assertEqual(rendered["pages_reviewed"], 1)


if __name__ == "__main__":
    unittest.main()
