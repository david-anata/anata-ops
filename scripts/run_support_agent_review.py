#!/usr/bin/env python3
"""Generate read-only support-agent candidate review artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import support_agent


TIMEZONE = ZoneInfo("America/Denver")
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "fulfillment_support.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the support-agent review pipeline.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = support_agent.load_config(args.config)
    if args.output_dir.strip():
        config = support_agent.load_config(
            args.config,
            overrides={"reports_dir": Path(args.output_dir).expanduser()},
        )
    pipeline = support_agent.run_candidate_review_pipeline(
        config_path=args.config,
        support_config=config,
        now=datetime.now(TIMEZONE),
        persist=not args.dry_run,
    )
    report = pipeline["report"]
    summary_section = report.get("summary", {})
    if not isinstance(summary_section, dict):
        summary_section = {}
    summary = {
        "schema_version": report.get("schema_version", ""),
        "report_id": report.get("report_id", ""),
        "status": report["status"],
        "candidate_count": report["candidate_count"],
        "action_counts": summary_section.get("action_counts", report.get("action_counts", {})),
        "lifecycle_counts": summary_section.get("lifecycle_counts", report.get("lifecycle_counts", {})),
        "artifacts": {key: str(value) for key, value in pipeline.get("artifacts", {}).items()},
        "output_dir": str(config.reports_dir),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
