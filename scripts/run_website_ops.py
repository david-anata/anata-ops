#!/usr/bin/env python3
"""Scheduled website ops reporter for Anata."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import website_ops


TIMEZONE = ZoneInfo("America/Denver")
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "website_ops.json"
REPORT_TITLE_BY_MODE = {
    "daily": "Anata Website Ops Daily Report",
    "weekly": "Anata Website Ops Weekly Report",
    "monthly": "Anata Website Ops Monthly Report",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the website ops report pipeline.")
    parser.add_argument("--mode", choices=("daily", "weekly", "monthly"), default="daily")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--urls", nargs="*", default=[])
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def read_raw_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def parse_env_urls(raw: str) -> List[str]:
    normalized = raw.replace("\n", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def resolve_urls(args: argparse.Namespace, raw_config: Dict[str, Any]) -> List[str]:
    if args.urls:
        return [item.strip() for item in args.urls if item.strip()]
    env_urls = parse_env_urls(os.getenv("WEBSITE_OPS_URLS", ""))
    if env_urls:
        return env_urls
    config_urls = raw_config.get("urls", [])
    if isinstance(config_urls, Sequence) and not isinstance(config_urls, (str, bytes)):
        return [str(item).strip() for item in config_urls if str(item).strip()]
    return []


def report_title(mode: str, raw_config: Dict[str, Any]) -> str:
    env_title = os.getenv("WEBSITE_OPS_REPORT_TITLE", "").strip()
    if env_title:
        return env_title
    if mode == "daily":
        configured = str(raw_config.get("report_title", "")).strip()
        if configured:
            return configured
    return REPORT_TITLE_BY_MODE[mode]


def report_scope(mode: str) -> str:
    if mode == "daily":
        return "automated read-only daily sweep"
    if mode == "weekly":
        return "automated weekly trend review"
    return "automated monthly architecture review"


def report_output_dir(mode: str, args: argparse.Namespace, config: website_ops.WebsiteOpsConfig) -> Path:
    if args.output_dir.strip():
        return Path(args.output_dir).expanduser()
    return config.website_ops_root / "reports" / mode


def report_notes(mode: str, urls: Sequence[str], feedback_entries: Sequence[Dict[str, Any]]) -> List[str]:
    notes = [
        f"Run mode: {mode}.",
        f"Monitored URLs: {len(urls)}.",
        f"Feedback records loaded: {len(feedback_entries)}.",
    ]
    open_feedback = [
        item for item in feedback_entries
        if str(item.get("status", "")).strip().lower() not in {"closed", "resolved", "done"}
    ]
    approved_feedback = [
        item for item in feedback_entries
        if str(item.get("status", "")).strip().lower() == "approved"
    ]
    if open_feedback:
        notes.append(f"Open feedback items waiting for review: {len(open_feedback)}.")
    if approved_feedback:
        notes.append(f"Approved feedback items ready for execution: {len(approved_feedback)}.")
    return notes


def main() -> None:
    args = parse_args()
    raw_config_path = Path(args.config).expanduser()
    raw_config = read_raw_config(raw_config_path)
    config = website_ops.load_config(raw_config_path)
    urls = resolve_urls(args, raw_config)
    if not urls:
        raise SystemExit("No URLs configured. Set WEBSITE_OPS_URLS or config/website_ops.json.")

    feedback_entries = website_ops.load_feedback_entries(config=config)
    executed_actions = []
    if website_ops.execution_enabled() and not args.dry_run:
        approved_items = [
            item for item in feedback_entries
            if str(item.get("status", "")).strip().lower() == "approved" and str(item.get("action_type", "")).strip()
        ]
        for item in approved_items:
            try:
                result = website_ops.execute_feedback_action(item, config=config)
            except website_ops.ExecutionError as exc:
                website_ops.update_feedback_entry(
                    item,
                    {
                        "status": "error",
                        "execution_error": str(exc),
                        "last_execution_at": datetime.now(TIMEZONE).isoformat(),
                    },
                )
            else:
                executed_actions.append(result)
                website_ops.update_feedback_entry(
                    item,
                    {
                        "status": "done",
                        "last_execution_at": result["executed_at"],
                        "execution_result": result,
                    },
                )
        feedback_entries = website_ops.load_feedback_entries(config=config)
    pipeline = website_ops.run_daily_report_pipeline(
        urls,
        config=config,
        output_dir=report_output_dir(args.mode, args, config),
        persist=not args.dry_run,
        feedback_entries=feedback_entries,
        title=report_title(args.mode, raw_config),
        report_type=f"website_ops_{args.mode}",
        scope=report_scope(args.mode),
        notes=report_notes(args.mode, urls, feedback_entries),
        report_date=datetime.now(TIMEZONE).date().isoformat(),
        executed_actions=executed_actions,
    )
    report = pipeline["report"]
    summary = {
        "mode": args.mode,
        "date": report["date"],
        "status": report["status"],
        "pages_reviewed": report["pages_reviewed"],
        "issues_found": report["issues_found"],
        "feedback_received": report.get("feedback_received", 0),
        "output_dir": str(report_output_dir(args.mode, args, config)),
        "artifacts": {key: str(value) for key, value in pipeline.get("artifacts", {}).items()},
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
