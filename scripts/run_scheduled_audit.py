#!/usr/bin/env python3
"""Render cron wrapper for AP audits."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


TIMEZONE = ZoneInfo("America/Denver")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a scheduled AP audit.")
    parser.add_argument("--mode", choices=("daily", "weekly"), required=True)
    return parser.parse_args()


def bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def within_schedule_window(mode: str, now: datetime) -> bool:
    if mode == "daily":
        return now.weekday() < 5 and now.hour == 8
    return now.weekday() == 0 and now.hour == 8 and now.minute == 30


def download_suffix(source_url: str) -> str:
    parsed = urlparse(source_url)
    suffix = Path(parsed.path).suffix
    return suffix or ".csv"


def download_transactions(source_url: str, destination: Path) -> None:
    headers = {}
    auth_token = os.getenv("AP_TRANSACTIONS_AUTH_TOKEN", "").strip()
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    request = urllib.request.Request(source_url, headers=headers)
    with urllib.request.urlopen(request) as response:
        destination.write_bytes(response.read())


def resolve_transactions_path() -> str:
    direct_path = os.getenv("AP_TRANSACTIONS_PATH")
    if direct_path:
        return direct_path
    source_url = os.getenv("AP_TRANSACTIONS_URL")
    if source_url:
        suffix = download_suffix(source_url)
        destination = Path(tempfile.gettempdir()) / f"ap_transactions{suffix}"
        download_transactions(source_url, destination)
        return str(destination)
    raise FileNotFoundError("Set AP_TRANSACTIONS_PATH or AP_TRANSACTIONS_URL for scheduled audits.")


def main() -> None:
    args = parse_args()
    now = datetime.now(TIMEZONE)
    if not within_schedule_window(args.mode, now):
        print(f"Skipping {args.mode} audit at {now.isoformat()} outside the target America/Denver window.")
        return

    try:
        transactions_path = resolve_transactions_path()
    except FileNotFoundError as exc:
        print(f"Skipping {args.mode} audit: {exc}")
        return
    command = [
        sys.executable,
        str(Path(__file__).resolve().parents[1] / "ap_audit.py"),
        "--mode",
        args.mode,
        "--transactions",
        transactions_path,
        "--as-of-date",
        now.date().isoformat(),
        "--lookback-days",
        os.getenv("AP_LOOKBACK_DAYS", "7"),
    ]

    data_dir = os.getenv("AP_DATA_DIR")
    if data_dir:
        command.extend(["--data-dir", data_dir])
    if bool_env("AP_APPLY_CLICKUP_UPDATES", True):
        command.append("--apply-clickup-updates")
    if bool_env("AP_POST_SLACK", True):
        command.append("--post-slack")

    report_path = Path(tempfile.gettempdir()) / f"ap_{args.mode}_report.txt"
    payload_path = Path(tempfile.gettempdir()) / f"ap_{args.mode}_payload.json"
    slack_path = Path(tempfile.gettempdir()) / f"ap_{args.mode}_slack_payload.json"
    schema_path = Path(tempfile.gettempdir()) / f"ap_{args.mode}_schema.json"
    command.extend(
        [
            "--report-out",
            str(report_path),
            "--payload-out",
            str(payload_path),
            "--slack-payload-out",
            str(slack_path),
            "--schema-report-out",
            str(schema_path),
        ]
    )

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
