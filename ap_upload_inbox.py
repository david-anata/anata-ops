#!/usr/bin/env python3
"""Internal AP transaction upload inbox."""

from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import os
import re
import shutil
from datetime import date, datetime, timedelta, timezone
from email.parser import BytesParser
from email.policy import default
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode
from wsgiref.simple_server import make_server

import ap_audit
import qbo_client


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_STORAGE_DIR = ROOT_DIR / "data" / "upload_inbox"
LATEST_FILENAME = "latest.csv"
LATEST_METADATA_FILENAME = "latest.json"
ARCHIVE_DIRNAME = "archive"
DEFAULT_MAX_BYTES = 10 * 1024 * 1024
ACCEPTED_EXTENSIONS = {".csv"}
SESSION_COOKIE_NAME = "ap_upload_session"
SESSION_TTL_SECONDS = 12 * 60 * 60
STATIC_DIR = ROOT_DIR / "static"
QBO_TOKEN_FILENAME = "qbo_tokens.json"


def storage_dir() -> Path:
    configured = os.getenv("AP_UPLOAD_STORAGE_DIR")
    return Path(configured) if configured else DEFAULT_STORAGE_DIR


def max_upload_bytes() -> int:
    raw = os.getenv("AP_UPLOAD_MAX_BYTES", str(DEFAULT_MAX_BYTES))
    try:
        return max(int(raw), 1024)
    except ValueError:
        return DEFAULT_MAX_BYTES


def machine_token() -> str:
    return os.getenv("AP_UPLOAD_TOKEN", "").strip()


def admin_username() -> str:
    return os.getenv("AP_ADMIN_USERNAME", "").strip()


def admin_password() -> str:
    return os.getenv("AP_ADMIN_PASSWORD", "").strip()


def admin_login_enabled() -> bool:
    return bool(admin_username() and admin_password())


def session_secret() -> str:
    return (
        os.getenv("AP_SESSION_SECRET", "").strip()
        or machine_token()
        or admin_password()
    )


def ensure_storage(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / ARCHIVE_DIRNAME).mkdir(parents=True, exist_ok=True)


def latest_file_path(root: Path) -> Path:
    return root / LATEST_FILENAME


def latest_metadata_path(root: Path) -> Path:
    return root / LATEST_METADATA_FILENAME


def archive_dir(root: Path) -> Path:
    return root / ARCHIVE_DIRNAME


def qbo_token_store_path(root: Path) -> Path:
    return root / QBO_TOKEN_FILENAME


def runtime_rules(root: Path) -> Dict[str, Any]:
    return qbo_client.enrich_rules_with_qbo(ap_audit.load_rules(None), configured_path=qbo_token_store_path(root))


def sanitize_filename(filename: str) -> str:
    name = Path(filename or "").name or "transactions.csv"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", name)
    stem = stem.strip(".-") or "transactions.csv"
    if not stem.lower().endswith(".csv"):
        stem = f"{Path(stem).stem}.csv"
    return stem


def parse_query_string(environ: Dict[str, Any]) -> Dict[str, str]:
    parsed = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    return {key: values[-1] for key, values in parsed.items() if values}


def authorization_header_token(environ: Dict[str, Any]) -> str:
    header = environ.get("HTTP_AUTHORIZATION", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return ""


def request_token(environ: Dict[str, Any], form: Optional[Dict[str, Any]] = None) -> str:
    if form is not None:
        value = form.get("access_token", "")
        if isinstance(value, str) and value:
            return value.strip()
    query = parse_query_string(environ)
    if query.get("token"):
        return query["token"].strip()
    return authorization_header_token(environ)


def token_is_valid(token: str) -> bool:
    configured = machine_token()
    if not configured:
        return True
    return bool(token) and token == configured


def parse_cookie_header(environ: Dict[str, Any]) -> Dict[str, str]:
    raw = environ.get("HTTP_COOKIE", "")
    if not raw:
        return {}
    cookie = SimpleCookie()
    cookie.load(raw)
    return {name: morsel.value for name, morsel in cookie.items()}


def sign_session(username: str, expires_at: int) -> str:
    secret = session_secret()
    if not secret:
        raise ValueError("AP_SESSION_SECRET, AP_UPLOAD_TOKEN, or AP_ADMIN_PASSWORD is required when login is enabled.")
    payload = f"{username}:{expires_at}"
    signature = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    token = f"{payload}:{signature}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("utf-8")


def verify_session(token: str) -> bool:
    if not admin_login_enabled():
        return True
    if not token:
        return False
    try:
        decoded = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        username, expires_at_raw, signature = decoded.rsplit(":", 2)
        expires_at = int(expires_at_raw)
    except Exception:
        return False
    if username != admin_username() or expires_at < int(datetime.now(timezone.utc).timestamp()):
        return False
    payload = f"{username}:{expires_at}"
    expected = hmac.new(session_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected)


def request_is_admin_authenticated(environ: Dict[str, Any]) -> bool:
    if not admin_login_enabled():
        return True
    cookies = parse_cookie_header(environ)
    return verify_session(cookies.get(SESSION_COOKIE_NAME, ""))


def set_cookie_header(environ: Dict[str, Any], cookie_value: str) -> str:
    secure = (environ.get("HTTP_X_FORWARDED_PROTO") or environ.get("wsgi.url_scheme") or "http") == "https"
    parts = [
        f"{SESSION_COOKIE_NAME}={cookie_value}",
        "Path=/",
        f"Max-Age={SESSION_TTL_SECONDS}",
        "HttpOnly",
        "SameSite=Lax",
    ]
    if secure:
        parts.append("Secure")
    return "; ".join(parts)


def clear_cookie_header(environ: Dict[str, Any]) -> str:
    secure = (environ.get("HTTP_X_FORWARDED_PROTO") or environ.get("wsgi.url_scheme") or "http") == "https"
    parts = [
        f"{SESSION_COOKIE_NAME}=",
        "Path=/",
        "Max-Age=0",
        "HttpOnly",
        "SameSite=Lax",
    ]
    if secure:
        parts.append("Secure")
    return "; ".join(parts)


def current_metadata(root: Path) -> Dict[str, Any]:
    path = latest_metadata_path(root)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def store_upload(root: Path, original_filename: str, content: bytes) -> Dict[str, Any]:
    ensure_storage(root)
    safe_name = sanitize_filename(original_filename)
    timestamp = datetime.now(timezone.utc)
    stamped_name = f"{timestamp.strftime('%Y%m%dT%H%M%SZ')}_{safe_name}"
    archive_path = archive_dir(root) / stamped_name
    latest_path = latest_file_path(root)
    archive_path.write_bytes(content)
    shutil.copyfile(archive_path, latest_path)
    metadata = {
        "original_filename": safe_name,
        "stored_filename": stamped_name,
        "byte_size": len(content),
        "uploaded_at": timestamp.isoformat(),
        "latest_path": str(latest_path),
        "archive_path": str(archive_path),
    }
    latest_metadata_path(root).write_text(json.dumps(metadata, indent=2, sort_keys=True))
    return metadata


def format_timestamp(value: str) -> str:
    if not value:
        return "No upload yet"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def response(start_response: Any, status: str, body: bytes, headers: Iterable[Tuple[str, str]]) -> Iterable[bytes]:
    header_list = list(headers)
    header_list.append(("Content-Length", str(len(body))))
    start_response(status, header_list)
    return [body]


def text_response(start_response: Any, status: str, text: str, content_type: str = "text/plain; charset=utf-8") -> Iterable[bytes]:
    return response(start_response, status, text.encode("utf-8"), [("Content-Type", content_type)])


def json_response(start_response: Any, status: str, payload: Dict[str, Any]) -> Iterable[bytes]:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    return response(start_response, status, body, [("Content-Type", "application/json; charset=utf-8")])


def redirect_response(start_response: Any, location: str, headers: Optional[Iterable[Tuple[str, str]]] = None) -> Iterable[bytes]:
    base_headers = [("Location", location), ("Cache-Control", "no-store")]
    if headers:
        base_headers.extend(headers)
    return response(start_response, "303 See Other", b"", base_headers)


def page_shell(title: str, eyebrow: str, heading: str, intro: str, status_block: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="stylesheet" href="/static/style.css">
</head>
<body>
  <main class="page-shell">
    <section class="page-panel">
      <div class="page-head">
        <p class="page-eyebrow">{html.escape(eyebrow)}</p>
        <h1 class="page-title">{html.escape(heading)}</h1>
        <p class="page-copy">{html.escape(intro)}</p>
      </div>
      {status_block}
      {body}
    </section>
  </main>
</body>
</html>"""


def login_page(status_message: str) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    body = """<div class="grid">
        <section class="card card-form">
          <h2 class="section-title">Admin Login</h2>
          <p class="hint">Use the AP admin credentials once, then the browser keeps a signed session for future uploads.</p>
          <form action="/login" method="post">
            <label for="username">Username</label>
            <input id="username" name="username" type="text" autocomplete="username">
            <label class="label-spaced" for="password">Password</label>
            <input id="password" name="password" type="password" autocomplete="current-password">
            <button type="submit">Sign In</button>
          </form>
        </section>
      </div>"""
    return page_shell(
        title="Anata AP Upload Login",
        eyebrow="Anata AP Intake",
        heading="Admin Login",
        intro="Sign in before uploading the weekly bank transactions CSV.",
        status_block=status_block,
        body=body,
    )


def upload_page(status_message: str, metadata: Dict[str, Any], analysis_html: str = "") -> str:
    latest_name = html.escape(metadata.get("original_filename", "No file uploaded"))
    latest_uploaded_at = html.escape(format_timestamp(metadata.get("uploaded_at", "")))
    latest_size = metadata.get("byte_size", 0)
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    body = f"""<div class="toolbar">
        <p class="hint">Upload the newest bank-export CSV. The daily and weekly AP audits always fetch the current file from this inbox.</p>
        <form action="/logout" method="post">
          <button class="ghost" type="submit">Log Out</button>
        </form>
      </div>
      <div class="grid">
        <section class="card card-form">
          <h2 class="section-title">Upload Current File</h2>
          <form action="/upload" method="post" enctype="multipart/form-data">
            <label for="transaction_file">Transactions CSV</label>
            <input id="transaction_file" name="transaction_file" type="file" accept=".csv,text/csv">
            <button type="submit">Upload Latest CSV</button>
          </form>
        </section>
        <section class="card">
          <h2 class="section-title">Current File</h2>
          <div class="metric"><strong>Filename</strong>{latest_name}</div>
          <div class="metric"><strong>Uploaded At</strong>{latest_uploaded_at}</div>
          <div class="metric"><strong>Size</strong>{latest_size:,} bytes</div>
          <p><a href="/latest.csv">Download current transactions CSV</a></p>
          <p class="hint">Cron jobs read <code>/latest.csv</code> using the machine token, but admins can also download it directly while logged in.</p>
        </section>
      </div>
      {analysis_html}"""
    return page_shell(
        title="Anata AP Upload Inbox",
        eyebrow="Anata AP Intake",
        heading="Weekly Bank Export Inbox",
        intro="Submit the latest bank transactions CSV here instead of relying on a local file path.",
        status_block=status_block,
        body=body,
    )


def format_money(amount: float) -> str:
    return f"${amount:,.2f}"


def load_normalized_transactions(path: Path, root: Path) -> List[ap_audit.Transaction]:
    if not path.exists():
        return []
    rules = runtime_rules(root)
    rows = ap_audit.load_rows(str(path))
    return ap_audit.normalize_transactions(rows, rules)


def analysis_lookback_days() -> int:
    raw = os.getenv("AP_INBOX_LOOKBACK_DAYS", "7").strip()
    try:
        return max(int(raw), 1)
    except ValueError:
        return 7


def filter_recent_transactions(
    transactions: List[ap_audit.Transaction],
    *,
    lookback_days: Optional[int] = None,
) -> List[ap_audit.Transaction]:
    dated = [transaction for transaction in transactions if transaction.date]
    if not dated:
        return list(transactions)
    anchor = max(transaction.date for transaction in dated if transaction.date)  # type: ignore[arg-type]
    window = lookback_days or analysis_lookback_days()
    start_ord = anchor.toordinal() - max(window - 1, 0)
    return [transaction for transaction in transactions if not transaction.date or transaction.date.toordinal() >= start_ord]


def archive_paths(root: Path, current_stored_filename: str) -> List[Path]:
    paths = sorted(archive_dir(root).glob("*.csv"), key=lambda item: item.stat().st_mtime, reverse=True)
    return [path for path in paths if path.name != current_stored_filename]


def vendor_totals(transactions: List[ap_audit.Transaction]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for transaction in transactions:
        totals[transaction.vendor_name] = round(totals.get(transaction.vendor_name, 0.0) + transaction.amount, 2)
    return totals


def vendor_amount_history(transactions: List[ap_audit.Transaction]) -> Dict[str, List[float]]:
    history: Dict[str, List[float]] = {}
    for transaction in transactions:
        history.setdefault(transaction.vendor_name, []).append(transaction.amount)
    return history


def vendor_categories(transactions: List[ap_audit.Transaction]) -> Dict[str, str]:
    categories: Dict[str, str] = {}
    for transaction in transactions:
        categories.setdefault(transaction.vendor_name, transaction.category or "Uncategorized")
    return categories


def build_connected_systems(root: Path, rules: Dict[str, Any]) -> Dict[str, Any]:
    qbo_status = qbo_client.connection_status(qbo_token_store_path(root))
    qbo_known_vendor_keys = {
        ap_audit.normalize_key(alias)
        for alias in qbo_client.build_vendor_aliases(qbo_client.fetch_vendors(qbo_token_store_path(root))).values()
    } if qbo_status.get("connected") else set()

    clickup_token = os.getenv("CLICKUP_API_TOKEN", "").strip()
    clickup_list_id = os.getenv("CLICKUP_LIST_ID", "").strip()
    clickup_view_id = os.getenv("CLICKUP_VIEW_ID", "").strip()
    clickup_status = {
        "configured": bool(clickup_token and (clickup_list_id or clickup_view_id)),
        "connected": False,
        "vendor_count": 0,
        "message": "ClickUp credentials not configured.",
    }
    clickup_known_vendor_keys = set()
    if clickup_status["configured"]:
        try:
            task_rows = ap_audit.fetch_clickup_tasks(clickup_token, clickup_list_id or None, clickup_view_id or None)
            tasks = ap_audit.normalize_tasks(task_rows, rules)
            clickup_known_vendor_keys = {
                ap_audit.normalize_key(task.vendor_name)
                for task in tasks
                if task.vendor_name
            }
            clickup_status.update(
                {
                    "connected": True,
                    "vendor_count": len(clickup_known_vendor_keys),
                    "message": "AP vendor sync active.",
                }
            )
        except Exception as exc:
            clickup_status["message"] = f"ClickUp sync failed: {exc}"
    return {
        "known_vendor_keys": clickup_known_vendor_keys | qbo_known_vendor_keys,
        "clickup": clickup_status,
        "qbo": qbo_status,
    }


def build_archive_analysis(root: Path, metadata: Dict[str, Any], systems: Dict[str, Any]) -> Dict[str, Any]:
    latest_path = latest_file_path(root)
    if not latest_path.exists():
        return {"available": False}

    current_transactions = filter_recent_transactions(load_normalized_transactions(latest_path, root))
    current_stored = str(metadata.get("stored_filename", ""))
    history_files = archive_paths(root, current_stored)
    previous_file = history_files[0] if history_files else None
    previous_transactions = filter_recent_transactions(load_normalized_transactions(previous_file, root)) if previous_file else []
    history_transactions: List[ap_audit.Transaction] = []
    for path in history_files[:8]:
        history_transactions.extend(filter_recent_transactions(load_normalized_transactions(path, root)))

    current_totals = vendor_totals(current_transactions)
    previous_totals = vendor_totals(previous_transactions)
    historical_amounts = vendor_amount_history(history_transactions)
    categories = vendor_categories(current_transactions)
    known_vendor_keys = set(systems.get("known_vendor_keys", set()))
    vendor_counts: Dict[str, int] = {}
    for transaction in current_transactions:
        vendor_counts[transaction.vendor_name] = vendor_counts.get(transaction.vendor_name, 0) + 1

    baseline_ready = bool(history_files)
    new_charges: List[Dict[str, Any]] = []
    for transaction in sorted(current_transactions, key=lambda item: item.amount, reverse=True):
        history = historical_amounts.get(transaction.vendor_name, [])
        vendor_key = ap_audit.normalize_key(transaction.vendor_name)
        vendor_is_known = vendor_key in known_vendor_keys
        if not history:
            if vendor_is_known:
                continue
            new_charges.append(
                {
                    "vendor": transaction.vendor_name,
                    "amount": transaction.amount,
                    "date": transaction.date.isoformat() if transaction.date else "",
                    "reason": (
                        "Vendor does not appear in prior uploaded transaction history or the connected AP/QBO vendor set."
                        if baseline_ready
                        else "Vendor is not present in the connected ClickUp AP list or QuickBooks vendor set."
                    ),
                    "classification": "NEW_VENDOR" if baseline_ready else "NEW_UNMAPPED_VENDOR",
                    "action": "Confirm owner, necessity, and whether this should become a tracked recurring AP item.",
                }
            )
            continue
        if not baseline_ready:
            continue
        average_amount = sum(history) / len(history)
        recent_sample = history[:3]
        if (
            transaction.amount > average_amount * 1.2
            and transaction.amount - average_amount > 25
            and all(abs(transaction.amount - previous) > max(10.0, previous * 0.1) for previous in recent_sample)
        ):
            new_charges.append(
                {
                    "vendor": transaction.vendor_name,
                    "amount": transaction.amount,
                    "date": transaction.date.isoformat() if transaction.date else "",
                    "reason": "Amount is materially above the prior observed pattern for this vendor.",
                    "classification": "NEW_CHARGE_PATTERN",
                    "action": "Validate the invoice, seats, usage, or plan tier before the next cycle closes.",
                }
            )

    spend_growth: List[Dict[str, Any]] = []
    if baseline_ready:
        for vendor, current_total in current_totals.items():
            previous_total = previous_totals.get(vendor, 0.0)
            if previous_total > 0 and current_total > previous_total * 1.15 and current_total - previous_total > 25:
                spend_growth.append(
                    {
                        "vendor": vendor,
                        "current_total": current_total,
                        "previous_total": previous_total,
                        "delta": round(current_total - previous_total, 2),
                        "growth_pct": round(((current_total - previous_total) / previous_total) * 100, 1),
                        "category": categories.get(vendor, "Uncategorized"),
                    }
                )
    spend_growth.sort(key=lambda item: item["delta"], reverse=True)

    savings_opportunities: List[Dict[str, Any]] = []
    if baseline_ready:
        for item in spend_growth:
            if item["category"] in {"Software", "Marketing", "Operations"}:
                savings_opportunities.append(
                    {
                        "vendor": item["vendor"],
                        "amount": item["current_total"],
                        "reason": f"Spend increased {item['growth_pct']:.1f}% versus the previous uploaded period.",
                        "action": "Review plan tier, seats, downgrade options, or cancellation immediately.",
                        "priority": "High",
                    }
                )
        for item in new_charges:
            if categories.get(item["vendor"], "Uncategorized") in {"Software", "Marketing", "Operations"}:
                savings_opportunities.append(
                    {
                        "vendor": item["vendor"],
                        "amount": item["amount"],
                        "reason": "New operating spend detected before recurrence is established.",
                        "action": "Challenge ownership and approve only if it survives a savings review.",
                        "priority": "High" if item["amount"] >= 100 else "Medium",
                    }
                )
    for vendor, count in sorted(vendor_counts.items(), key=lambda entry: entry[1], reverse=True):
        total = current_totals[vendor]
        category = categories.get(vendor, "Uncategorized")
        if count > 1 and category in {"Software", "Marketing"}:
            savings_opportunities.append(
                {
                    "vendor": vendor,
                    "amount": total,
                    "reason": "Multiple charges from the same vendor landed in the current period.",
                    "action": "Check for duplicate subscriptions, split plans, or overlapping seats.",
                    "priority": "Medium",
                }
            )
        if category == "Software" and total <= 50:
            savings_opportunities.append(
                {
                    "vendor": vendor,
                    "amount": total,
                    "reason": "Low-dollar software line item may be easy to remove with little disruption.",
                    "action": "Confirm active usage and cut aggressively if no clear owner exists.",
                    "priority": "Medium",
                }
            )

    deduped_savings: List[Dict[str, Any]] = []
    seen_pairs = set()
    for item in savings_opportunities:
        key = (item["vendor"], item["action"])
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        deduped_savings.append(item)

    return {
        "available": True,
        "baseline_ready": baseline_ready,
        "lookback_days": analysis_lookback_days(),
        "current_transaction_count": len(current_transactions),
        "historical_upload_count": len(history_files),
        "total_current_spend": round(sum(transaction.amount for transaction in current_transactions), 2),
        "history_note": "" if baseline_ready else "Growth comparisons need at least one earlier uploaded file. New-charge checks still use live ClickUp vendors, and QuickBooks only if that later phase is enabled.",
        "new_charges": new_charges[:12],
        "spend_growth": spend_growth[:12],
        "savings_opportunities": deduped_savings[:12],
        "systems": systems,
    }


def run_live_ap_audit(root: Path, metadata: Dict[str, Any], systems: Dict[str, Any]) -> Dict[str, Any]:
    latest_path = latest_file_path(root)
    if not latest_path.exists():
        return {"available": False, "message": "No uploaded transaction file is available yet.", "systems": systems}
    clickup_token = os.getenv("CLICKUP_API_TOKEN", "").strip()
    clickup_list_id = os.getenv("CLICKUP_LIST_ID", "").strip()
    clickup_view_id = os.getenv("CLICKUP_VIEW_ID", "").strip()
    if not clickup_token or not (clickup_list_id or clickup_view_id):
        return {"available": False, "message": "Set CLICKUP_API_TOKEN and CLICKUP_LIST_ID on the inbox service to enable live AP urgency analysis.", "systems": systems}

    try:
        rules = runtime_rules(root)
        transactions = filter_recent_transactions(load_normalized_transactions(latest_path, root))
        as_of_date = max((transaction.date for transaction in transactions if transaction.date), default=date.today())
        task_rows = ap_audit.fetch_clickup_tasks(clickup_token, clickup_list_id or None, clickup_view_id or None)
        tasks = ap_audit.normalize_tasks(task_rows, rules)
        match_result = ap_audit.find_matches(transactions, tasks, rules, as_of_date)
        overdue = ap_audit.overdue_reviews(tasks, transactions, match_result["matched_transactions"], as_of_date)
        material_amount = rules.get("material_warning_amount", ap_audit.MATERIAL_WARNING_AMOUNT)
        warnings = ap_audit.build_slack_warnings(
            tasks,
            match_result["update_tasks"],
            match_result["create_tasks"],
            as_of_date,
            "daily",
            material_amount,
        )
        urgent_items = ap_audit.slim_daily_slack_warnings(warnings, as_of_date=as_of_date)
        new_charge_alerts = ap_audit.build_new_charge_alerts(
            transactions=transactions,
            tasks=tasks,
            creates=match_result["create_tasks"],
            exceptions=match_result["exceptions"],
            material_amount=material_amount,
        )
        return {
            "available": True,
            "as_of_date": as_of_date.isoformat(),
            "urgent_items": urgent_items,
            "new_charge_alerts": new_charge_alerts[:10],
            "create_count": len(match_result["create_tasks"]),
            "update_count": len(match_result["update_tasks"]),
            "overdue_count": len(overdue),
            "systems": systems,
        }
    except Exception as exc:
        return {"available": False, "message": f"Live AP audit failed: {exc}", "systems": systems}


def render_analysis_html(archive_analysis: Dict[str, Any], live_audit: Dict[str, Any]) -> str:
    if not archive_analysis.get("available"):
        return ""

    def render_rows(items: List[Dict[str, Any]], keys: List[Tuple[str, str]]) -> str:
        if not items:
            return "<p class='hint'>None right now.</p>"
        rows = []
        for item in items:
            parts = []
            for label, key in keys:
                value = item.get(key, "")
                if isinstance(value, float) and key.endswith("_pct"):
                    value = f"{value:.1f}%"
                elif isinstance(value, float):
                    value = format_money(value)
                parts.append(f"<strong>{html.escape(label)}:</strong> {html.escape(str(value))}")
            rows.append(f"<li class='detail-item'>{' | '.join(parts)}</li>")
        return f"<ul class='detail-list'>{''.join(rows)}</ul>"

    def render_system_card(title: str, status: Dict[str, Any]) -> str:
        badge_class = "badge-good" if status.get("connected") else "badge-warn" if status.get("configured") else "badge-muted"
        badge_text = "Connected" if status.get("connected") else "Needs setup" if status.get("configured") else "Not configured"
        return (
            f"<div class='system-row'>"
            f"<div><h3 class='system-title'>{html.escape(title)}</h3><p class='hint'>{html.escape(status.get('message', ''))}</p></div>"
            f"<div class='system-meta'><span class='badge {badge_class}'>{html.escape(badge_text)}</span>"
            f"<span class='system-count'>{status.get('vendor_count', 0)} vendors</span></div></div>"
        )

    urgent_html = "<p class='hint'>Live AP urgency analysis is not available yet.</p>"
    if live_audit.get("available"):
        urgent_html = render_rows(
            live_audit["urgent_items"],
            [("Vendor", "vendor"), ("Due", "due_date"), ("Remaining", "remaining_balance"), ("Action", "action"), ("Level", "level")],
        )
    elif live_audit.get("message"):
        urgent_html = f"<p class='hint'>{html.escape(live_audit['message'])}</p>"

    live_new_charges = live_audit["new_charge_alerts"] if live_audit.get("available") else archive_analysis["new_charges"]
    new_charge_keys = (
        [("Vendor", "vendor"), ("Amount", "amount"), ("Date", "date"), ("Type", "alert_type"), ("Action", "recommended_next_action")]
        if live_audit.get("available")
        else [("Vendor", "vendor"), ("Amount", "amount"), ("Date", "date"), ("Type", "classification"), ("Action", "action")]
    )
    baseline_note = (
        f"<p class='hint'>{html.escape(archive_analysis['history_note'])}</p>"
        if archive_analysis.get("history_note")
        else ""
    )
    systems = archive_analysis.get("systems") or live_audit.get("systems") or {}
    system_cards = [render_system_card("ClickUp AP", systems.get("clickup", {}))]
    if systems.get("qbo", {}).get("configured") or systems.get("qbo", {}).get("connected"):
        system_cards.append(render_system_card("QuickBooks Vendors", systems.get("qbo", {})))
    systems_html = "".join(system_cards)
    return f"""
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Analysis Overview</h2>
          <div class="metric"><strong>Current review window spend</strong>{format_money(archive_analysis['total_current_spend'])}</div>
          <div class="metric"><strong>Transactions in review window</strong>{archive_analysis['current_transaction_count']}</div>
          <div class="metric"><strong>Lookback days</strong>{archive_analysis['lookback_days']}</div>
          <div class="metric"><strong>Prior uploads available</strong>{archive_analysis['historical_upload_count']}</div>
          <div class="metric"><strong>New charges flagged</strong>{len(archive_analysis['new_charges'])}</div>
          <div class="metric"><strong>Spend growth flags</strong>{len(archive_analysis['spend_growth'])}</div>
          <div class="metric"><strong>Savings opportunities</strong>{len(archive_analysis['savings_opportunities'])}</div>
          {baseline_note}
        </section>
        <section class="card">
          <h2 class="section-title">Urgent This Week</h2>
          {urgent_html}
        </section>
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">New Charges / Unrecognized Activity</h2>
          <p class="hint">Flagged from the latest uploaded file versus prior uploads and current AP mappings.</p>
          {render_rows(live_new_charges, new_charge_keys)}
        </section>
        <section class="card">
          <h2 class="section-title">Spend Growing</h2>
          <p class="hint">Compared against the previous uploaded transaction file.</p>
          {render_rows(archive_analysis['spend_growth'], [("Vendor", "vendor"), ("Current", "current_total"), ("Previous", "previous_total"), ("Increase", "delta"), ("Growth", "growth_pct")])}
        </section>
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Savings Opportunities</h2>
          <p class="hint">Heuristic cut list. Use this to challenge spend aggressively every week.</p>
          {render_rows(archive_analysis['savings_opportunities'], [("Vendor", "vendor"), ("Amount", "amount"), ("Priority", "priority"), ("Reason", "reason"), ("Action", "action")])}
        </section>
        <section class="card">
          <h2 class="section-title">AP Audit Snapshot</h2>
          <p class="hint">This uses live ClickUp data when the inbox service has ClickUp credentials configured.</p>
          <div class="metric"><strong>New AP items</strong>{live_audit.get('create_count', 0)}</div>
          <div class="metric"><strong>Existing items to update</strong>{live_audit.get('update_count', 0)}</div>
          <div class="metric"><strong>Overdue review items</strong>{live_audit.get('overdue_count', 0)}</div>
          <div class="metric"><strong>Audit as of</strong>{html.escape(str(live_audit.get('as_of_date', 'Not available')))}</div>
        </section>
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Connected Systems</h2>
          <p class="hint">Phase 1 runs from ClickUp AP. Additional accounting connections stay optional until you enable them later.</p>
          <div class="system-grid">{systems_html}</div>
        </section>
      </div>
    """


def latest_download_url(environ: Dict[str, Any], token: str) -> str:
    host = environ.get("HTTP_HOST") or "localhost"
    scheme = environ.get("HTTP_X_FORWARDED_PROTO") or environ.get("wsgi.url_scheme") or "http"
    query = f"?{urlencode({'token': token})}" if token else ""
    return f"{scheme}://{host}/latest.csv{query}"


def parse_multipart_form(environ: Dict[str, Any]) -> Dict[str, Any]:
    content_type = environ.get("CONTENT_TYPE", "")
    if "multipart/form-data" not in content_type:
        raise ValueError("Expected multipart/form-data")
    content_length = int(environ.get("CONTENT_LENGTH") or "0")
    body = environ["wsgi.input"].read(content_length)
    raw_message = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8") + body
    message = BytesParser(policy=default).parsebytes(raw_message)
    form: Dict[str, Any] = {}
    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        filename = part.get_filename()
        payload = part.get_payload(decode=True) or b""
        if filename is not None:
            form[name] = {
                "filename": filename,
                "content": payload,
                "content_type": part.get_content_type(),
            }
            continue
        charset = part.get_content_charset() or "utf-8"
        form[name] = payload.decode(charset, errors="replace")
    return form


def parse_urlencoded_form(environ: Dict[str, Any]) -> Dict[str, str]:
    content_length = int(environ.get("CONTENT_LENGTH") or "0")
    body = environ["wsgi.input"].read(content_length).decode("utf-8")
    parsed = parse_qs(body, keep_blank_values=True)
    return {key: values[-1] for key, values in parsed.items() if values}


def upload_error(start_response: Any, status: str, message: str) -> Iterable[bytes]:
    return text_response(start_response, status, message, "text/plain; charset=utf-8")


def login_status_message(query: Dict[str, str]) -> str:
    status_message = query.get("status", "")
    if status_message == "uploaded":
        return "Upload accepted and current AP transaction file updated."
    if status_message == "logged-out":
        return "You have been logged out."
    if status_message == "bad-login":
        return "Login failed. Check the admin username and password."
    if status_message == "unauthorized":
        return "Sign in first."
    if status_message == "missing-file":
        return "Choose a CSV file before uploading."
    if status_message == "bad-type":
        return "Only CSV uploads are accepted."
    if status_message == "too-large":
        return f"Upload exceeds the {max_upload_bytes():,}-byte limit."
    return ""


def app(environ: Dict[str, Any], start_response: Any) -> Iterable[bytes]:
    root = storage_dir()
    ensure_storage(root)
    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/")
    query = parse_query_string(environ)
    metadata = current_metadata(root)

    if method == "GET" and path == "/static/style.css":
        css_path = STATIC_DIR / "style.css"
        if not css_path.exists():
            return text_response(start_response, "404 Not Found", "Not Found")
        return response(
            start_response,
            "200 OK",
            css_path.read_bytes(),
            [("Content-Type", "text/css; charset=utf-8"), ("Cache-Control", "public, max-age=300")],
        )

    if method == "GET" and path == "/health":
        return json_response(
            start_response,
            "200 OK",
            {
                "ok": True,
                "admin_login_enabled": admin_login_enabled(),
                "latest_upload": metadata,
                "machine_download_url": latest_download_url(environ, machine_token()) if machine_token() else latest_download_url(environ, ""),
            },
        )

    if method == "GET" and path in {"/", "/index.html"}:
        status_message = login_status_message(query)
        if request_is_admin_authenticated(environ):
            rules = runtime_rules(root)
            systems = build_connected_systems(root, rules)
            archive_analysis = build_archive_analysis(root, metadata, systems)
            live_audit = run_live_ap_audit(root, metadata, systems)
            body = upload_page(status_message, metadata, render_analysis_html(archive_analysis, live_audit))
        else:
            body = login_page(status_message)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/latest.csv":
        if not (request_is_admin_authenticated(environ) or token_is_valid(request_token(environ))):
            return text_response(start_response, "401 Unauthorized", "Unauthorized")
        latest_path = latest_file_path(root)
        if not latest_path.exists():
            return text_response(start_response, "404 Not Found", "No upload available yet.")
        body = latest_path.read_bytes()
        headers = [
            ("Content-Type", "text/csv; charset=utf-8"),
            ("Content-Disposition", 'attachment; filename="latest_transactions.csv"'),
            ("Cache-Control", "no-store"),
        ]
        return response(start_response, "200 OK", body, headers)

    if method == "POST" and path == "/login":
        form = parse_urlencoded_form(environ)
        if not admin_login_enabled():
            return redirect_response(start_response, "/")
        if form.get("username", "").strip() != admin_username() or form.get("password", "") != admin_password():
            return redirect_response(start_response, "/?status=bad-login")
        expires_at = int((datetime.now(timezone.utc) + timedelta(seconds=SESSION_TTL_SECONDS)).timestamp())
        cookie_header = set_cookie_header(environ, sign_session(admin_username(), expires_at))
        return redirect_response(start_response, "/", headers=[("Set-Cookie", cookie_header)])

    if method == "POST" and path == "/logout":
        return redirect_response(start_response, "/?status=logged-out", headers=[("Set-Cookie", clear_cookie_header(environ))])

    if method == "POST" and path == "/upload":
        if not request_is_admin_authenticated(environ):
            return redirect_response(start_response, "/?status=unauthorized")
        try:
            form = parse_multipart_form(environ)
        except ValueError:
            return upload_error(start_response, "400 Bad Request", "Could not parse upload form.")
        if "transaction_file" not in form:
            return redirect_response(start_response, "/?status=missing-file")
        upload_field = form["transaction_file"]
        if not isinstance(upload_field, dict):
            return redirect_response(start_response, "/?status=missing-file")
        filename = str(upload_field.get("filename", "")).strip()
        if not filename:
            return redirect_response(start_response, "/?status=missing-file")
        if Path(filename).suffix.lower() not in ACCEPTED_EXTENSIONS:
            return redirect_response(start_response, "/?status=bad-type")
        content = upload_field.get("content", b"")
        if not isinstance(content, bytes):
            return upload_error(start_response, "400 Bad Request", "Uploaded file content was invalid.")
        if len(content) > max_upload_bytes():
            return redirect_response(start_response, "/?status=too-large")
        store_upload(root, filename, content)
        return redirect_response(start_response, "/?status=uploaded")

    return text_response(start_response, "404 Not Found", "Not Found")


def main() -> None:
    host = "0.0.0.0"
    port = int(os.getenv("PORT", "10000"))
    with make_server(host, port, app) as server:
        print(f"AP upload inbox listening on {host}:{port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
