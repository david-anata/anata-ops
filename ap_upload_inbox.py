#!/usr/bin/env python3
"""Internal AP transaction upload inbox."""

from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import mimetypes
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
import hubspot_sales
import hubspot_sales_os
import qbo_client
import support_agent


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
WEBSITE_OPS_DIRNAME = "website-ops"
WEBSITE_OPS_REPORTS_DIRNAME = "reports"
WEBSITE_OPS_FEEDBACK_DIRNAME = "feedback"
WEBSITE_OPS_BACKUPS_DIRNAME = "backups"
WEBSITE_OPS_FEEDBACK_INBOX_DIRNAME = "inbox"
SUPPORT_AGENT_DIRNAME = "support-agent"
ADMIN_DIRNAME = "admin"
FULFILLMENT_CS_DIRNAME = "fulfillment-cs"


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


def admin_readonly_token() -> str:
    return os.getenv("ANATA_ADMIN_READONLY_TOKEN", "").strip()


def admin_username() -> str:
    return os.getenv("AP_ADMIN_USERNAME", "").strip()


def admin_password() -> str:
    return os.getenv("AP_ADMIN_PASSWORD", "").strip()


def admin_login_enabled() -> bool:
    return bool(admin_username() and admin_password())


def unauthenticated_local_bypass_enabled() -> bool:
    return os.getenv("ANATA_ALLOW_UNAUTHENTICATED_LOCAL", "").strip().lower() == "true"


def admin_auth_missing_env() -> List[str]:
    missing = []
    if not admin_username():
        missing.append("AP_ADMIN_USERNAME")
    if not admin_password():
        missing.append("AP_ADMIN_PASSWORD")
    return missing


def session_secret() -> str:
    return (
        os.getenv("AP_SESSION_SECRET", "").strip()
        or machine_token()
        or admin_password()
    )


def ensure_storage(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / ARCHIVE_DIRNAME).mkdir(parents=True, exist_ok=True)


def ensure_website_ops_storage() -> None:
    website_ops_root().mkdir(parents=True, exist_ok=True)
    website_ops_reports_root().mkdir(parents=True, exist_ok=True)
    website_ops_feedback_inbox_root().mkdir(parents=True, exist_ok=True)
    website_ops_backups_root().mkdir(parents=True, exist_ok=True)


def ensure_support_agent_storage() -> None:
    support_agent_reports_root().mkdir(parents=True, exist_ok=True)


def latest_file_path(root: Path) -> Path:
    return root / LATEST_FILENAME


def latest_metadata_path(root: Path) -> Path:
    return root / LATEST_METADATA_FILENAME


def archive_dir(root: Path) -> Path:
    return root / ARCHIVE_DIRNAME


def qbo_token_store_path(root: Path) -> Path:
    return root / QBO_TOKEN_FILENAME


def website_ops_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_DIR", "").strip()
    return Path(configured) if configured else ROOT_DIR / WEBSITE_OPS_DIRNAME


def website_ops_reports_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_REPORTS_DIR", "").strip()
    return Path(configured) if configured else website_ops_root() / WEBSITE_OPS_REPORTS_DIRNAME


def website_ops_feedback_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_FEEDBACK_DIR", "").strip()
    return Path(configured) if configured else website_ops_root() / WEBSITE_OPS_FEEDBACK_DIRNAME


def website_ops_feedback_inbox_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_FEEDBACK_INBOX_DIR", "").strip()
    return Path(configured) if configured else website_ops_feedback_root() / WEBSITE_OPS_FEEDBACK_INBOX_DIRNAME


def website_ops_backups_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_BACKUPS_DIR", "").strip()
    return Path(configured) if configured else website_ops_root() / WEBSITE_OPS_BACKUPS_DIRNAME


def support_agent_reports_root() -> Path:
    configured = os.getenv("SUPPORT_AGENT_REPORTS_DIR", "").strip()
    if configured:
        return Path(configured)
    return support_agent.load_config().reports_dir


def fulfillment_cs_base_path() -> str:
    return f"/{ADMIN_DIRNAME}/{FULFILLMENT_CS_DIRNAME}"


def support_agent_legacy_base_path() -> str:
    return f"/{SUPPORT_AGENT_DIRNAME}"


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
        return False
    return bool(token) and token == configured


def admin_readonly_token_is_valid(token: str) -> bool:
    configured = admin_readonly_token()
    if not configured:
        return False
    return bool(token) and hmac.compare_digest(token, configured)


def request_has_admin_readonly_access(environ: Dict[str, Any]) -> bool:
    if environ.get("REQUEST_METHOD", "GET").upper() != "GET":
        return False
    return admin_readonly_token_is_valid(request_token(environ))


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
    if unauthenticated_local_bypass_enabled():
        return True
    if not admin_login_enabled():
        return False
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
    if unauthenticated_local_bypass_enabled():
        return True
    if not admin_login_enabled():
        return False
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


def auth_configuration_error_response(start_response: Any, required_env: List[str]) -> Iterable[bytes]:
    message = "Authentication is not configured. Set required env vars: " + ", ".join(required_env)
    return text_response(start_response, "503 Service Unavailable", message)


def unauthorized_response(environ: Dict[str, Any], start_response: Any, login_redirect: str = "/?status=unauthorized") -> Iterable[bytes]:
    if wants_json_response(environ):
        return json_response(start_response, "401 Unauthorized", {"ok": False, "error": "unauthorized"})
    return redirect_response(start_response, login_redirect)


def require_admin_request(environ: Dict[str, Any], start_response: Any) -> Optional[Iterable[bytes]]:
    if unauthenticated_local_bypass_enabled():
        return None
    if request_has_admin_readonly_access(environ):
        return None
    missing = admin_auth_missing_env()
    if missing:
        return auth_configuration_error_response(start_response, missing)
    if not request_is_admin_authenticated(environ):
        return unauthorized_response(environ, start_response)
    return None


def is_protected_admin_path(path: str) -> bool:
    return path == f"/{ADMIN_DIRNAME}" or path.startswith(f"/{ADMIN_DIRNAME}/")


def is_protected_website_ops_path(path: str) -> bool:
    return path == f"/{WEBSITE_OPS_DIRNAME}" or path.startswith(f"/{WEBSITE_OPS_DIRNAME}/")


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
  <main class="deck ap-shell">
    <div class="deck-toolbar">
      <div class="brand-toolbar">
        <span class="brand-wordmark">
          <img src="/static/wordmark.png" alt="Anata" class="brand-image">
        </span>
      </div>
    </div>
    <section class="slide slide-cover ap-hero">
      <p class="eyebrow">{html.escape(eyebrow)}</p>
      <h1>{html.escape(heading)}</h1>
      <p class="lead ap-lead">{html.escape(intro)}</p>
      {status_block}
    </section>
    <section class="slide ap-workspace">
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
        <div class="ops-nav">
          <a href="/admin/sales/">Sales OS</a>
          <a href="/website-ops/">Website Ops</a>
          <a href="{html.escape(fulfillment_cs_base_path(), quote=True)}/">Fulfillment CS</a>
        </div>
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


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower())
    return slug.strip("-") or "item"


def extract_report_metadata(text: str, path: Path) -> Dict[str, str]:
    title_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    date_match = re.search(r"^Date:\s*(.+)$", text, re.MULTILINE)
    scope_match = re.search(r"^Scope:\s*(.+)$", text, re.MULTILINE)
    method_match = re.search(r"^Method:\s*(.+)$", text, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else path.stem.replace("-", " ").title()
    excerpt = ""
    paragraphs = []
    current: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            if current:
                paragraphs.append(" ".join(current).strip())
                current = []
            continue
        if stripped.startswith("#") or re.match(r"^(Date|Scope|Method):\s*", stripped):
            if current:
                paragraphs.append(" ".join(current).strip())
                current = []
            continue
        current.append(stripped)
        if sum(len(item) + 1 for item in current) > 240:
            paragraphs.append(" ".join(current).strip())
            current = []
            break
    if current:
        paragraphs.append(" ".join(current).strip())
    if paragraphs:
        excerpt = paragraphs[0]
    return {
        "title": title,
        "date": date_match.group(1).strip() if date_match else "",
        "scope": scope_match.group(1).strip() if scope_match else "",
        "method": method_match.group(1).strip() if method_match else "",
        "excerpt": excerpt,
    }


def _rewrite_internal_link(href: str) -> str:
    href = href.strip()
    marker = f"/{WEBSITE_OPS_DIRNAME}/"
    if marker in href:
        suffix = href.split(marker, 1)[1]
        return f"/{WEBSITE_OPS_DIRNAME}/{suffix.lstrip('/')}"
    return href


def render_inline_markup(text: str) -> str:
    escaped = html.escape(text)

    def replace_link(match: re.Match[str]) -> str:
        label = match.group(1)
        href = html.escape(_rewrite_internal_link(match.group(2)), quote=True)
        return f'<a href="{href}">{label}</a>'

    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", replace_link, escaped)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"__(.+?)__", r"<strong>\1</strong>", escaped)
    return escaped


def render_markdown(text: str) -> str:
    lines = text.splitlines()
    blocks: List[str] = []
    paragraph: List[str] = []
    list_items: List[str] = []
    list_type: Optional[str] = None
    code_lines: List[str] = []
    in_code = False

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            blocks.append(f"<p>{render_inline_markup(' '.join(paragraph).strip())}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_items, list_type
        if list_items:
            tag = "ol" if list_type == "ol" else "ul"
            blocks.append(f"<{tag}>" + "".join(list_items) + f"</{tag}>")
            list_items = []
            list_type = None

    def flush_code() -> None:
        nonlocal code_lines
        if code_lines:
            blocks.append(f"<pre><code>{html.escape(chr(10).join(code_lines))}</code></pre>")
            code_lines = []

    for line in lines:
        stripped = line.rstrip()
        marker = stripped.strip()
        if marker.startswith("```"):
            if in_code:
                flush_code()
            else:
                flush_paragraph()
                flush_list()
                in_code = True
            continue
        if in_code:
            code_lines.append(stripped)
            continue
        if not marker:
            flush_paragraph()
            flush_list()
            continue
        heading_match = re.match(r"^(#{1,6})\s+(.+)$", marker)
        if heading_match:
            flush_paragraph()
            flush_list()
            level = len(heading_match.group(1))
            blocks.append(f"<h{level}>{render_inline_markup(heading_match.group(2))}</h{level}>")
            continue
        bullet_match = re.match(r"^[-*]\s+(.+)$", marker)
        ordered_match = re.match(r"^\d+\.\s+(.+)$", marker)
        if bullet_match or ordered_match:
            flush_paragraph()
            kind = "ol" if ordered_match else "ul"
            if list_type and list_type != kind:
                flush_list()
            list_type = kind
            item_text = bullet_match.group(1) if bullet_match else ordered_match.group(1)
            list_items.append(f"<li>{render_inline_markup(item_text)}</li>")
            continue
        paragraph.append(marker)

    flush_paragraph()
    flush_list()
    flush_code()
    return "".join(blocks)


def report_paths() -> List[Path]:
    root = website_ops_reports_root()
    if not root.exists():
        return []
    paths = [path for path in root.rglob("*.md") if path.is_file()]
    return sorted(paths, key=lambda item: (item.stat().st_mtime, item.name), reverse=True)


def report_index_entries() -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for path in report_paths():
        category = path.parent.name
        try:
            text = path.read_text()
        except OSError:
            continue
        metadata = extract_report_metadata(text, path)
        grouped.setdefault(category, []).append(
            {
                "path": path,
                "slug": path.stem,
                "title": metadata["title"],
                "date": metadata["date"],
                "scope": metadata["scope"],
                "method": metadata["method"],
                "excerpt": metadata["excerpt"],
                "url": f"/{WEBSITE_OPS_DIRNAME}/reports/{category}/{path.stem}",
                "modified": datetime.fromtimestamp(path.stat().st_mtime).astimezone().strftime("%Y-%m-%d %H:%M %Z"),
            }
        )
    return grouped


def latest_report_entry() -> Optional[Dict[str, Any]]:
    entries = report_paths()
    if not entries:
        return None
    path = entries[0]
    try:
        text = path.read_text()
    except OSError:
        return None
    metadata = extract_report_metadata(text, path)
    category = path.parent.name
    return {
        "path": path,
        "title": metadata["title"],
        "date": metadata["date"],
        "scope": metadata["scope"],
        "method": metadata["method"],
        "excerpt": metadata["excerpt"],
        "url": f"/{WEBSITE_OPS_DIRNAME}/reports/{category}/{path.stem}",
    }


def open_feedback_queue_entries() -> List[Dict[str, Any]]:
    ranked = {"urgent": 0, "high": 1, "medium": 2, "low": 3}
    entries = [
        item for item in load_feedback_submissions()
        if str(item.get("status", "")).strip().lower() not in {"closed", "resolved", "done"}
    ]
    entries.sort(
        key=lambda item: (
            ranked.get(str(item.get("priority", "")).strip().lower(), 4),
            str(item.get("submitted_at", "")),
        ),
        reverse=False,
    )
    return entries


def website_ops_report_path_from_route(route_path: str) -> Optional[Path]:
    root = website_ops_reports_root().resolve()
    relative = route_path.removeprefix(f"/{WEBSITE_OPS_DIRNAME}/reports/").strip("/")
    if not relative:
        return None
    candidate = (root / relative).resolve()
    if root != candidate and root not in candidate.parents:
        return None
    if candidate.is_dir():
        md_candidate = candidate / "index.md"
        if md_candidate.exists():
            return md_candidate
        return candidate
    if candidate.suffix:
        return candidate if candidate.exists() else None
    md_candidate = candidate.with_suffix(".md")
    return md_candidate if md_candidate.exists() else None


def website_ops_backup_path_from_route(route_path: str) -> Optional[Path]:
    root = website_ops_backups_root().resolve()
    relative = route_path.removeprefix(f"/{WEBSITE_OPS_DIRNAME}/backups/").strip("/")
    if not relative:
        return None
    candidate = (root / relative).resolve()
    if root != candidate and root not in candidate.parents:
        return None
    return candidate if candidate.exists() else None


def report_category_entries(category: str) -> List[Dict[str, Any]]:
    entries = report_index_entries().get(category, [])
    return sorted(entries, key=lambda item: item.get("date") or item.get("modified", ""), reverse=True)


def report_categories() -> List[Dict[str, Any]]:
    categories = []
    for category in sorted(report_index_entries().keys()):
        entries = report_category_entries(category)
        categories.append(
            {
                "name": category,
                "count": len(entries),
                "latest": entries[0] if entries else None,
                "url": f"/{WEBSITE_OPS_DIRNAME}/reports/{category}/",
            }
        )
    return categories


def load_feedback_submissions() -> List[Dict[str, Any]]:
    inbox = website_ops_feedback_inbox_root()
    if not inbox.exists():
        return []
    submissions: List[Dict[str, Any]] = []
    for path in sorted(inbox.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        record["_path"] = path
        record["_url"] = f"/{WEBSITE_OPS_DIRNAME}/feedback/submissions/{path.stem}"
        submissions.append(record)
    return submissions


def load_feedback_submission(submission_id: str) -> Optional[Dict[str, Any]]:
    if not submission_id:
        return None
    path = website_ops_feedback_inbox_root() / f"{submission_id}.json"
    if not path.exists():
        return None
    try:
        record = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    record["_path"] = path
    record["_url"] = f"/{WEBSITE_OPS_DIRNAME}/feedback/submissions/{path.stem}"
    return record


def normalize_feedback_status(value: str) -> str:
    normalized = re.sub(r"[^a-z]+", "-", str(value or "").strip().lower()).strip("-")
    return normalized or "new"


def feedback_status_label(value: str) -> str:
    labels = {
        "new": "New",
        "approved": "Approved",
        "in-progress": "In Progress",
        "done": "Done",
        "rejected": "Rejected",
        "error": "Error",
    }
    return labels.get(normalize_feedback_status(value), normalize_feedback_status(value).replace("-", " ").title())


def feedback_status_counts(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {"new": 0, "approved": 0, "in-progress": 0, "done": 0, "rejected": 0, "error": 0}
    for item in entries:
        key = normalize_feedback_status(str(item.get("status", "")))
        counts[key] = counts.get(key, 0) + 1
    return counts


def save_feedback_submission(payload: Dict[str, Any], environ: Dict[str, Any]) -> Dict[str, Any]:
    ensure_website_ops_storage()
    timestamp = datetime.now(timezone.utc)
    category = str(payload.get("category", "")).strip() or "general"
    summary = str(payload.get("summary", "")).strip() or "feedback"
    record = {
        "feedback_id": f"{timestamp.strftime('%Y%m%dT%H%M%SZ')}-{slugify(category)}-{slugify(summary)[:48]}",
        "submitted_at": timestamp.isoformat(),
        "category": category,
        "priority": str(payload.get("priority", "")).strip() or "Medium",
        "page_url": str(payload.get("page_url", "")).strip(),
        "page_title": str(payload.get("page_title", "")).strip(),
        "summary": summary,
        "details": str(payload.get("details", "")).strip(),
        "desired_outcome": str(payload.get("desired_outcome", "")).strip(),
        "recommended_fix": str(payload.get("recommended_fix", "")).strip(),
        "reporter_name": str(payload.get("reporter_name", "")).strip(),
        "reporter_email": str(payload.get("reporter_email", "")).strip(),
        "source": str(payload.get("source", "web")).strip() or "web",
        "user_agent": environ.get("HTTP_USER_AGENT", ""),
        "referer": environ.get("HTTP_REFERER", ""),
        "remote_addr": environ.get("REMOTE_ADDR", ""),
        "status": "new",
    }
    output = website_ops_feedback_inbox_root() / f"{record['feedback_id']}.json"
    output.write_text(json.dumps(record, indent=2, sort_keys=True))
    record["_path"] = output
    record["_url"] = f"/{WEBSITE_OPS_DIRNAME}/feedback/submissions/{output.stem}"
    return record


def update_feedback_submission(submission_id: str, payload: Dict[str, Any], environ: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    record = load_feedback_submission(submission_id)
    if not record:
        return None
    timestamp = datetime.now(timezone.utc).isoformat()
    status = normalize_feedback_status(str(payload.get("status", "")))
    if status:
        record["status"] = status
    reviewer_name = str(payload.get("reviewer_name", "")).strip()
    review_notes = str(payload.get("review_notes", "")).strip()
    if reviewer_name:
        record["reviewer_name"] = reviewer_name
    if review_notes:
        record["review_notes"] = review_notes
    action_type = str(payload.get("action_type", "")).strip()
    action_value = str(payload.get("action_value", "")).strip()
    target_post_id = str(payload.get("target_post_id", "")).strip()
    if action_type:
        record["action_type"] = action_type
    if action_value:
        record["action_value"] = action_value
    if target_post_id:
        record["target_post_id"] = target_post_id
    record["reviewed_at"] = timestamp
    record["review_source"] = str(payload.get("source", "dashboard")).strip() or "dashboard"
    record["review_user_agent"] = environ.get("HTTP_USER_AGENT", "")
    record["review_remote_addr"] = environ.get("REMOTE_ADDR", "")
    output_path = record.get("_path")
    if not output_path:
        output_path = website_ops_feedback_inbox_root() / f"{submission_id}.json"
    path = Path(str(output_path))
    path.write_text(json.dumps({key: value for key, value in record.items() if not str(key).startswith("_")}, indent=2, sort_keys=True))
    return load_feedback_submission(submission_id)


def parse_feedback_request(environ: Dict[str, Any]) -> Dict[str, Any]:
    content_type = (environ.get("CONTENT_TYPE") or "").lower()
    content_length = int(environ.get("CONTENT_LENGTH") or "0")
    body = environ.get("wsgi.input").read(content_length) if content_length else b""
    if "application/json" in content_type:
        if not body:
            return {}
        parsed = json.loads(body.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("Expected a JSON object")
        return {str(key): value for key, value in parsed.items()}
    if body:
        parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
        return {key: values[-1] for key, values in parsed.items() if values}
    return parse_urlencoded_form(environ)


def wants_json_response(environ: Dict[str, Any]) -> bool:
    accept = (environ.get("HTTP_ACCEPT") or "").lower()
    content_type = (environ.get("CONTENT_TYPE") or "").lower()
    return "application/json" in accept or "application/json" in content_type


def website_ops_status_message(query: Dict[str, str]) -> str:
    status = query.get("status", "")
    if status == "submitted":
        return "Feedback saved to the intake inbox."
    if status == "bad-request":
        return "Feedback submission was incomplete."
    if status == "report-not-found":
        return "Report not found."
    if status == "missing-feedback":
        return "Choose a feedback category and summary before submitting."
    if status == "bad-json":
        return "Could not parse the JSON payload."
    if status == "review-updated":
        return "Approval status updated."
    if status == "submission-not-found":
        return "The feedback record could not be found."
    return ""


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


def website_ops_nav() -> str:
    return """
      <div class="toolbar">
        <p class="hint">Website ops report viewer, feedback intake, and backup browser.</p>
        <div class="ops-nav">
          <a href="/website-ops">Dashboard</a>
          <a href="/website-ops/reports/">Reports</a>
          <a href="/website-ops/queue">Queue</a>
          <a href="/website-ops/feedback">Feedback</a>
          <a href="/website-ops/backups/">Backups</a>
          <a href="/">AP Inbox</a>
        </div>
      </div>
    """


def render_stat_card(label: str, value: str, note: str = "") -> str:
    note_html = f"<small>{html.escape(note)}</small>" if note else ""
    return f"""
      <div class="card stat-card">
        <span>{html.escape(label)}</span>
        <strong>{html.escape(value)}</strong>
        {note_html}
      </div>
    """


def report_list_markup(entries: List[Dict[str, Any]], empty_message: str = "No reports available yet.") -> str:
    if not entries:
        return f"<p class='hint'>{html.escape(empty_message)}</p>"
    cards = []
    for entry in entries:
        excerpt = html.escape(entry.get("excerpt", "") or "No summary available.")
        meta_bits = [entry.get("date", ""), entry.get("scope", ""), entry.get("method", "")]
        meta = " · ".join(bit for bit in meta_bits if bit)
        cards.append(
            f"""
              <article class="report-card">
                <p class="eyebrow">{html.escape(entry.get("modified", ""))}</p>
                <h3><a href="{html.escape(entry.get('url', '#'), quote=True)}">{html.escape(entry.get('title', 'Untitled report'))}</a></h3>
                <p class="muted">{html.escape(meta)}</p>
                <p>{excerpt}</p>
              </article>
            """
        )
    return "".join(cards)


def website_ops_dashboard_page(status_message: str, latest_report: Optional[Dict[str, Any]], feedback_entries: List[Dict[str, Any]]) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    report_count = len(report_paths())
    feedback_count = len(feedback_entries)
    status_counts = feedback_status_counts(feedback_entries)
    latest_title = latest_report["title"] if latest_report else "No reports yet"
    latest_url = latest_report["url"] if latest_report else "/website-ops/reports/"
    latest_excerpt = latest_report["excerpt"] if latest_report else "Drop markdown reports into website-ops/reports/daily/, weekly/, or monthly/ and they will appear here."
    feedback_preview = feedback_entries[:4]
    body = f"""
      {website_ops_nav()}
      <div class="grid section-gap">
        {render_stat_card("Reports indexed", str(report_count), "Daily, weekly, and monthly markdown files.")}
        {render_stat_card("Feedback records", str(feedback_count), "Structured JSON intake submissions.")}
        {render_stat_card("Awaiting review", str(status_counts.get('new', 0)), "New items not yet approved or rejected.")}
        {render_stat_card("Approved", str(status_counts.get('approved', 0)), "Ready for execution on the next pass.")}
        {render_stat_card("Latest report", latest_title, latest_report.get("date", "") if latest_report else "Awaiting first report.")}
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Latest Report</h2>
          <p class="muted">{html.escape(latest_excerpt)}</p>
          <p><a href="{html.escape(latest_url, quote=True)}">Open the latest report</a></p>
        </section>
        <section class="card">
          <h2 class="section-title">Intake Paths</h2>
          <p class="muted">Use the structured feedback form for page issues, SEO fixes, content requests, and technical notes.</p>
          <p><a href="/website-ops/feedback">Submit feedback</a></p>
          <p><a href="/website-ops/queue">Review approval queue</a></p>
          <p><a href="/website-ops/reports/">Browse report library</a></p>
        </section>
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Recent Feedback</h2>
          {render_feedback_list(feedback_preview, include_actions=True)}
        </section>
        <section class="card">
          <h2 class="section-title">Report Library</h2>
          <div class="report-list">
            {report_list_markup([extract_report_entry(path) for path in report_paths()[:3]])}
          </div>
        </section>
      </div>
    """
    return page_shell(
        title="Anata Website Ops",
        eyebrow="Website Ops",
        heading="Website Ops Command Center",
        intro="View daily reports, inspect backups, and collect structured website feedback in one internal surface.",
        status_block=status_block,
        body=body,
    )


def website_ops_reports_index_page(status_message: str) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    categories = report_categories()
    recent_reports = report_paths()[:8]
    category_cards = []
    for category in categories:
        latest = category["latest"]
        latest_html = ""
        if latest:
            latest_html = (
                f"<p class='muted'>Latest: <a href='{html.escape(latest['url'], quote=True)}'>{html.escape(latest['title'])}</a></p>"
                f"<p class='muted'>{html.escape(latest.get('date', ''))}</p>"
            )
        category_cards.append(
            f"""
              <article class="report-card">
                <h3><a href="{html.escape(category['url'], quote=True)}">{html.escape(category['name'].title())}</a></h3>
                <p class="muted">{category['count']} report(s)</p>
                {latest_html}
              </article>
            """
        )
    body = f"""
      {website_ops_nav()}
      <div class="grid section-gap">
        {''.join(category_cards) if category_cards else "<p class='hint'>No report categories found yet.</p>"}
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Recent Reports</h2>
        <div class="report-list">
          {report_list_markup([extract_report_entry(path) for path in recent_reports])}
        </div>
      </div>
    """
    return page_shell(
        title="Anata Website Ops Reports",
        eyebrow="Website Ops",
        heading="Report Library",
        intro="Daily, weekly, and monthly report files rendered directly from the website-ops folder.",
        status_block=status_block,
        body=body,
    )


def website_ops_report_category_page(category: str, status_message: str) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    entries = report_category_entries(category)
    latest = entries[0] if entries else None
    latest_title = html.escape(latest["title"]) if latest else "None"
    latest_date = html.escape(latest.get("date", "")) if latest else "No reports yet."
    body = f"""
      {website_ops_nav()}
      <div class="grid section-gap">
        {render_stat_card("Category", category.title(), f"{len(entries)} report(s)")}
        {render_stat_card("Latest report", latest_title, latest_date)}
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Reports in {html.escape(category.title())}</h2>
        <div class="report-list">{report_list_markup(entries)}</div>
      </div>
    """
    return page_shell(
        title=f"Anata Website Ops Reports - {category.title()}",
        eyebrow="Website Ops",
        heading=f"{category.title()} Reports",
        intro="Browse reports in this folder and open the rendered markdown details.",
        status_block=status_block,
        body=body,
    )


def extract_report_entry(path: Path) -> Dict[str, Any]:
    try:
        text = path.read_text()
    except OSError:
        return {"title": path.stem, "excerpt": "", "url": f"/{WEBSITE_OPS_DIRNAME}/reports/{path.parent.name}/{path.stem}", "modified": ""}
    metadata = extract_report_metadata(text, path)
    return {
        "path": path,
        "slug": path.stem,
        "title": metadata["title"],
        "date": metadata["date"],
        "scope": metadata["scope"],
        "method": metadata["method"],
        "excerpt": metadata["excerpt"],
        "url": f"/{WEBSITE_OPS_DIRNAME}/reports/{path.parent.name}/{path.stem}",
        "modified": datetime.fromtimestamp(path.stat().st_mtime).astimezone().strftime("%Y-%m-%d %H:%M %Z"),
    }


def website_ops_report_detail_page(path: Path, status_message: str) -> str:
    try:
        text = path.read_text()
    except OSError:
        return website_ops_not_found_page("Report file could not be read.")
    metadata = extract_report_metadata(text, path)
    rendered = render_markdown(text)
    category = path.parent.name
    try:
        source_file = path.resolve().relative_to(website_ops_root().resolve())
    except ValueError:
        source_file = path.name
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    body = f"""
      {website_ops_nav()}
      <div class="ops-layout section-gap">
        <aside class="card ops-sidebar">
          <h2 class="section-title">{html.escape(metadata['title'])}</h2>
          <div class="report-meta-list">
            <div><span>Date</span><strong>{html.escape(metadata['date'] or 'Unknown')}</strong></div>
            <div><span>Category</span><strong>{html.escape(category.title())}</strong></div>
            <div><span>Scope</span><strong>{html.escape(metadata['scope'] or 'Not listed')}</strong></div>
            <div><span>Method</span><strong>{html.escape(metadata['method'] or 'Not listed')}</strong></div>
          </div>
          <p><a href="/website-ops/reports/{html.escape(category, quote=True)}/">Back to {html.escape(category.title())}</a></p>
          <p><a href="/website-ops/reports/">Back to report library</a></p>
          <p class="hint">Source file: {html.escape(str(source_file))}</p>
        </aside>
        <article class="card report-content">
          {rendered}
        </article>
      </div>
    """
    return page_shell(
        title=f"Anata Website Ops - {metadata['title']}",
        eyebrow="Website Ops",
        heading=metadata["title"],
        intro=metadata["excerpt"] or "Rendered report detail.",
        status_block=status_block,
        body=body,
    )


def website_ops_not_found_page(message: str) -> str:
    body = f"""
      {website_ops_nav()}
      <div class="card section-gap">
        <h2 class="section-title">Not Found</h2>
        <p>{html.escape(message)}</p>
      </div>
    """
    return page_shell(
        title="Anata Website Ops - Not Found",
        eyebrow="Website Ops",
        heading="Report or Submission Not Found",
        intro="The requested report, backup, or feedback record could not be located.",
        status_block="",
        body=body,
    )


def render_feedback_actions(item: Dict[str, Any]) -> str:
    submission_id = html.escape(str(item.get("feedback_id", "")), quote=True)
    current_status = normalize_feedback_status(str(item.get("status", "")))
    buttons = []
    for raw_status in ["approved", "in-progress", "done", "rejected"]:
        active = " is-active" if current_status == raw_status else ""
        buttons.append(
            f"""
              <form action="/website-ops/feedback/submissions/{submission_id}/status" method="post" class="inline-action-form">
                <input type="hidden" name="status" value="{html.escape(raw_status, quote=True)}">
                <button type="submit" class="ghost small{active}">{html.escape(feedback_status_label(raw_status))}</button>
              </form>
            """
        )
    return f'<div class="feedback-actions">{"".join(buttons)}</div>'


def render_feedback_list(entries: List[Dict[str, Any]], *, include_actions: bool = False) -> str:
    if not entries:
        return "<p class='hint'>No feedback submitted yet.</p>"
    cards = []
    for item in entries:
        submitted_at = item.get("submitted_at") or item.get("recorded_at", "")
        title = html.escape(item.get("summary", "Untitled feedback"))
        url = item.get("_url", "")
        title_html = f'<a href="{html.escape(url, quote=True)}">{title}</a>' if url else title
        cards.append(
            f"""
              <article class="feedback-item">
                <p class="eyebrow">{html.escape(submitted_at)}</p>
                <h3>{title_html}</h3>
                <p class="muted">{html.escape(item.get('category', 'General'))} · {html.escape(item.get('priority', 'Medium'))} · {html.escape(feedback_status_label(str(item.get('status', 'new'))))}</p>
                <p>{html.escape(item.get('page_url', '') or item.get('page_title', '') or 'No page specified')}</p>
                {render_feedback_actions(item) if include_actions else ""}
              </article>
            """
        )
    return "".join(cards)


def website_ops_queue_page(status_message: str) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    entries = open_feedback_queue_entries()
    counts = feedback_status_counts(load_feedback_submissions())
    urgent = [item for item in entries if str(item.get("priority", "")).strip().lower() == "urgent"]
    approved = [item for item in entries if normalize_feedback_status(str(item.get("status", ""))) == "approved"]
    body = f"""
      {website_ops_nav()}
      <div class="grid section-gap">
        {render_stat_card("Open items", str(len(entries)), "Feedback records not yet resolved.")}
        {render_stat_card("Urgent", str(len(urgent)), "Highest-priority queue items.")}
        {render_stat_card("Approved", str(len(approved)), "Ready for the next execution pass.")}
        {render_stat_card("New", str(counts.get('new', 0)), "Awaiting review.")}
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Open Queue</h2>
        {render_feedback_list(entries, include_actions=True)}
      </div>
    """
    return page_shell(
        title="Anata Website Ops Queue",
        eyebrow="Website Ops",
        heading="Open Work Queue",
        intro="Prioritized feedback records waiting for review, implementation, or closure.",
        status_block=status_block,
        body=body,
    )


def website_ops_feedback_page(status_message: str, feedback_entries: List[Dict[str, Any]]) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    body = f"""
      {website_ops_nav()}
      <div class="feedback-layout section-gap">
        <section class="card card-form">
          <h2 class="section-title">Submit Structured Feedback</h2>
          <p class="hint">Capture what changed, where it happened, and what outcome is needed. Submissions are stored as JSON in the feedback inbox.</p>
          <form action="/website-ops/feedback" method="post">
            <div class="feedback-grid">
              <div>
                <label for="reporter_name">Your Name</label>
                <input id="reporter_name" name="reporter_name" type="text" autocomplete="name">
              </div>
              <div>
                <label for="reporter_email">Email</label>
                <input id="reporter_email" name="reporter_email" type="text" autocomplete="email">
              </div>
              <div>
                <label for="category">Category</label>
                <select id="category" name="category">
                  <option value="">Choose one</option>
                  <option>Content</option>
                  <option>SEO</option>
                  <option>UX</option>
                  <option>Technical</option>
                  <option>Conversion</option>
                  <option>Strategy</option>
                </select>
              </div>
              <div>
                <label for="priority">Priority</label>
                <select id="priority" name="priority">
                  <option>Low</option>
                  <option selected>Medium</option>
                  <option>High</option>
                  <option>Urgent</option>
                </select>
              </div>
              <div class="feedback-span">
                <label for="page_url">Page URL</label>
                <input id="page_url" name="page_url" type="text" placeholder="https://anatainc.com/services/...">
              </div>
              <div class="feedback-span">
                <label for="page_title">Page Title</label>
                <input id="page_title" name="page_title" type="text" placeholder="Optional page title">
              </div>
              <div class="feedback-span">
                <label for="summary">Summary</label>
                <input id="summary" name="summary" type="text" placeholder="Short description of the issue or request">
              </div>
              <div class="feedback-span">
                <label for="details">Details</label>
                <textarea id="details" name="details" rows="5" placeholder="Describe what you saw, why it matters, and any reproduction notes."></textarea>
              </div>
              <div class="feedback-span">
                <label for="desired_outcome">Desired Outcome</label>
                <textarea id="desired_outcome" name="desired_outcome" rows="3" placeholder="What should be true after the fix?"></textarea>
              </div>
              <div class="feedback-span">
                <label for="recommended_fix">Recommended Fix</label>
                <textarea id="recommended_fix" name="recommended_fix" rows="3" placeholder="Optional solution or implementation note."></textarea>
              </div>
            </div>
            <button type="submit">Save Feedback</button>
          </form>
        </section>
        <aside class="card">
          <h2 class="section-title">Recent Intake</h2>
          {render_feedback_list(feedback_entries[:8], include_actions=True)}
        </aside>
      </div>
    """
    return page_shell(
        title="Anata Website Ops Feedback",
        eyebrow="Website Ops",
        heading="Structured Feedback Intake",
        intro="Collect actionable website feedback with enough context to queue, prioritize, and hand off without extra clarification.",
        status_block=status_block,
        body=body,
    )


def website_ops_feedback_submissions_page(status_message: str) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    entries = load_feedback_submissions()
    body = f"""
      {website_ops_nav()}
      <div class="card section-gap">
        <h2 class="section-title">Feedback Inbox</h2>
        {render_feedback_list(entries, include_actions=True)}
      </div>
    """
    return page_shell(
        title="Anata Website Ops Feedback Inbox",
        eyebrow="Website Ops",
        heading="Feedback Inbox",
        intro="Review all submitted website-ops feedback records.",
        status_block=status_block,
        body=body,
    )


def website_ops_feedback_submission_page(record: Dict[str, Any]) -> str:
    body = f"""
      {website_ops_nav()}
      <div class="card section-gap">
        <h2 class="section-title">Submission Saved</h2>
        <div class="report-meta-list">
          <div><span>ID</span><strong>{html.escape(record.get('feedback_id', ''))}</strong></div>
          <div><span>Category</span><strong>{html.escape(record.get('category', ''))}</strong></div>
          <div><span>Priority</span><strong>{html.escape(record.get('priority', ''))}</strong></div>
          <div><span>Summary</span><strong>{html.escape(record.get('summary', ''))}</strong></div>
        </div>
        <p><a href="/website-ops/feedback">Submit another item</a></p>
        <p><a href="/website-ops/feedback/submissions/{html.escape(record.get('feedback_id', ''), quote=True)}">Open this record</a></p>
      </div>
    """
    return page_shell(
        title="Anata Website Ops Feedback Saved",
        eyebrow="Website Ops",
        heading="Feedback Saved",
        intro="The intake record is now stored as a JSON artifact for review and triage.",
        status_block="",
        body=body,
    )


def website_ops_feedback_submission_detail(record: Dict[str, Any]) -> str:
    body = f"""
      {website_ops_nav()}
      <div class="ops-layout section-gap">
        <aside class="card ops-sidebar">
          <h2 class="section-title">Submission Details</h2>
          <div class="report-meta-list">
            <div><span>ID</span><strong>{html.escape(record.get('feedback_id', ''))}</strong></div>
            <div><span>Submitted</span><strong>{html.escape(record.get('submitted_at', ''))}</strong></div>
            <div><span>Category</span><strong>{html.escape(record.get('category', ''))}</strong></div>
            <div><span>Priority</span><strong>{html.escape(record.get('priority', ''))}</strong></div>
            <div><span>Status</span><strong>{html.escape(feedback_status_label(str(record.get('status', 'new'))))}</strong></div>
          </div>
          <p><a href="/website-ops/feedback">Back to intake</a></p>
        </aside>
        <article class="card report-content">
          <h2>{html.escape(record.get('summary', ''))}</h2>
          <p><strong>Page:</strong> {html.escape(record.get('page_url', '') or 'Not specified')}</p>
          <p><strong>Desired outcome:</strong> {html.escape(record.get('desired_outcome', '') or 'Not specified')}</p>
          <p><strong>Recommended fix:</strong> {html.escape(record.get('recommended_fix', '') or 'Not specified')}</p>
          <p><strong>Details:</strong></p>
          <p>{html.escape(record.get('details', '') or 'No details provided.')}</p>
          <section class="card subtle-card">
            <h3>Review and Approval</h3>
            <form action="/website-ops/feedback/submissions/{html.escape(record.get('feedback_id', ''), quote=True)}/status" method="post">
              <div class="feedback-grid">
                <div>
                  <label for="status">Status</label>
                  <select id="status" name="status">
                    <option value="new" {'selected' if normalize_feedback_status(str(record.get('status', 'new'))) == 'new' else ''}>New</option>
                    <option value="approved" {'selected' if normalize_feedback_status(str(record.get('status', 'new'))) == 'approved' else ''}>Approved</option>
                    <option value="in-progress" {'selected' if normalize_feedback_status(str(record.get('status', 'new'))) == 'in-progress' else ''}>In Progress</option>
                    <option value="done" {'selected' if normalize_feedback_status(str(record.get('status', 'new'))) == 'done' else ''}>Done</option>
                    <option value="rejected" {'selected' if normalize_feedback_status(str(record.get('status', 'new'))) == 'rejected' else ''}>Rejected</option>
                    <option value="error" {'selected' if normalize_feedback_status(str(record.get('status', 'new'))) == 'error' else ''}>Error</option>
                  </select>
                </div>
                <div>
                  <label for="reviewer_name">Reviewer</label>
                  <input id="reviewer_name" name="reviewer_name" type="text" value="{html.escape(record.get('reviewer_name', ''), quote=True)}" placeholder="Optional reviewer name">
                </div>
                <div>
                  <label for="action_type">Action Type</label>
                  <select id="action_type" name="action_type">
                    <option value="">Manual only</option>
                    <option value="replace_primary_heading" {'selected' if str(record.get('action_type', '')) == 'replace_primary_heading' else ''}>Replace Primary Heading</option>
                  </select>
                </div>
                <div>
                  <label for="target_post_id">Target Post ID</label>
                  <input id="target_post_id" name="target_post_id" type="text" value="{html.escape(record.get('target_post_id', ''), quote=True)}" placeholder="Optional WordPress post ID">
                </div>
                <div class="feedback-span">
                  <label for="action_value">Action Value</label>
                  <input id="action_value" name="action_value" type="text" value="{html.escape(record.get('action_value', ''), quote=True)}" placeholder="For heading changes, enter the new H1 text">
                </div>
                <div class="feedback-span">
                  <label for="review_notes">Review Notes</label>
                  <textarea id="review_notes" name="review_notes" rows="4" placeholder="Why this should be approved, rejected, or deferred.">{html.escape(record.get('review_notes', ''))}</textarea>
                </div>
              </div>
              <button type="submit">Submit Review</button>
            </form>
          </section>
        </article>
      </div>
    """
    return page_shell(
        title=f"Anata Website Ops Feedback - {record.get('summary', 'Record')}",
        eyebrow="Website Ops",
        heading=record.get("summary", "Feedback Submission"),
        intro=record.get("category", "Structured intake"),
        status_block="",
        body=body,
    )


def website_ops_backup_index_page() -> str:
    root = website_ops_backups_root()
    directories = [path for path in sorted(root.iterdir(), key=lambda item: item.name, reverse=True) if path.is_dir()] if root.exists() else []
    cards = []
    for directory in directories:
        file_count = len([path for path in directory.iterdir() if path.is_file()]) if directory.exists() else 0
        cards.append(
            f"""
              <article class="report-card">
                <h3><a href="/website-ops/backups/{html.escape(directory.name, quote=True)}">{html.escape(directory.name)}</a></h3>
                <p class="muted">{file_count} file(s)</p>
              </article>
            """
        )
    body = f"""
      {website_ops_nav()}
      <div class="card section-gap">
        <h2 class="section-title">Backup Sets</h2>
        <div class="report-list">{''.join(cards) if cards else "<p class='hint'>No backup folders found yet.</p>"}</div>
      </div>
    """
    return page_shell(
        title="Anata Website Ops Backups",
        eyebrow="Website Ops",
        heading="Backup Browser",
        intro="Browse report backups and exported page JSON from the website-ops archive.",
        status_block="",
        body=body,
    )


def website_ops_backup_detail_page(path: Path) -> str:
    if path.is_file():
        if path.suffix.lower() == ".json":
            try:
                rendered = json.dumps(json.loads(path.read_text()), indent=2, sort_keys=True)
            except (OSError, json.JSONDecodeError):
                rendered = path.read_text(errors="replace")
        else:
            rendered = path.read_text(errors="replace")
        body = f"""
          {website_ops_nav()}
          <div class="card section-gap">
            <h2 class="section-title">{html.escape(path.name)}</h2>
            <pre class="file-preview">{html.escape(rendered)}</pre>
          </div>
        """
    else:
        entries = []
        for child in sorted(path.iterdir(), key=lambda item: item.name):
            if child.is_file():
                entries.append(f"<li><a href=\"/website-ops/backups/{html.escape(path.name, quote=True)}/{html.escape(child.name, quote=True)}\">{html.escape(child.name)}</a></li>")
        body = f"""
          {website_ops_nav()}
          <div class="card section-gap">
            <h2 class="section-title">{html.escape(path.name)}</h2>
            <ul class="backup-list">{''.join(entries) if entries else '<li>No files found.</li>'}</ul>
          </div>
        """
    return page_shell(
        title=f"Anata Website Ops Backups - {path.name}",
        eyebrow="Website Ops",
        heading=path.name,
        intro="Backup set contents.",
        status_block="",
        body=body,
    )


def support_agent_nav() -> str:
    base = fulfillment_cs_base_path()
    return f"""
      <div class="toolbar">
        <p class="hint">Fulfillment CS review dashboard, candidate queue, and report library.</p>
        <div class="ops-nav">
          <a href="{base}">Dashboard</a>
          <a href="{base}/reports/">Reports</a>
          <a href="{base}/reports/latest">Latest</a>
          <a href="/admin/website-ops">Website Ops</a>
          <a href="/">AP Inbox</a>
        </div>
      </div>
    """


def support_agent_report_paths() -> List[Path]:
    root = support_agent_reports_root()
    if not root.exists():
        return []
    return sorted(
        [path for path in root.glob("support-review-*.json") if path.is_file()],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )


def support_agent_report_slug_from_route(route_path: str) -> str:
    prefixes = [
        f"/{SUPPORT_AGENT_DIRNAME}/reports/",
        f"{fulfillment_cs_base_path()}/reports/",
        f"/{ADMIN_DIRNAME}/{SUPPORT_AGENT_DIRNAME}/reports/",
    ]
    slug = route_path
    for prefix in prefixes:
        if route_path.startswith(prefix):
            slug = route_path.removeprefix(prefix)
            break
    slug = slug.strip("/").split("/", 1)[0]
    if not re.fullmatch(r"support-review-[A-Za-z0-9._+-]*", slug):
        return ""
    return slug


def support_agent_report_path_from_route(route_path: str) -> Optional[Path]:
    slug = support_agent_report_slug_from_route(route_path)
    if not slug:
        return None
    candidate = support_agent_reports_root() / f"{slug}.json"
    return candidate if candidate.exists() and candidate.is_file() else None


def support_agent_report_artifact_path_from_route(route_path: str) -> Optional[Path]:
    slug = support_agent_report_slug_from_route(route_path)
    if not slug:
        return None
    stem, suffix = os.path.splitext(slug)
    if suffix not in {".json", ".html", ".md"}:
        return None
    candidate = support_agent_reports_root() / f"{stem}{suffix}"
    return candidate if candidate.exists() and candidate.is_file() else None


def support_agent_report_artifact_content_type(path: Path) -> str:
    if path.suffix == ".json":
        return "application/json; charset=utf-8"
    if path.suffix == ".html":
        return "text/html; charset=utf-8"
    if path.suffix == ".md":
        return "text/markdown; charset=utf-8"
    return "application/octet-stream"


def load_support_agent_report(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def support_agent_latest_report_path() -> Optional[Path]:
    root = support_agent_reports_root()
    latest_path = root / "latest.json"
    if latest_path.exists() and latest_path.is_file():
        return latest_path
    paths = support_agent_report_paths()
    return paths[0] if paths else None


def support_agent_latest_timestamped_report_entry() -> Optional[Dict[str, Any]]:
    paths = support_agent_report_paths()
    return extract_support_agent_report_entry(paths[0]) if paths else None


def format_action_counts(action_counts: Dict[str, Any]) -> str:
    if not action_counts:
        return "No action counts recorded."
    bits = [f"{key}: {action_counts[key]}" for key in sorted(action_counts)]
    return ", ".join(bits)


def extract_support_agent_report_entry(path: Path) -> Dict[str, Any]:
    payload = load_support_agent_report(path) or {}
    generated_at = format_timestamp(str(payload.get("generated_at", "")))
    candidate_count = int(payload.get("candidate_count", 0) or 0)
    action_counts = payload.get("action_counts", {}) if isinstance(payload.get("action_counts", {}), dict) else {}
    excerpt = f"{candidate_count} candidate thread(s). {format_action_counts(action_counts)}"
    title = str(payload.get("title", "Fulfillment Support Review")).strip() or "Fulfillment Support Review"
    return {
        "path": path,
        "slug": path.stem,
        "title": title,
        "generated_at": generated_at,
        "candidate_count": candidate_count,
        "action_counts": action_counts,
        "excerpt": excerpt,
        "url": f"{fulfillment_cs_base_path()}/reports/{path.stem}",
    }


def render_support_agent_candidates(candidates: List[Dict[str, Any]]) -> str:
    if not candidates:
        return "<p class='hint'>No candidate threads available yet.</p>"
    cards = []
    for item in candidates:
        recommended_action = item.get("recommended_action", {})
        identifiers = item.get("identifiers", {})
        evidence = item.get("evidence", {})
        ids = []
        for key in ("order_numbers", "tracking_numbers", "po_numbers"):
            values = identifiers.get(key, [])
            if values:
                ids.append(f"{key.replace('_', ' ').title()}: {', '.join(str(value) for value in values)}")
        evidence_bits = []
        for key in ("labelogics", "shopify"):
            source = evidence.get(key, {})
            if not isinstance(source, dict):
                continue
            status = str(source.get("status", "")).strip()
            if status:
                evidence_bits.append(f"{key.title()}: {status}")
        cards.append(
            f"""
              <article class="feedback-item">
                <p class="eyebrow">{html.escape(str(item.get('brand_name', item.get('channel', 'Unknown'))))}</p>
                <h3><a href="{html.escape(str(item.get('permalink', '#')), quote=True)}">Open Slack thread</a></h3>
                <p class="muted">{html.escape(str(item.get('channel', '')))} · {html.escape(str(recommended_action.get('reply_type', 'unknown')))}</p>
                <p>{html.escape(str(item.get('question_summary', 'No summary available.')))}</p>
                <p class="hint">{html.escape(' | '.join(ids) if ids else 'No extracted identifiers yet.')}</p>
                <p class="hint">{html.escape(' | '.join(evidence_bits) if evidence_bits else 'No system evidence attached yet.')}</p>
                <p><strong>Draft reply:</strong> {html.escape(str(recommended_action.get('customer_reply', '')))}</p>
              </article>
            """
        )
    return "".join(cards)


def support_agent_dashboard_page(status_message: str, latest_report: Optional[Dict[str, Any]]) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    candidate_count = latest_report.get("candidate_count", 0) if latest_report else 0
    generated_at = latest_report.get("generated_at", "Awaiting first review run.") if latest_report else "Awaiting first review run."
    action_counts = latest_report.get("action_counts", {}) if latest_report else {}
    action_cards = "".join(
        render_stat_card(key.replace("_", " ").title(), str(value), "Current recommended actions in the latest review.")
        for key, value in sorted(action_counts.items())
    )
    latest_candidates = latest_report.get("candidates", [])[:6] if latest_report else []
    body = f"""
      {support_agent_nav()}
      <div class="grid section-gap">
        {render_stat_card("Candidate threads", str(candidate_count), "Open review candidates from the latest pass.")}
        {render_stat_card("Latest review", generated_at, "Timestamp from the latest persisted support review.")}
        {action_cards or render_stat_card("Action counts", "0", "Run the review pipeline to populate candidate actions.")}
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Latest Review</h2>
          <p class="muted">{html.escape(latest_report.get('title', 'Fulfillment Support Review') if latest_report else 'No report has been generated yet.')}</p>
          <p><a href="{fulfillment_cs_base_path()}/reports/latest">Open the latest report</a></p>
          <p><a href="{fulfillment_cs_base_path()}/reports/">Browse report library</a></p>
        </section>
        <section class="card">
          <h2 class="section-title">Dashboard Contract</h2>
          <p class="muted">The review pipeline writes stable `latest.json`, `latest.md`, `latest.html`, and `index.json` artifacts for the future `agent.anatainc.com` support page.</p>
        </section>
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Candidate Preview</h2>
        {render_support_agent_candidates(latest_candidates)}
      </div>
    """
    return page_shell(
        title="Anata Fulfillment CS",
        eyebrow="Fulfillment CS",
        heading="Fulfillment CS",
        intro="Read-only dashboard for candidate customer-service threads, draft replies, and escalation recommendations.",
        status_block=status_block,
        body=body,
    )


def support_agent_reports_index_page(status_message: str) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    entries = [extract_support_agent_report_entry(path) for path in support_agent_report_paths()]
    cards = []
    for entry in entries[:12]:
        cards.append(
            f"""
              <article class="report-card">
                <p class="eyebrow">{html.escape(entry.get('generated_at', ''))}</p>
                <h3><a href="{html.escape(entry.get('url', '#'), quote=True)}">{html.escape(entry.get('title', 'Support Review'))}</a></h3>
                <p class="muted">{html.escape(entry.get('excerpt', ''))}</p>
              </article>
            """
        )
    body = f"""
      {support_agent_nav()}
      <div class="card section-gap">
        <h2 class="section-title">Support Review Reports</h2>
        <div class="report-list">{''.join(cards) if cards else "<p class='hint'>No support-review reports found yet.</p>"}</div>
      </div>
    """
    return page_shell(
        title="Anata Fulfillment CS Reports",
        eyebrow="Fulfillment CS",
        heading="Report Library",
        intro="Browse persisted support review reports generated from the Slack-first fulfillment review pipeline.",
        status_block=status_block,
        body=body,
    )


def support_agent_report_detail_page(path: Path, status_message: str) -> str:
    report = load_support_agent_report(path)
    if not report:
        return support_agent_not_found_page("The requested support report could not be read.")
    try:
        source_file = path.resolve().relative_to(ROOT_DIR.resolve())
    except ValueError:
        source_file = path.name
    action_counts = report.get("action_counts", {}) if isinstance(report.get("action_counts", {}), dict) else {}
    action_markup = "".join(
        f"<div><span>{html.escape(key.replace('_', ' ').title())}</span><strong>{html.escape(str(value))}</strong></div>"
        for key, value in sorted(action_counts.items())
    ) or "<div><span>Actions</span><strong>None recorded</strong></div>"
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    body = f"""
      {support_agent_nav()}
      <div class="ops-layout section-gap">
        <aside class="card ops-sidebar">
          <h2 class="section-title">{html.escape(str(report.get('title', 'Fulfillment Support Review')))}</h2>
          <div class="report-meta-list">
            <div><span>Generated</span><strong>{html.escape(format_timestamp(str(report.get('generated_at', ''))))}</strong></div>
            <div><span>Status</span><strong>{html.escape(str(report.get('status', 'unknown')))}</strong></div>
            <div><span>Candidate threads</span><strong>{html.escape(str(report.get('candidate_count', 0)))}</strong></div>
          </div>
          <div class="report-meta-list">{action_markup}</div>
          <p><a href="{fulfillment_cs_base_path()}/reports/">Back to report library</a></p>
          <p class="hint">Source file: {html.escape(str(source_file))}</p>
        </aside>
        <article class="card report-content">
          <h2 class="section-title">Candidate Threads</h2>
          {render_support_agent_candidates(report.get('candidates', []))}
        </article>
      </div>
    """
    return page_shell(
        title=f"Anata Fulfillment CS - {report.get('title', 'Fulfillment Support Review')}",
        eyebrow="Fulfillment CS",
        heading=str(report.get("title", "Fulfillment Support Review")),
        intro="Read-only support review detail with current candidate threads and draft next actions.",
        status_block=status_block,
        body=body,
    )


def support_agent_not_found_page(message: str) -> str:
    body = f"""
      {support_agent_nav()}
      <div class="card section-gap">
        <h2 class="section-title">Not Found</h2>
        <p>{html.escape(message)}</p>
      </div>
    """
    return page_shell(
        title="Anata Fulfillment CS - Not Found",
        eyebrow="Fulfillment CS",
        heading="Support Report Not Found",
        intro="The requested support review artifact could not be located.",
        status_block="",
        body=body,
    )


def sales_nav() -> str:
    return """
      <div class="toolbar">
        <p class="hint">HubSpot is the source of truth. This operator layer reads the live pipeline and applies only high-confidence changes.</p>
        <div class="ops-nav">
          <a href="/admin/sales/">Sales OS</a>
          <a href="/admin/sales/deals/create">Create Deal</a>
          <a href="/admin/api/sales/dashboard">Sales API</a>
          <a href="/admin/fulfillment-cs/">Fulfillment CS</a>
          <a href="/">AP Inbox</a>
        </div>
      </div>
    """


def sales_status_message(query: Dict[str, str]) -> str:
    status = query.get("status", "")
    if status == "created":
        deal_id = query.get("deal_id", "").strip()
        return f"HubSpot deal created{f': {deal_id}' if deal_id else ''}."
    if status == "hubspot-not-configured":
        return "HubSpot is not configured for this environment."
    if status == "bad-request":
        return "Deal creation request was incomplete."
    if status == "hubspot-error":
        return "HubSpot rejected or failed the request."
    if status == "writeback-previewed":
        return "Sales write-back preview generated."
    if status == "writeback-applied":
        return "High-confidence sales write-back actions were applied."
    return ""


def _format_sales_money(amount: Optional[float]) -> str:
    if amount is None:
        return "Missing"
    return f"${amount:,.0f}"


def _format_sales_relative_timestamp(value: str) -> str:
    if not value:
        return "Unknown"
    try:
        current = datetime.now(timezone.utc)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return format_timestamp(value)
    delta = current - parsed.astimezone(timezone.utc)
    if delta < timedelta(hours=24):
        return f"{max(int(delta.total_seconds() // 3600), 0)}h ago"
    return f"{max(delta.days, 0)}d ago"


def _sales_badge(status: str) -> str:
    normalized = str(status).strip().lower()
    if normalized in {"won", "applied", "healthy", "live"}:
        badge_class = "badge-good"
    elif normalized in {"lost", "critical", "blocked"}:
        badge_class = "badge-warn"
    else:
        badge_class = "badge-muted"
    return f"<span class='badge {badge_class}'>{html.escape(status)}</span>"


def _sales_stage_markup(snapshot: Dict[str, Any]) -> str:
    stages = snapshot.get("pipeline", {}).get("stages", [])
    cards = []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        cards.append(
            f"""
              <article class="report-card">
                <p class="eyebrow">{html.escape(str(stage.get('status', 'open')).title())}</p>
                <h3>{html.escape(str(stage.get('label', 'Unknown stage')))}</h3>
                <p class="muted">{int(stage.get('dealCount', 0) or 0)} deal(s) · {_format_sales_money(float(stage.get('totalAmount', 0) or 0))}</p>
                <p>{int(stage.get('needsAttentionCount', 0) or 0)} need attention in this stage.</p>
              </article>
            """
        )
    return "".join(cards) or "<p class='hint'>No live pipeline stages were returned.</p>"


def _sales_object_definition_markup(snapshot: Dict[str, Any]) -> str:
    object_definitions = snapshot.get("objectDefinitions", {})
    cards = []
    for object_name in ("contact", "company", "deal", "deck", "audit", "quote", "task", "communication"):
        definition = object_definitions.get(object_name, {})
        if not isinstance(definition, dict):
            continue
        fields = definition.get("required_fields", [])
        rules = definition.get("rules", {})
        notes = []
        if fields:
            notes.append(f"Required: {', '.join(str(field) for field in fields)}")
        if "system_of_record" in definition:
            notes.append(f"Source: {definition['system_of_record']}")
        if rules:
            enabled_rules = [key.replace("_", " ") for key, enabled in rules.items() if enabled]
            if enabled_rules:
                notes.append(f"Rules: {', '.join(enabled_rules)}")
        if "create_when" in definition:
            notes.append(f"Create when: {definition['create_when']}")
        cards.append(
            f"""
              <article class="report-card">
                <p class="eyebrow">{html.escape(object_name.title())}</p>
                <h3>{html.escape(object_name.title())}</h3>
                <p>{html.escape(' · '.join(notes) if notes else 'Definition captured with no extra notes yet.')}</p>
              </article>
            """
        )
    return "".join(cards) or "<p class='hint'>Object definitions are not available.</p>"


def _sales_recent_deals_markup(snapshot: Dict[str, Any]) -> str:
    deals = snapshot.get("recentDeals", [])
    if not deals:
        return "<p class='hint'>No recent deals were returned from HubSpot.</p>"
    cards = []
    for deal in deals:
        if not isinstance(deal, dict):
            continue
        missing_fields = deal.get("missingFields", [])
        missing_text = ", ".join(str(field) for field in missing_fields) if missing_fields else "No critical gaps detected."
        next_step = str(deal.get("nextStep") or "No next step")
        link = str(deal.get("url") or "")
        title = html.escape(str(deal.get("name", "Unnamed deal")))
        title_markup = f"<a href=\"{html.escape(link, quote=True)}\">{title}</a>" if link else title
        cards.append(
            f"""
              <article class="feedback-item">
                <p class="eyebrow">{html.escape(str(deal.get('primaryOffer', 'Unclassified')))}</p>
                <h3>{title_markup}</h3>
                <p class="muted">{html.escape(str(deal.get('company', 'No company')))} · {html.escape(str(deal.get('contact', 'No contact')))}</p>
                <p>{_sales_badge(str(deal.get('stageStatus', 'open')).title())} {_format_sales_money(deal.get('amount'))} · {html.escape(str(deal.get('stage', 'Unknown stage')))}</p>
                <p><strong>Owner:</strong> {html.escape(str(deal.get('owner', 'Unassigned')))}</p>
                <p><strong>Next step:</strong> {html.escape(next_step)}</p>
                <p><strong>Missing:</strong> {html.escape(missing_text)}</p>
                <p class="hint">Updated {_format_sales_relative_timestamp(str(deal.get('updatedAt', '') or ''))}</p>
              </article>
            """
        )
    return "".join(cards)


def _sales_writeback_markup(writeback: Optional[Dict[str, Any]]) -> str:
    if not writeback:
        return "<p class='hint'>Run a preview to see the first autonomous action layer against live HubSpot deals.</p>"
    summary = writeback.get("summary", {})
    deals = writeback.get("deals", [])
    cards = [
        render_stat_card("Mode", str(writeback.get("mode", "preview")).title(), "Preview shows candidate changes. Apply writes only high-confidence actions."),
        render_stat_card("Candidates", str(int(summary.get("candidateDeals", 0) or 0)), "Deals with action candidates in this run."),
        render_stat_card("Applied", str(int(summary.get("appliedActions", 0) or 0)), "Direct updates, notes, and tasks written this run."),
        render_stat_card("Deferred", str(int(summary.get("deferredActions", 0) or 0)), "Candidate changes that stayed below the apply threshold."),
    ]
    deal_cards = []
    for deal in deals:
        if not isinstance(deal, dict):
            continue
        action_items = []
        for action in deal.get("actions", []):
            if not isinstance(action, dict):
                continue
            action_items.append(
                f"<li class='detail-item'>{html.escape(str(action.get('type', 'action')))} · {html.escape(str(action.get('status', 'preview')))} · {html.escape(str(action.get('reason', '')))}</li>"
            )
        reason_items = []
        for reason in deal.get("inference", {}).get("reasons", []):
            reason_items.append(f"<li class='detail-item'>{html.escape(str(reason))}</li>")
        deal_cards.append(
            f"""
              <article class="report-card">
                <p class="eyebrow">{html.escape(str(deal.get('stageStatus', 'open')).title())}</p>
                <h3>{html.escape(str(deal.get('dealName', 'Unnamed deal')))}</h3>
                <p class="muted">{html.escape(str(deal.get('companyName', 'No company')))} · {html.escape(str(deal.get('stage', 'Unknown stage')))}</p>
                <p><strong>Current service type:</strong> {html.escape(str(deal.get('current', {}).get('serviceType') or 'Blank'))}</p>
                <p><strong>Current next step:</strong> {html.escape(str(deal.get('current', {}).get('nextStep') or 'Blank'))}</p>
                <p><strong>Inference:</strong> {html.escape(str(deal.get('inference', {}).get('primaryOffer') or 'Unclassified'))} ({round(float(deal.get('inference', {}).get('confidence', 0.0) or 0.0) * 100)}%)</p>
                <ul class="detail-list">{''.join(action_items) or "<li class='detail-item'>No actions were recorded.</li>"}</ul>
                <p class="hint">Signals:</p>
                <ul class="detail-list">{''.join(reason_items) or "<li class='detail-item'>No signals were recorded.</li>"}</ul>
              </article>
            """
        )
    return f"""
      <div class="grid section-gap">{''.join(cards)}</div>
      <div class="report-list section-gap">{''.join(deal_cards) if deal_cards else "<p class='hint'>No candidate deals were returned from this write-back run.</p>"}</div>
    """


def sales_dashboard_page(status_message: str, snapshot: Optional[Dict[str, Any]] = None, writeback: Optional[Dict[str, Any]] = None) -> str:
    status_block = f"<p class='status-banner'>{html.escape(status_message)}</p>" if status_message else ""
    snapshot = snapshot or {}
    summary = snapshot.get("summary", {})
    schema = snapshot.get("schema", {})
    pipeline = snapshot.get("pipeline", {})
    stage_drift = snapshot.get("stageDrift", {})
    autonomy = snapshot.get("autonomy", {})
    body = f"""
      {sales_nav()}
      <div class="grid section-gap">
        {render_stat_card("Open pipeline", str(int(summary.get("openDeals", 0) or 0)), f"{_format_sales_money(summary.get('openAmount'))} in current open value")}
        {render_stat_card("Unclassified deals", str(int(summary.get("unclassifiedDeals", 0) or 0)), "Need confident service or software mapping.")}
        {render_stat_card("Missing next step", str(int(summary.get("openDealsMissingNextStep", 0) or 0)), "Open or nurture deals that still need follow-up guidance.")}
        {render_stat_card("Multi-offer signals", str(int(summary.get("multiOfferCandidates", 0) or 0)), "Candidates for second linked deals.")}
        {render_stat_card("Schema model", str(int(schema.get("properties", {}).get("deals", {}).get("customCount", 0) or 0) + int(schema.get("properties", {}).get("companies", {}).get("customCount", 0) or 0) + int(schema.get("properties", {}).get("contacts", {}).get("customCount", 0) or 0)), "Custom properties across commercial objects.")}
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">What Is Happening</h2>
          <div class="detail-list">{''.join(f"<li class='detail-item'>{html.escape(str(item))}</li>" for item in snapshot.get("directives", {}).get("happening", [])) or "<p class='hint'>No live directives yet.</p>"}</div>
        </section>
        <section class="card">
          <h2 class="section-title">What Is Broken</h2>
          <div class="detail-list">{''.join(f"<li class='detail-item'>{html.escape(str(item))}</li>" for item in snapshot.get("directives", {}).get("broken", [])) or "<p class='hint'>No live directives yet.</p>"}</div>
        </section>
        <section class="card">
          <h2 class="section-title">What Should Happen Next</h2>
          <div class="detail-list">{''.join(f"<li class='detail-item'>{html.escape(str(item))}</li>" for item in snapshot.get("directives", {}).get("next", [])) or "<p class='hint'>No next directives yet.</p>"}</div>
        </section>
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Live Pipeline</h2>
          <p class="muted">Portal {html.escape(str(snapshot.get('portalId', '') or 'Unknown'))} · {html.escape(str(pipeline.get('label', 'HubSpot pipeline')))} / {html.escape(str(pipeline.get('id', '')))}</p>
          <p class="hint">{int(pipeline.get('liveStageCount', 0) or 0)} live stages · {int(pipeline.get('targetStageCount', 0) or 0)} target stages</p>
          <div class="report-list section-gap">{_sales_stage_markup(snapshot)}</div>
        </section>
        <section class="card">
          <h2 class="section-title">Object Definitions</h2>
          <p class="muted">Current commercial truth across HubSpot, decks, audits, and communications.</p>
          <div class="report-list section-gap">{_sales_object_definition_markup(snapshot)}</div>
        </section>
      </div>
      <div class="grid section-gap">
        <section class="card">
          <h2 class="section-title">Autonomy Policy</h2>
          <p class="muted">{html.escape(str(autonomy.get('autonomy_mode', 'high_confidence_only')).replace('_', ' '))}</p>
          <p><strong>Agent can:</strong> {html.escape(', '.join(str(item).replace('_', ' ') for item in autonomy.get('agent_can', [])))}</p>
          <p><strong>When not confident:</strong> {html.escape(', '.join(str(item).replace('_', ' ') for item in autonomy.get('when_not_confident', [])))}</p>
          <p class="hint">High-confidence threshold: {round(float(schema.get('confidencePolicy', {}).get('highThreshold', 0.0) or 0.0) * 100)}% · Medium threshold: {round(float(schema.get('confidencePolicy', {}).get('mediumThreshold', 0.0) or 0.0) * 100)}%</p>
        </section>
        <section class="card">
          <h2 class="section-title">Stage Drift</h2>
          <p><strong>Target only:</strong> {html.escape(', '.join(str(item) for item in stage_drift.get('targetOnly', [])) or 'None')}</p>
          <p><strong>Live only:</strong> {html.escape(', '.join(str(item) for item in stage_drift.get('liveOnly', [])) or 'None')}</p>
          <p class="hint">This is where the shared operating model still differs from the current live HubSpot pipeline labels.</p>
        </section>
      </div>
      <div class="card section-gap">
        <h2 class="section-title">First Write-Back Action Layer</h2>
        <p class="muted">Preview candidate actions first. Apply writes only high-confidence deal updates plus the supporting notes and review tasks.</p>
        <form action="/admin/sales/actions/writeback" method="post" class="sales-inline-form">
          <label for="sales-writeback-limit">Candidate limit</label>
          <input id="sales-writeback-limit" name="limit" type="text" value="10">
          <div class="sales-button-row">
            <button type="submit" name="mode" value="preview">Preview write-back</button>
            <button type="submit" name="mode" value="apply" class="ghost">Apply high-confidence actions</button>
          </div>
        </form>
        {_sales_writeback_markup(writeback)}
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Recent Deals</h2>
        <p class="muted">Latest commercial records from the live pipeline with the current missing-field audit.</p>
        <div class="report-list section-gap">{_sales_recent_deals_markup(snapshot)}</div>
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Next Operator Action</h2>
        <p><a href="/admin/sales/deals/create">Create a HubSpot deal</a></p>
        <p><a href="/admin/api/sales/dashboard">Open the dashboard JSON contract</a></p>
        <p class="hint">Deal creation is live today. Deck, audit, and quote creation remain downstream layers after this dashboard and write-back surface.</p>
      </div>
    """
    return page_shell(
        title="Anata Sales OS",
        eyebrow="Sales OS",
        heading="Sales OS",
        intro="Commercial operator surface for the live HubSpot pipeline, object definitions, and the first autonomous write-back layer.",
        status_block=status_block,
        body=body,
    )


def _input_value(values: Dict[str, Any], key: str) -> str:
    return html.escape(str(values.get(key, "") or ""), quote=True)


def sales_deal_create_page(
    status_message: str = "",
    *,
    errors: Optional[List[str]] = None,
    values: Optional[Dict[str, Any]] = None,
) -> str:
    errors = errors or []
    values = values or {}
    status_parts = []
    if status_message:
        status_parts.append(f"<p class='status-banner'>{html.escape(status_message)}</p>")
    if errors:
        items = "".join(f"<li>{html.escape(error)}</li>" for error in errors)
        status_parts.append(f"<div class='status-banner'><strong>Deal not created.</strong><ul>{items}</ul></div>")
    status_block = "".join(status_parts)
    body = f"""
      {sales_nav()}
      <div class="grid section-gap">
        <section class="card card-form">
          <h2 class="section-title">Create HubSpot Deal</h2>
          <form action="/admin/sales/deals/create" method="post">
            <label for="dealname">Deal name</label>
            <input id="dealname" name="dealname" type="text" value="{_input_value(values, 'dealname')}" required>

            <label class="label-spaced" for="pipeline">Pipeline</label>
            <input id="pipeline" name="pipeline" type="text" value="{_input_value(values, 'pipeline')}" placeholder="Default can come from HUBSPOT_DEFAULT_DEAL_PIPELINE" required>

            <label class="label-spaced" for="dealstage">Deal stage</label>
            <input id="dealstage" name="dealstage" type="text" value="{_input_value(values, 'dealstage')}" placeholder="Default can come from HUBSPOT_DEFAULT_DEAL_STAGE" required>

            <label class="label-spaced" for="anata_service_line">Service line</label>
            <input id="anata_service_line" name="anata_service_line" type="text" value="{_input_value(values, 'anata_service_line')}" placeholder="fulfillment" required>

            <label class="label-spaced" for="anata_lead_source_detail">Lead source detail</label>
            <input id="anata_lead_source_detail" name="anata_lead_source_detail" type="text" value="{_input_value(values, 'anata_lead_source_detail')}" placeholder="website" required>

            <label class="label-spaced" for="hubspot_owner_id">HubSpot owner ID</label>
            <input id="hubspot_owner_id" name="hubspot_owner_id" type="text" value="{_input_value(values, 'hubspot_owner_id')}" placeholder="Default can come from HUBSPOT_DEFAULT_OWNER_ID" required>

            <label class="label-spaced" for="company_id">HubSpot company ID</label>
            <input id="company_id" name="company_id" type="text" value="{_input_value(values, 'company_id')}" required>

            <label class="label-spaced" for="contact_id">HubSpot contact ID</label>
            <input id="contact_id" name="contact_id" type="text" value="{_input_value(values, 'contact_id')}" required>

            <label class="label-spaced" for="amount">Amount</label>
            <input id="amount" name="amount" type="text" value="{_input_value(values, 'amount')}">

            <label class="label-spaced" for="closedate">Close date</label>
            <input id="closedate" name="closedate" type="date" value="{_input_value(values, 'closedate')}">

            <label class="label-spaced" for="anata_next_step">Next step</label>
            <input id="anata_next_step" name="anata_next_step" type="text" value="{_input_value(values, 'anata_next_step')}">

            <button type="submit">Create HubSpot Deal</button>
          </form>
        </section>
        <section class="card">
          <h2 class="section-title">Validation Contract</h2>
          <p class="muted">Before writing to HubSpot, this route loads `config/hubspot_sales_rules.json` and validates required deal properties plus required company/contact associations.</p>
          <p class="hint">Quote creation stays disabled until deck and quote readiness rules are implemented.</p>
        </section>
      </div>
    """
    return page_shell(
        title="Anata Sales OS - Create Deal",
        eyebrow="Sales OS",
        heading="Create HubSpot Deal",
        intro="Create the opportunity record first. Quotes and decks depend on a clean deal model.",
        status_block=status_block,
        body=body,
    )


def sales_deal_created_redirect(deal: Dict[str, Any]) -> str:
    if deal.get("hubspot_url"):
        return str(deal["hubspot_url"])
    query = urlencode({"status": "created", "deal_id": str(deal.get("id", ""))})
    return f"/admin/sales/?{query}"


def sales_os_guard_page(requested_path: str) -> str:
    body = f"""
      <div class="toolbar">
        <p class="hint">This HubSpot sales route is specified, but the handler is not wired yet.</p>
        <div class="ops-nav">
          <a href="/admin/sales/">Sales OS</a>
          <a href="/admin/sales/deals/create">Create Deal</a>
          <a href="{fulfillment_cs_base_path()}/">Fulfillment CS</a>
          <a href="/admin/website-ops">Website Ops</a>
          <a href="/">AP Inbox</a>
        </div>
      </div>
      <div class="grid section-gap">
        {render_stat_card("Requested route", requested_path, "The operator action that was attempted.")}
        {render_stat_card("HubSpot API", "Partially wired", "Deal creation is live; this downstream action is still guarded.")}
        {render_stat_card("Operator action", "Blocked", "Do not assume a HubSpot record was created.")}
      </div>
      <div class="card section-gap">
        <h2 class="section-title">Blocked Commercial Flow</h2>
        <p>Deal creation is available at `/admin/sales/deals/create`, but quote and deck creation still require quote readiness validation, deck artifact persistence, and redirect targets after successful writes.</p>
      </div>
    """
    return page_shell(
        title="Anata Sales OS - Not Wired",
        eyebrow="Sales OS",
        heading="Commercial Flow Blocked",
        intro="This route is intentionally guarded because the downstream commercial flow is not implemented yet.",
        status_block="<p class='status-banner'>No HubSpot action was performed.</p>",
        body=body,
    )


def parse_sales_deal_ids(value: Any) -> List[str]:
    raw = str(value or "").replace("\n", ",")
    return [item.strip() for item in raw.split(",") if item.strip()]


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
    ensure_website_ops_storage()
    ensure_support_agent_storage()
    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/")
    query = parse_query_string(environ)
    metadata = current_metadata(root)

    if method == "GET" and path.startswith("/static/"):
        asset_path = (STATIC_DIR / path.removeprefix("/static/")).resolve()
        if STATIC_DIR.resolve() not in asset_path.parents and asset_path != STATIC_DIR.resolve():
            return text_response(start_response, "404 Not Found", "Not Found")
        if not asset_path.exists() or not asset_path.is_file():
            return text_response(start_response, "404 Not Found", "Not Found")
        content_type, _ = mimetypes.guess_type(str(asset_path))
        return response(
            start_response,
            "200 OK",
            asset_path.read_bytes(),
            [("Content-Type", content_type or "application/octet-stream"), ("Cache-Control", "public, max-age=300")],
        )

    if is_protected_website_ops_path(path) or is_protected_admin_path(path):
        auth_response = require_admin_request(environ, start_response)
        if auth_response is not None:
            return auth_response

    if path == "/website-ops":
        return redirect_response(start_response, "/website-ops/")

    if path == "/admin/website-ops":
        return redirect_response(start_response, "/website-ops/")

    if path == "/admin/sales":
        return redirect_response(start_response, "/admin/sales/")

    if path in {"/support-agent", "/admin/support-agent", fulfillment_cs_base_path()}:
        return redirect_response(start_response, f"{fulfillment_cs_base_path()}/")

    if method == "GET" and path == "/admin/sales/":
        try:
            snapshot = hubspot_sales_os.get_sales_dashboard_snapshot()
            body = sales_dashboard_page(sales_status_message(query), snapshot=snapshot)
            return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")
        except hubspot_sales.HubSpotSalesError as exc:
            body = sales_dashboard_page(str(exc))
            return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")
        except Exception as exc:
            body = sales_dashboard_page(f"Sales dashboard failed to load: {exc}")
            return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/admin/api/sales/dashboard":
        try:
            snapshot = hubspot_sales_os.get_sales_dashboard_snapshot(force_refresh=True)
        except hubspot_sales.HubSpotSalesError as exc:
            return json_response(start_response, "503 Service Unavailable", {"ok": False, "error": str(exc), "hubspot": exc.payload})
        except Exception as exc:
            return json_response(start_response, "500 Internal Server Error", {"ok": False, "error": str(exc)})
        return json_response(start_response, "200 OK", {"ok": True, "snapshot": snapshot})

    if method == "POST" and path in {"/admin/sales/actions/writeback", "/admin/api/sales/writeback"}:
        try:
            payload = parse_feedback_request(environ)
        except (json.JSONDecodeError, ValueError):
            if wants_json_response(environ) or path.startswith("/admin/api/"):
                return json_response(start_response, "400 Bad Request", {"ok": False, "error": "bad-request"})
            body = sales_dashboard_page("Sales write-back request was incomplete.")
            return text_response(start_response, "400 Bad Request", body, "text/html; charset=utf-8")
        mode = str(payload.get("mode", "preview") or "preview").strip().lower()
        if mode not in {"preview", "apply"}:
            mode = "preview"
        try:
            limit = int(str(payload.get("limit", "10") or "10"))
        except ValueError:
            limit = 10
        try:
            result = hubspot_sales_os.run_writeback(
                mode=mode,
                deal_ids=parse_sales_deal_ids(payload.get("deal_ids")),
                limit=limit,
            )
            snapshot = hubspot_sales_os.get_sales_dashboard_snapshot(force_refresh=(mode == "apply"))
        except hubspot_sales.HubSpotSalesError as exc:
            if wants_json_response(environ) or path.startswith("/admin/api/"):
                return json_response(start_response, "503 Service Unavailable", {"ok": False, "error": str(exc), "hubspot": exc.payload})
            body = sales_dashboard_page(str(exc))
            return text_response(start_response, "503 Service Unavailable", body, "text/html; charset=utf-8")
        except Exception as exc:
            if wants_json_response(environ) or path.startswith("/admin/api/"):
                return json_response(start_response, "500 Internal Server Error", {"ok": False, "error": str(exc)})
            body = sales_dashboard_page(f"Sales write-back failed: {exc}")
            return text_response(start_response, "500 Internal Server Error", body, "text/html; charset=utf-8")
        if wants_json_response(environ) or path.startswith("/admin/api/"):
            return json_response(start_response, "200 OK", {"ok": True, "result": result, "snapshot": snapshot})
        status_message = "High-confidence sales write-back actions were applied." if mode == "apply" else "Sales write-back preview generated."
        body = sales_dashboard_page(status_message, snapshot=snapshot, writeback=result)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/admin/sales/deals/create":
        body = sales_deal_create_page(sales_status_message(query))
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "POST" and path == "/admin/sales/deals/create":
        if not request_is_admin_authenticated(environ):
            return redirect_response(start_response, "/?status=unauthorized")
        try:
            payload = parse_feedback_request(environ)
            rules = hubspot_sales.read_sales_rules()
            deal_request = hubspot_sales.normalize_deal_create_request(payload, rules)
        except (json.JSONDecodeError, ValueError):
            if wants_json_response(environ):
                return json_response(start_response, "400 Bad Request", {"ok": False, "error": "bad-request"})
            body = sales_deal_create_page("Deal creation request was incomplete.", values={})
            return text_response(start_response, "400 Bad Request", body, "text/html; charset=utf-8")
        except hubspot_sales.HubSpotSalesError as exc:
            if wants_json_response(environ):
                return json_response(start_response, "500 Internal Server Error", {"ok": False, "error": str(exc)})
            body = sales_deal_create_page(str(exc), values={key: str(value) for key, value in payload.items()})
            return text_response(start_response, "500 Internal Server Error", body, "text/html; charset=utf-8")
        validation_errors = hubspot_sales.validate_deal_create_request(deal_request, rules)
        if validation_errors:
            if wants_json_response(environ):
                return json_response(
                    start_response,
                    "400 Bad Request",
                    {"ok": False, "error": "validation-failed", "errors": validation_errors},
                )
            body = sales_deal_create_page(
                "Deal creation request failed validation.",
                errors=validation_errors,
                values={key: str(value) for key, value in payload.items()},
            )
            return text_response(start_response, "400 Bad Request", body, "text/html; charset=utf-8")
        try:
            deal = hubspot_sales.create_deal(deal_request)
        except hubspot_sales.HubSpotSalesError as exc:
            status = "503 Service Unavailable" if exc.status_code == 503 else "502 Bad Gateway"
            if wants_json_response(environ):
                return json_response(
                    start_response,
                    status,
                    {"ok": False, "error": str(exc), "hubspot": exc.payload},
                )
            body = sales_deal_create_page(
                str(exc),
                errors=[str(exc)],
                values={key: str(value) for key, value in payload.items()},
            )
            return text_response(start_response, status, body, "text/html; charset=utf-8")
        if wants_json_response(environ):
            return json_response(start_response, "201 Created", {"ok": True, "deal": deal})
        return redirect_response(start_response, sales_deal_created_redirect(deal))

    if method in {"GET", "POST"} and path.startswith("/admin/sales/"):
        body = sales_os_guard_page(path)
        return text_response(start_response, "501 Not Implemented", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/website-ops/":
        latest_report = latest_report_entry()
        feedback_entries = load_feedback_submissions()
        body = website_ops_dashboard_page(website_ops_status_message(query), latest_report, feedback_entries)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path in {"/support-agent/", "/admin/support-agent/", f"{fulfillment_cs_base_path()}/"}:
        latest_report_path = support_agent_latest_report_path()
        latest_report = load_support_agent_report(latest_report_path) if latest_report_path else None
        body = support_agent_dashboard_page("", latest_report)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path in {"/website-ops/reports", "/website-ops/reports/"}:
        body = website_ops_reports_index_page(website_ops_status_message(query))
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path in {
        "/support-agent/reports",
        "/support-agent/reports/",
        "/admin/support-agent/reports",
        "/admin/support-agent/reports/",
        f"{fulfillment_cs_base_path()}/reports",
        f"{fulfillment_cs_base_path()}/reports/",
    }:
        body = support_agent_reports_index_page("")
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/website-ops/reports/latest":
        latest_report = latest_report_entry()
        if not latest_report:
            return redirect_response(start_response, "/website-ops/reports/?status=report-not-found")
        return redirect_response(start_response, latest_report["url"])

    if method == "GET" and path in {
        "/support-agent/reports/latest",
        "/admin/support-agent/reports/latest",
        f"{fulfillment_cs_base_path()}/reports/latest",
    }:
        latest_report = support_agent_latest_timestamped_report_entry()
        if not latest_report:
            return redirect_response(start_response, f"{fulfillment_cs_base_path()}/reports/")
        return redirect_response(start_response, latest_report["url"])

    if method == "GET" and path.startswith("/website-ops/reports/"):
        report_path = website_ops_report_path_from_route(path)
        if report_path is None:
            return text_response(start_response, "404 Not Found", website_ops_not_found_page("The requested report was not found."), "text/html; charset=utf-8")
        if report_path.is_dir():
            body = website_ops_report_category_page(report_path.name, website_ops_status_message(query))
            return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")
        body = website_ops_report_detail_page(report_path, website_ops_status_message(query))
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and (
        path.startswith("/support-agent/reports/")
        or path.startswith("/admin/support-agent/reports/")
        or path.startswith(f"{fulfillment_cs_base_path()}/reports/")
    ):
        artifact_path = support_agent_report_artifact_path_from_route(path)
        if artifact_path is not None:
            return response(
                start_response,
                "200 OK",
                artifact_path.read_bytes(),
                [("Content-Type", support_agent_report_artifact_content_type(artifact_path)), ("Cache-Control", "no-store")],
            )
        report_path = support_agent_report_path_from_route(path)
        if report_path is None:
            return text_response(start_response, "404 Not Found", support_agent_not_found_page("The requested support report was not found."), "text/html; charset=utf-8")
        body = support_agent_report_detail_page(report_path, "")
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path in {"/website-ops/feedback", "/website-ops/feedback/"}:
        body = website_ops_feedback_page(website_ops_status_message(query), load_feedback_submissions())
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path in {"/website-ops/queue", "/website-ops/queue/"}:
        body = website_ops_queue_page(website_ops_status_message(query))
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "POST" and path == "/website-ops/feedback":
        try:
            payload = parse_feedback_request(environ)
        except (json.JSONDecodeError, ValueError):
            if wants_json_response(environ):
                return json_response(start_response, "400 Bad Request", {"ok": False, "error": "bad-json"})
            return redirect_response(start_response, "/website-ops/feedback?status=bad-json")
        if not str(payload.get("category", "")).strip() or not str(payload.get("summary", "")).strip():
            if wants_json_response(environ):
                return json_response(
                    start_response,
                    "400 Bad Request",
                    {"ok": False, "error": "missing-feedback", "fields": ["category", "summary"]},
                )
            return redirect_response(start_response, "/website-ops/feedback?status=missing-feedback")
        record = save_feedback_submission(payload, environ)
        if wants_json_response(environ):
            public_record = {key: value for key, value in record.items() if not key.startswith("_")}
            return json_response(start_response, "201 Created", {"ok": True, "record": public_record})
        return redirect_response(start_response, "/website-ops/feedback?status=submitted")

    if method == "GET" and path in {"/website-ops/feedback/submissions", "/website-ops/feedback/submissions/"}:
        body = website_ops_feedback_submissions_page(website_ops_status_message(query))
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path.startswith("/website-ops/feedback/submissions/"):
        submission_id = path.rstrip("/").rsplit("/", 1)[-1]
        record = load_feedback_submission(submission_id)
        if not record:
            return text_response(start_response, "404 Not Found", website_ops_not_found_page("The requested feedback submission was not found."), "text/html; charset=utf-8")
        body = website_ops_feedback_submission_detail(record)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "POST" and path.startswith("/website-ops/feedback/submissions/") and path.endswith("/status"):
        submission_id = path.removeprefix("/website-ops/feedback/submissions/").removesuffix("/status").strip("/")
        payload = parse_feedback_request(environ)
        record = update_feedback_submission(submission_id, payload, environ)
        if not record:
            if wants_json_response(environ):
                return json_response(start_response, "404 Not Found", {"ok": False, "error": "submission-not-found"})
            return redirect_response(start_response, "/website-ops/queue?status=submission-not-found")
        if wants_json_response(environ):
            public_record = {key: value for key, value in record.items() if not key.startswith("_")}
            return json_response(start_response, "200 OK", {"ok": True, "record": public_record})
        return redirect_response(start_response, f"/website-ops/feedback/submissions/{submission_id}?status=review-updated")

    if method == "GET" and path in {"/website-ops/backups", "/website-ops/backups/"}:
        body = website_ops_backup_index_page()
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path.startswith("/website-ops/backups/"):
        backup_path = website_ops_backup_path_from_route(path)
        if not backup_path:
            return text_response(start_response, "404 Not Found", website_ops_not_found_page("The requested backup set was not found."), "text/html; charset=utf-8")
        body = website_ops_backup_detail_page(backup_path)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/health":
        return json_response(
            start_response,
            "200 OK",
            {
                "ok": True,
                "admin_login_enabled": admin_login_enabled(),
                "latest_upload": metadata,
                "machine_download_url": latest_download_url(environ, ""),
                "machine_token_configured": bool(machine_token()),
            },
        )

    if method == "GET" and path in {"/", "/index.html"}:
        status_message = login_status_message(query)
        missing_admin_env = admin_auth_missing_env()
        if missing_admin_env and not unauthenticated_local_bypass_enabled():
            return auth_configuration_error_response(start_response, missing_admin_env)
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
        supplied_token = request_token(environ)
        if supplied_token:
            if not token_is_valid(supplied_token):
                return text_response(start_response, "401 Unauthorized", "Unauthorized")
        else:
            auth_response = require_admin_request(environ, start_response)
            if auth_response is not None:
                return auth_response
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
        missing_admin_env = admin_auth_missing_env()
        if missing_admin_env and not unauthenticated_local_bypass_enabled():
            return auth_configuration_error_response(start_response, missing_admin_env)
        if form.get("username", "").strip() != admin_username() or form.get("password", "") != admin_password():
            return redirect_response(start_response, "/?status=bad-login")
        expires_at = int((datetime.now(timezone.utc) + timedelta(seconds=SESSION_TTL_SECONDS)).timestamp())
        cookie_header = set_cookie_header(environ, sign_session(admin_username(), expires_at))
        return redirect_response(start_response, "/", headers=[("Set-Cookie", cookie_header)])

    if method == "POST" and path == "/logout":
        return redirect_response(start_response, "/?status=logged-out", headers=[("Set-Cookie", clear_cookie_header(environ))])

    if method == "POST" and path == "/upload":
        auth_response = require_admin_request(environ, start_response)
        if auth_response is not None:
            return auth_response
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
