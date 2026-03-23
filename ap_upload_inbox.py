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
from datetime import datetime, timedelta, timezone
from email.parser import BytesParser
from email.policy import default
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import parse_qs, urlencode
from wsgiref.simple_server import make_server


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_STORAGE_DIR = ROOT_DIR / "data" / "upload_inbox"
LATEST_FILENAME = "latest.csv"
LATEST_METADATA_FILENAME = "latest.json"
ARCHIVE_DIRNAME = "archive"
DEFAULT_MAX_BYTES = 10 * 1024 * 1024
ACCEPTED_EXTENSIONS = {".csv"}
SESSION_COOKIE_NAME = "ap_upload_session"
SESSION_TTL_SECONDS = 12 * 60 * 60


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
  <style>
    :root {{
      --bg: #f2efe7;
      --panel: #fffdf7;
      --ink: #18202a;
      --muted: #6f726c;
      --accent: #0e6e58;
      --accent-2: #c96c31;
      --line: #d8d0bf;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(201,108,49,.16), transparent 30%),
        linear-gradient(180deg, #f7f4ec, var(--bg));
    }}
    main {{
      max-width: 860px;
      margin: 48px auto;
      padding: 24px;
    }}
    .hero {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(24,32,42,.08);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(2rem, 4vw, 3rem);
      line-height: 1;
    }}
    p {{ line-height: 1.5; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-top: 24px;
    }}
    .card {{
      background: rgba(255,255,255,.88);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px;
    }}
    label {{
      display: block;
      margin-bottom: 6px;
      font-size: 0.95rem;
      color: var(--muted);
    }}
    input[type="text"],
    input[type="password"],
    input[type="file"] {{
      width: 100%;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      font: inherit;
    }}
    button {{
      appearance: none;
      border: 0;
      border-radius: 999px;
      padding: 12px 18px;
      background: var(--accent);
      color: #fff;
      font: inherit;
      cursor: pointer;
      margin-top: 14px;
    }}
    .ghost {{
      background: transparent;
      border: 1px solid var(--line);
      color: var(--ink);
    }}
    .status {{
      background: rgba(14,110,88,.08);
      border-left: 4px solid var(--accent);
      padding: 12px 14px;
      border-radius: 12px;
      margin-top: 18px;
    }}
    .hint {{
      color: var(--muted);
      margin-top: 0;
    }}
    .metric {{
      margin: 10px 0;
      padding-bottom: 10px;
      border-bottom: 1px dashed var(--line);
    }}
    .metric strong {{
      display: block;
      text-transform: uppercase;
      letter-spacing: .08em;
      font-size: .78rem;
      color: var(--muted);
      margin-bottom: 4px;
    }}
    .toolbar {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
    }}
    .toolbar form {{
      margin: 0;
    }}
    code {{
      background: rgba(24,32,42,.06);
      padding: 2px 6px;
      border-radius: 6px;
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p style="margin:0;color:var(--accent-2);text-transform:uppercase;letter-spacing:.12em;font-size:.78rem;">{html.escape(eyebrow)}</p>
      <h1>{html.escape(heading)}</h1>
      <p>{html.escape(intro)}</p>
      {status_block}
      {body}
    </section>
  </main>
</body>
</html>"""


def login_page(status_message: str) -> str:
    status_block = f"<p class='status'>{html.escape(status_message)}</p>" if status_message else ""
    body = """<div class="grid">
        <section class="card">
          <h2 style="margin-top:0;">Admin Login</h2>
          <p class="hint">Use the AP admin credentials once, then the browser keeps a signed session for future uploads.</p>
          <form action="/login" method="post">
            <label for="username">Username</label>
            <input id="username" name="username" type="text" autocomplete="username">
            <label for="password" style="margin-top:14px;">Password</label>
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


def upload_page(status_message: str, metadata: Dict[str, Any]) -> str:
    latest_name = html.escape(metadata.get("original_filename", "No file uploaded"))
    latest_uploaded_at = html.escape(format_timestamp(metadata.get("uploaded_at", "")))
    latest_size = metadata.get("byte_size", 0)
    status_block = f"<p class='status'>{html.escape(status_message)}</p>" if status_message else ""
    body = f"""<div class="toolbar">
        <p class="hint">Upload the newest bank-export CSV. The daily and weekly AP audits always fetch the current file from this inbox.</p>
        <form action="/logout" method="post">
          <button class="ghost" type="submit">Log Out</button>
        </form>
      </div>
      <div class="grid">
        <section class="card">
          <h2 style="margin-top:0;">Upload Current File</h2>
          <form action="/upload" method="post" enctype="multipart/form-data">
            <label for="transaction_file">Transactions CSV</label>
            <input id="transaction_file" name="transaction_file" type="file" accept=".csv,text/csv">
            <button type="submit">Upload Latest CSV</button>
          </form>
        </section>
        <section class="card">
          <h2 style="margin-top:0;">Current File</h2>
          <div class="metric"><strong>Filename</strong>{latest_name}</div>
          <div class="metric"><strong>Uploaded At</strong>{latest_uploaded_at}</div>
          <div class="metric"><strong>Size</strong>{latest_size:,} bytes</div>
          <p><a href="/latest.csv">Download current transactions CSV</a></p>
          <p class="hint">Cron jobs read <code>/latest.csv</code> using the machine token, but admins can also download it directly while logged in.</p>
        </section>
      </div>"""
    return page_shell(
        title="Anata AP Upload Inbox",
        eyebrow="Anata AP Intake",
        heading="Weekly Bank Export Inbox",
        intro="Submit the latest bank transactions CSV here instead of relying on a local file path.",
        status_block=status_block,
        body=body,
    )


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
            body = upload_page(status_message, metadata)
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
