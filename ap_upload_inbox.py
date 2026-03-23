#!/usr/bin/env python3
"""Internal AP transaction upload inbox."""

from __future__ import annotations

import html
import json
import os
import re
import shutil
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default
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


def storage_dir() -> Path:
    configured = os.getenv("AP_UPLOAD_STORAGE_DIR")
    return Path(configured) if configured else DEFAULT_STORAGE_DIR


def max_upload_bytes() -> int:
    raw = os.getenv("AP_UPLOAD_MAX_BYTES", str(DEFAULT_MAX_BYTES))
    try:
        return max(int(raw), 1024)
    except ValueError:
        return DEFAULT_MAX_BYTES


def configured_token() -> str:
    return os.getenv("AP_UPLOAD_TOKEN", "").strip()


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
    configured = configured_token()
    if not configured:
        return True
    return bool(token) and token == configured


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


def redirect_response(start_response: Any, location: str) -> Iterable[bytes]:
    return response(start_response, "303 See Other", b"", [("Location", location), ("Cache-Control", "no-store")])


def html_page(status_message: str, metadata: Dict[str, Any], authorized: bool, latest_url: str) -> str:
    latest_name = html.escape(metadata.get("original_filename", "No file uploaded"))
    latest_uploaded_at = html.escape(format_timestamp(metadata.get("uploaded_at", "")))
    latest_size = metadata.get("byte_size", 0)
    status_block = f"<p class='status'>{html.escape(status_message)}</p>" if status_message else ""
    download_block = (
        f"<p><a href='{html.escape(latest_url)}'>Download current transactions CSV</a></p>"
        if authorized and latest_url and metadata
        else "<p>Provide the access token to enable the latest-file download link.</p>"
    )
    auth_hint = (
        "<p class='hint'>This inbox is protected. Use the shared AP upload token to upload or download files.</p>"
        if configured_token()
        else "<p class='hint'>No AP upload token is configured yet. Upload and download are currently open.</p>"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Anata AP Upload Inbox</title>
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
    .status {{
      background: rgba(14,110,88,.08);
      border-left: 4px solid var(--accent);
      padding: 12px 14px;
      border-radius: 12px;
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
      <p style="margin:0;color:var(--accent-2);text-transform:uppercase;letter-spacing:.12em;font-size:.78rem;">Anata AP Intake</p>
      <h1>Weekly Bank Export Inbox</h1>
      <p>Upload the latest bank transactions CSV here. The AP audit jobs pull the current file from this inbox instead of depending on a local path.</p>
      {status_block}
      {auth_hint}
      <div class="grid">
        <section class="card">
          <h2 style="margin-top:0;">Upload Current File</h2>
          <form action="/upload" method="post" enctype="multipart/form-data">
            <label for="access_token">Access Token</label>
            <input id="access_token" name="access_token" type="password" autocomplete="current-password" placeholder="Paste AP upload token">
            <label for="transaction_file" style="margin-top:14px;">Transactions CSV</label>
            <input id="transaction_file" name="transaction_file" type="file" accept=".csv,text/csv">
            <button type="submit">Upload Latest CSV</button>
          </form>
        </section>
        <section class="card">
          <h2 style="margin-top:0;">Current File</h2>
          <div class="metric"><strong>Filename</strong>{latest_name}</div>
          <div class="metric"><strong>Uploaded At</strong>{latest_uploaded_at}</div>
          <div class="metric"><strong>Size</strong>{latest_size:,} bytes</div>
          {download_block}
          <p class="hint">Cron jobs should use <code>/latest.csv</code> as the transaction source.</p>
        </section>
      </div>
    </section>
  </main>
</body>
</html>"""


def latest_download_url(environ: Dict[str, Any], token: str) -> str:
    host = environ.get("HTTP_HOST") or "localhost"
    scheme = environ.get("HTTP_X_FORWARDED_PROTO") or environ.get("wsgi.url_scheme") or "http"
    query = f"?{urlencode({'token': token})}" if token else ""
    return f"{scheme}://{host}/latest.csv{query}"


def parse_upload_form(environ: Dict[str, Any]) -> Dict[str, Any]:
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


def upload_error(start_response: Any, status: str, message: str) -> Iterable[bytes]:
    return text_response(start_response, status, message, "text/plain; charset=utf-8")


def app(environ: Dict[str, Any], start_response: Any) -> Iterable[bytes]:
    root = storage_dir()
    ensure_storage(root)
    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/")
    query = parse_query_string(environ)
    metadata = current_metadata(root)

    if method == "GET" and path == "/health":
        return json_response(start_response, "200 OK", {"ok": True, "latest_upload": metadata})

    if method == "GET" and path in {"/", "/index.html"}:
        token = request_token(environ)
        authorized = token_is_valid(token)
        status_message = query.get("status", "")
        if status_message == "uploaded":
            status_message = "Upload accepted and current AP transaction file updated."
        elif status_message == "unauthorized":
            status_message = "Access token rejected."
        elif status_message == "missing-file":
            status_message = "Choose a CSV file before uploading."
        elif status_message == "bad-type":
            status_message = "Only CSV uploads are accepted."
        elif status_message == "too-large":
            status_message = f"Upload exceeds the {max_upload_bytes():,}-byte limit."
        download_url = latest_download_url(environ, token) if authorized and metadata else ""
        body = html_page(status_message, metadata, authorized, download_url)
        return text_response(start_response, "200 OK", body, "text/html; charset=utf-8")

    if method == "GET" and path == "/latest.csv":
        token = request_token(environ)
        if not token_is_valid(token):
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

    if method == "POST" and path == "/upload":
        try:
            form = parse_upload_form(environ)
        except ValueError:
            return upload_error(start_response, "400 Bad Request", "Could not parse upload form.")

        token = request_token(environ, form)
        if not token_is_valid(token):
            return upload_error(start_response, "401 Unauthorized", "Unauthorized")

        if "transaction_file" not in form:
            return upload_error(start_response, "400 Bad Request", "Missing transaction_file.")

        upload_field = form["transaction_file"]
        if not isinstance(upload_field, dict):
            return upload_error(start_response, "400 Bad Request", "Missing uploaded file.")
        filename = str(upload_field.get("filename", "")).strip()
        if not filename:
            return upload_error(start_response, "400 Bad Request", "Missing file name.")
        if Path(filename).suffix.lower() not in ACCEPTED_EXTENSIONS:
            return upload_error(start_response, "400 Bad Request", "Only CSV uploads are accepted.")
        content = upload_field.get("content", b"")
        if not isinstance(content, bytes):
            return upload_error(start_response, "400 Bad Request", "Uploaded file content was invalid.")
        if len(content) > max_upload_bytes():
            return upload_error(start_response, "413 Payload Too Large", "Upload exceeds the configured size limit.")

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
