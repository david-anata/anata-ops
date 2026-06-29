#!/usr/bin/env python3
"""Prepare, validate, and run the fulfillment support agent."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence
from zoneinfo import ZoneInfo


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "fulfillment_support.json"
DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = DEFAULT_WORKSPACE_ROOT / ".env"
DEFAULT_CASE_STATUSES_OPEN = ("new", "investigating", "responded", "escalated", "waiting_human")
DEFAULT_RELATIONSHIP_TYPE = "new"
RELATIONSHIP_TYPES = ("new", "duplicate", "follow_up", "reopened")
CASE_LOOKBACK_DAYS = 14
SIMILARITY_DUPLICATE_THRESHOLD = 0.68
SIMILARITY_REOPEN_THRESHOLD = 0.58
RESOLUTION_MARKERS = ("resolved", "mark resolved", "closed", "complete", "done")
INTERNAL_UPDATE_PATTERNS = (
    "can be marked as fulfilled",
    "in process of resolving",
    "resolved/in process",
    "oos orders",
    "pr orders",
)
CANDIDATE_ACKNOWLEDGMENTS = (
    "thank you",
    "thanks",
    "yes",
    "sounds good",
    "got it",
    "perfect",
)
CANDIDATE_KEYWORDS = re.compile(
    r"\?|order|tracking|shipp|delay|deliver|replacement|return|refund|inventory|stock|eta|where is|what happened|why|damag|wrong item|missing",
    re.IGNORECASE,
)
FOLLOW_UP_PATTERNS = (
    "any update",
    "following up",
    "follow up",
    "checking in",
    "just checking",
    "bumping",
    "bump",
    "status update",
    "can you check",
    "did you see this",
)
SUPPORT_TEXT_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "by", "can", "could", "for", "from",
    "have", "hey", "hi", "i", "im", "is", "it", "just", "me", "my", "need", "of", "on", "or",
    "our", "please", "the", "this", "that", "to", "update", "we", "what", "when", "where",
    "why", "with", "would", "you", "your", "order", "tracking", "shipment", "shipping", "label",
    "package", "client", "customer", "fulfillment", "support", "team",
}
VON_PRIMARY_CATEGORIES = {
    "warehouse_execution",
    "inventory_po",
    "shipment_stuck",
    "shipment_exception",
    "fulfillment_status_mismatch",
}
ASHLEY_PRIMARY_CATEGORIES = {
    "waiting_on_customer",
    "client_coordination",
    "refund_replacement",
    "address_change",
    "match_missing",
    "insufficient_data",
    "general_support",
}
DUAL_ESCALATION_CATEGORIES = {
    "delivery_exception",
    "wrong_item_or_damage",
    "system_conflict",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare and validate the fulfillment support environment.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_PATH))
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--check-live", action="store_true")
    parser.add_argument("--run-agent", action="store_true")
    parser.add_argument("--review-candidates", action="store_true")
    parser.add_argument("--force-run", action="store_true")
    parser.add_argument("--now", default="")
    return parser.parse_args()


def read_config(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise SystemExit(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Config file is not valid JSON: {path}") from exc


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, value.strip())


def resolve_path(raw: str, *, base: Path) -> Path:
    candidate = Path(raw).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def resolve_workspace_root(config: Mapping[str, Any]) -> Path:
    raw_value = env_value("SUPPORT_WORKSPACE_ROOT") or str(config.get("workspace_root", "."))
    return resolve_path(raw_value, base=DEFAULT_WORKSPACE_ROOT)


def resolve_agent_root(config: Mapping[str, Any], workspace_root: Path) -> Path:
    raw_value = env_value("SUPPORT_AGENT_ROOT") or str(config.get("agent_root", "support-agent"))
    return resolve_path(raw_value, base=workspace_root)


def resolve_directories(config: Mapping[str, Any], workspace_root: Path) -> Dict[str, Path]:
    raw_paths = config.get("paths", {})
    if not isinstance(raw_paths, Mapping):
        raw_paths = {}
    directories: Dict[str, Path] = {}
    for name in (
        "intake",
        "runs",
        "knowledge",
        "escalations",
        "connections_db",
        "shopify_accounts",
        "labelogics_accounts",
    ):
        raw_value = str(raw_paths.get(name, f"support-agent/{name}"))
        directories[name] = resolve_path(raw_value, base=workspace_root)
    return directories


def ensure_directories(directories: Mapping[str, Path]) -> None:
    for name, path in directories.items():
        target = path if name in {"intake", "runs", "knowledge", "escalations"} else path.parent
        target.mkdir(parents=True, exist_ok=True)


def env_value(name: str) -> str:
    return os.getenv(name, "").strip()


def split_csv_env(name: str) -> List[str]:
    raw_value = env_value(name)
    if not raw_value:
        return []
    normalized = raw_value.replace("\n", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def enabled_missing_env(section: Mapping[str, Any]) -> Dict[str, list[str]]:
    missing: Dict[str, list[str]] = {}
    for name, payload in section.items():
        if not isinstance(payload, Mapping):
            continue
        if not bool(payload.get("enabled")):
            continue
        required = payload.get("required_env", [])
        missing_vars = [item for item in required if not env_value(str(item))]
        if missing_vars:
            missing[name] = missing_vars
    return missing


def enabled_names(section: Mapping[str, Any]) -> list[str]:
    names = []
    for name, payload in section.items():
        if isinstance(payload, Mapping) and bool(payload.get("enabled")):
            names.append(name)
    return names


def channel_summary() -> Dict[str, Any]:
    channels = split_csv_env("SUPPORT_SLACK_CHANNELS")
    duplicate_names = sorted({item for item in channels if channels.count(item) > 1})
    non_ascii = [item for item in channels if not item.isascii()]
    return {
        "count": len(channels),
        "names": channels,
        "duplicates": duplicate_names,
        "non_ascii": non_ascii,
    }


def account_matching_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    payload = config.get("account_matching", {})
    if not isinstance(payload, Mapping):
        return {"enabled": False, "ignored_tokens": [], "alias_overrides": {}}
    ignored_tokens = payload.get("ignored_tokens", [])
    alias_overrides = payload.get("alias_overrides", {})
    return {
        "enabled": bool(payload.get("enabled")),
        "ignored_tokens": [str(item).strip().lower() for item in ignored_tokens if str(item).strip()],
        "alias_overrides": {str(key): str(value) for key, value in alias_overrides.items()},
    }


def agent_runtime_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    payload = config.get("agent_runtime", {})
    if not isinstance(payload, Mapping):
        payload = {}
    resolution_markers = payload.get("resolution_markers", list(RESOLUTION_MARKERS))
    if not isinstance(resolution_markers, Sequence) or isinstance(resolution_markers, (str, bytes)):
        resolution_markers = list(RESOLUTION_MARKERS)
    return {
        "enabled": bool(payload.get("enabled", True)),
        "mode": str(payload.get("mode", "scheduled")),
        "queue_channel": str(payload.get("queue_channel", "fulfillment-ops")),
        "lookback_hours": int(payload.get("lookback_hours", 6)),
        "escalation_slack_user_ids_env": str(
            payload.get("escalation_slack_user_ids_env", "SUPPORT_ESCALATION_SLACK_USER_IDS")
        ),
        "resolution_markers": [str(item).strip().lower() for item in resolution_markers if str(item).strip()],
    }


def normalize_identifier(value: str, *, alias_overrides: Mapping[str, str]) -> str:
    normalized = alias_overrides.get(value, value).strip().lower()
    normalized = normalized.replace(".myshopify.com", "")
    normalized = normalized.replace("&", " and ")
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized


def identifier_tokens(value: str, *, ignored_tokens: Sequence[str], alias_overrides: Mapping[str, str]) -> List[str]:
    normalized = normalize_identifier(value, alias_overrides=alias_overrides)
    pattern_items = [re.escape(item) for item in sorted(ignored_tokens, key=len, reverse=True) if item]
    expanded = normalized
    if pattern_items:
        expanded = re.sub("|".join(pattern_items), lambda match: f"-{match.group(0)}-", expanded)
    expanded = re.sub(r"-{2,}", "-", expanded).strip("-")
    return [item for item in expanded.split("-") if item and item not in ignored_tokens and not item.isdigit()]


def load_json_array(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, Mapping)]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_runtime_now(now_arg: str, timezone_name: str) -> datetime:
    tz = ZoneInfo(timezone_name)
    if not now_arg:
        return datetime.now(tz)
    candidate = datetime.fromisoformat(now_arg)
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=tz)
    return candidate.astimezone(tz)


def slack_ts_sort_key(message: Mapping[str, Any]) -> float:
    try:
        return float(str(message.get("ts", "0")))
    except ValueError:
        return 0.0


def max_slack_ts(left: str, right: str) -> str:
    return left if float(left or 0) >= float(right or 0) else right


def slack_ts_to_permalink_fragment(ts: str) -> str:
    return "p" + str(ts).replace(".", "")


def format_slack_permalink(team_url: str, channel_id: str, thread_ts: str) -> str:
    if not team_url or not channel_id or not thread_ts:
        return ""
    return f"{team_url.rstrip('/')}/archives/{channel_id}/{slack_ts_to_permalink_fragment(thread_ts)}"


def case_id_for_thread(channel_id: str, thread_ts: str) -> str:
    digest = hashlib.sha1(f"{channel_id}:{thread_ts}".encode("utf-8")).hexdigest()
    return f"case-{digest[:16]}"


def json_request(url: str, *, headers: Mapping[str, str], method: str = "POST", body: bytes = b"") -> Dict[str, Any]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload or "{}")
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", "ignore")
        try:
            details = json.loads(payload or "{}")
        except json.JSONDecodeError:
            details = {"raw": payload}
        return {
            "ok": False,
            "http_status": exc.code,
            "error": details.get("error") or details.get("message") or "http_error",
            "details": details,
        }
    except urllib.error.URLError as exc:
        return {"ok": False, "error": str(exc.reason)}


def can_run_required_env(names: Iterable[str]) -> bool:
    return all(env_value(name) for name in names)


def form_encoded_request(url: str, *, headers: Mapping[str, str], body: Mapping[str, str], method: str = "POST") -> Dict[str, Any]:
    encoded = urllib.parse.urlencode(body).encode("utf-8")
    request_headers = {"Content-Type": "application/x-www-form-urlencoded", **dict(headers)}
    return json_request(url, headers=request_headers, body=encoded, method=method)


def json_body_request(url: str, *, headers: Mapping[str, str], body: Mapping[str, Any], method: str = "POST") -> Dict[str, Any]:
    request_headers = {"Content-Type": "application/json", **dict(headers)}
    return json_request(url, headers=request_headers, body=json.dumps(body).encode("utf-8"), method=method)


def slack_api_request(method_name: str, *, form_body: Mapping[str, str] | None = None, json_body: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    base_url = env_value("SLACK_API_BASE_URL").rstrip("/")
    token = env_value("SLACK_BOT_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url}/{method_name}"
    if json_body is not None:
        return json_body_request(url, headers=headers, body=json_body)
    return form_encoded_request(url, headers=headers, body=form_body or {})


def slack_conversations() -> List[Dict[str, Any]]:
    cursor = ""
    conversations: List[Dict[str, Any]] = []
    while True:
        payload = {"types": "public_channel,private_channel", "limit": "1000"}
        if cursor:
            payload["cursor"] = cursor
        response = slack_api_request("conversations.list", form_body=payload)
        if not response.get("ok"):
            break
        conversations.extend(response.get("channels", []))
        cursor = str(response.get("response_metadata", {}).get("next_cursor", "")).strip()
        if not cursor:
            break
    return conversations


def slack_channel_history(channel_id: str, *, oldest: str) -> List[Dict[str, Any]]:
    cursor = ""
    messages: List[Dict[str, Any]] = []
    while True:
        payload = {"channel": channel_id, "limit": "200", "oldest": oldest, "inclusive": "true"}
        if cursor:
            payload["cursor"] = cursor
        response = slack_api_request("conversations.history", form_body=payload)
        if not response.get("ok"):
            break
        messages.extend(response.get("messages", []))
        cursor = str(response.get("response_metadata", {}).get("next_cursor", "")).strip()
        if not cursor:
            break
    return sorted(messages, key=slack_ts_sort_key)


def slack_thread_messages(channel_id: str, thread_ts: str) -> List[Dict[str, Any]]:
    cursor = ""
    messages: List[Dict[str, Any]] = []
    while True:
        payload = {"channel": channel_id, "ts": thread_ts, "limit": "200"}
        if cursor:
            payload["cursor"] = cursor
        response = slack_api_request("conversations.replies", form_body=payload)
        if not response.get("ok"):
            break
        messages.extend(response.get("messages", []))
        cursor = str(response.get("response_metadata", {}).get("next_cursor", "")).strip()
        if not cursor:
            break
    return sorted(messages, key=slack_ts_sort_key)


def slack_post_message(channel_id: str, text: str, *, thread_ts: str = "") -> Dict[str, Any]:
    payload: Dict[str, Any] = {"channel": channel_id, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return slack_api_request("chat.postMessage", json_body=payload)


def slack_live_check() -> Dict[str, Any]:
    required = ("SLACK_API_BASE_URL", "SLACK_BOT_TOKEN")
    if not can_run_required_env(required):
        return {"ok": False, "skipped": True, "error": "missing_env"}
    response = slack_api_request("auth.test")
    summary = {
        "ok": bool(response.get("ok")),
        "team": response.get("team", ""),
        "team_id": response.get("team_id", ""),
        "team_url": response.get("url", ""),
        "user_id": response.get("user_id", ""),
        "bot_id": response.get("bot_id", ""),
        "error": response.get("error", ""),
    }
    if summary["ok"] and env_value("SUPPORT_SLACK_CHANNELS"):
        conversations = slack_conversations()
        visible_names = {str(item.get("name", "")): str(item.get("id", "")) for item in conversations}
        configured_names = split_csv_env("SUPPORT_SLACK_CHANNELS")
        summary["visible_channel_count"] = len(conversations)
        summary["visible_channels"] = [
            {"name": str(item.get("name", "")), "id": str(item.get("id", ""))}
            for item in sorted(conversations, key=lambda entry: str(entry.get("name", "")))
        ]
        summary["configured_channels_found"] = [{"name": name, "id": visible_names[name]} for name in configured_names if name in visible_names]
        summary["configured_channels_missing"] = [name for name in configured_names if name not in visible_names]
    return summary


def labelogics_basic_auth_header() -> str:
    raw_value = f"{env_value('LABELOGICS_KEY')}:{env_value('LABELOGICS_PASSWORD')}".encode("utf-8")
    return "Basic " + base64.b64encode(raw_value).decode("ascii")


def labelogics_access_token() -> Dict[str, Any]:
    app_url = env_value("LABELOGICS_APP_URL").rstrip("/")
    if not app_url:
        return {"ok": False, "error": "missing_app_url"}
    response = json_request(
        f"{app_url}/api/auth/tokens/generate",
        headers={"Authorization": labelogics_basic_auth_header()},
    )
    token_data = response.get("data", {}).get("tokens", {}).get("access", {})
    token = token_data.get("token", "")
    return {
        "ok": bool(response.get("result") == "success" or token),
        "token": token,
        "expires": token_data.get("expires", ""),
        "error": response.get("error") or response.get("message", ""),
        "raw": response,
    }


def labelogics_live_check() -> Dict[str, Any]:
    required = ("LABELOGICS_APP_URL", "LABELOGICS_KEY", "LABELOGICS_PASSWORD")
    if not can_run_required_env(required):
        return {"ok": False, "skipped": True, "error": "missing_env"}
    token_data = labelogics_access_token()
    summary = {
        "ok": bool(token_data.get("ok")),
        "has_access_token": bool(token_data.get("token")),
        "access_expires": token_data.get("expires", ""),
        "error": token_data.get("error", ""),
    }
    return summary


def shopify_live_check() -> Dict[str, Any]:
    required = ("SHOPIFY_STORE_DOMAIN", "SHOPIFY_ADMIN_API_ACCESS_TOKEN", "SHOPIFY_API_VERSION")
    if not can_run_required_env(required):
        return {"ok": False, "skipped": True, "error": "missing_env"}
    store_domain = env_value("SHOPIFY_STORE_DOMAIN")
    access_token = env_value("SHOPIFY_ADMIN_API_ACCESS_TOKEN")
    scopes_response = json_request(
        f"https://{store_domain}/admin/oauth/access_scopes.json",
        headers={"X-Shopify-Access-Token": access_token},
        method="GET",
    )
    scopes = [item.get("handle", "") for item in scopes_response.get("access_scopes", []) if item.get("handle")]
    return {
        "ok": bool(scopes_response.get("access_scopes")),
        "store_domain": store_domain,
        "api_version": env_value("SHOPIFY_API_VERSION"),
        "scopes": scopes,
        "read_orders": "read_orders" in scopes,
        "read_all_orders": "read_all_orders" in scopes,
        "error": scopes_response.get("error", ""),
    }


def live_checks() -> Dict[str, Any]:
    return {
        "slack": slack_live_check(),
        "shopify": shopify_live_check(),
        "labelogics": labelogics_live_check(),
    }


def build_source_records(
    records: Sequence[Mapping[str, Any]],
    *,
    name_keys: Sequence[str],
    id_key: str,
    ignored_tokens: Sequence[str],
    alias_overrides: Mapping[str, str],
) -> List[Dict[str, Any]]:
    built: List[Dict[str, Any]] = []
    for item in records:
        display_name = ""
        for key in name_keys:
            raw_value = str(item.get(key, "")).strip()
            if raw_value:
                display_name = raw_value
                break
        if not display_name:
            continue
        aliases = [display_name]
        extra_aliases = item.get("aliases", [])
        if isinstance(extra_aliases, Sequence) and not isinstance(extra_aliases, (str, bytes)):
            aliases.extend(str(entry).strip() for entry in extra_aliases if str(entry).strip())
        token_set = sorted(
            {
                token
                for alias in aliases
                for token in identifier_tokens(alias, ignored_tokens=ignored_tokens, alias_overrides=alias_overrides)
            }
        )
        built.append(
            {
                "id": str(item.get(id_key, "")).strip(),
                "display_name": display_name,
                "aliases": aliases,
                "tokens": token_set,
                "normalized": [normalize_identifier(alias, alias_overrides=alias_overrides) for alias in aliases if alias.strip()],
            }
        )
    return built


def build_slack_records(
    channels: Sequence[str],
    *,
    ignored_tokens: Sequence[str],
    alias_overrides: Mapping[str, str],
) -> List[Dict[str, Any]]:
    built = []
    for channel in channels:
        built.append(
            {
                "id": channel,
                "display_name": channel,
                "aliases": [channel],
                "tokens": identifier_tokens(channel, ignored_tokens=ignored_tokens, alias_overrides=alias_overrides),
                "normalized": [normalize_identifier(channel, alias_overrides=alias_overrides)],
            }
        )
    return built


def score_record_match(left: Mapping[str, Any], right: Mapping[str, Any]) -> int:
    left_tokens = set(left.get("tokens", []))
    right_tokens = set(right.get("tokens", []))
    overlap = left_tokens & right_tokens
    if not overlap:
        return 0
    score = len(overlap) * 10
    if any(item in right.get("normalized", []) for item in left.get("normalized", [])):
        score += 50
    if left_tokens == right_tokens:
        score += 25
    return score


def best_match(source: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    ranked = []
    for candidate in candidates:
        score = score_record_match(source, candidate)
        if score > 0:
            ranked.append((score, candidate))
    ranked.sort(key=lambda item: (-item[0], item[1].get("display_name", "")))
    if not ranked:
        return {}
    score, candidate = ranked[0]
    return {"id": candidate.get("id", ""), "display_name": candidate.get("display_name", ""), "score": score}


def account_matching_summary(config: Mapping[str, Any], directories: Mapping[str, Path]) -> Dict[str, Any]:
    matching = account_matching_config(config)
    if not matching["enabled"]:
        return {"enabled": False}
    ignored_tokens = matching["ignored_tokens"]
    alias_overrides = matching["alias_overrides"]
    slack_records = build_slack_records(
        split_csv_env("SUPPORT_SLACK_CHANNELS"),
        ignored_tokens=ignored_tokens,
        alias_overrides=alias_overrides,
    )
    shopify_records = build_source_records(
        load_json_array(directories["shopify_accounts"]),
        name_keys=("shop_name", "store_domain"),
        id_key="store_domain",
        ignored_tokens=ignored_tokens,
        alias_overrides=alias_overrides,
    )
    labelogics_records = build_source_records(
        load_json_array(directories["labelogics_accounts"]),
        name_keys=("account_name",),
        id_key="account_id",
        ignored_tokens=ignored_tokens,
        alias_overrides=alias_overrides,
    )
    candidate_matches = []
    unmatched_channels = []
    for record in slack_records:
        shopify_match = best_match(record, shopify_records)
        labelogics_match = best_match(record, labelogics_records)
        candidate_matches.append(
            {
                "slack_channel": record["display_name"],
                "tokens": record["tokens"],
                "shopify_match": shopify_match,
                "labelogics_match": labelogics_match,
            }
        )
        if not shopify_match or not labelogics_match:
            unmatched_channels.append(record["display_name"])
    return {
        "enabled": True,
        "slack_records": slack_records,
        "shopify_records": shopify_records,
        "labelogics_records": labelogics_records,
        "shopify_accounts_path": str(directories["shopify_accounts"]),
        "labelogics_accounts_path": str(directories["labelogics_accounts"]),
        "shopify_accounts_loaded": len(shopify_records),
        "labelogics_accounts_loaded": len(labelogics_records),
        "candidate_matches": candidate_matches,
        "unmatched_slack_channels": unmatched_channels,
    }


def open_connections_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS slack_channels (
            channel_name TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL DEFAULT '',
            visible INTEGER NOT NULL DEFAULT 0,
            normalized_name TEXT NOT NULL DEFAULT '',
            tokens_json TEXT NOT NULL DEFAULT '[]',
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS shopify_accounts (
            store_domain TEXT PRIMARY KEY,
            shop_name TEXT NOT NULL DEFAULT '',
            aliases_json TEXT NOT NULL DEFAULT '[]',
            normalized_names_json TEXT NOT NULL DEFAULT '[]',
            tokens_json TEXT NOT NULL DEFAULT '[]',
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS labelogics_accounts (
            account_id TEXT PRIMARY KEY,
            account_name TEXT NOT NULL DEFAULT '',
            aliases_json TEXT NOT NULL DEFAULT '[]',
            normalized_names_json TEXT NOT NULL DEFAULT '[]',
            tokens_json TEXT NOT NULL DEFAULT '[]',
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS connection_matches (
            slack_channel TEXT PRIMARY KEY,
            slack_tokens_json TEXT NOT NULL DEFAULT '[]',
            shopify_store_domain TEXT NOT NULL DEFAULT '',
            shopify_display_name TEXT NOT NULL DEFAULT '',
            shopify_score INTEGER NOT NULL DEFAULT 0,
            labelogics_account_id TEXT NOT NULL DEFAULT '',
            labelogics_display_name TEXT NOT NULL DEFAULT '',
            labelogics_score INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'unmatched',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS support_cases (
            case_id TEXT PRIMARY KEY,
            canonical_case_id TEXT NOT NULL DEFAULT '',
            related_case_id TEXT NOT NULL DEFAULT '',
            relationship_type TEXT NOT NULL DEFAULT 'new',
            relationship_confidence INTEGER NOT NULL DEFAULT 0,
            source_channel_name TEXT NOT NULL,
            source_channel_id TEXT NOT NULL,
            source_thread_ts TEXT NOT NULL,
            latest_message_ts TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'new',
            brand_name TEXT NOT NULL DEFAULT '',
            connection_status TEXT NOT NULL DEFAULT '',
            shopify_store_domain TEXT NOT NULL DEFAULT '',
            labelogics_account_id TEXT NOT NULL DEFAULT '',
            operational_object_key TEXT NOT NULL DEFAULT '',
            complaint_fingerprint TEXT NOT NULL DEFAULT '',
            issue_category TEXT NOT NULL DEFAULT '',
            primary_owner TEXT NOT NULL DEFAULT '',
            secondary_owner TEXT NOT NULL DEFAULT '',
            waiting_on TEXT NOT NULL DEFAULT '',
            customer_question_summary TEXT NOT NULL DEFAULT '',
            customer_facing_reply TEXT NOT NULL DEFAULT '',
            latest_evidence_summary TEXT NOT NULL DEFAULT '',
            latest_resolution_summary TEXT NOT NULL DEFAULT '',
            escalation_channel_name TEXT NOT NULL DEFAULT '',
            escalation_channel_id TEXT NOT NULL DEFAULT '',
            escalation_thread_ts TEXT NOT NULL DEFAULT '',
            escalation_reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT DEFAULT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_support_cases_thread
        ON support_cases (source_channel_id, source_thread_ts);

        CREATE TABLE IF NOT EXISTS support_case_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL UNIQUE,
            case_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            slack_channel_id TEXT NOT NULL DEFAULT '',
            slack_message_ts TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(case_id) REFERENCES support_cases(case_id)
        );

        CREATE TABLE IF NOT EXISTS support_case_threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_channel_id TEXT NOT NULL,
            source_thread_ts TEXT NOT NULL,
            case_id TEXT NOT NULL,
            relationship_type TEXT NOT NULL DEFAULT 'new',
            related_case_id TEXT NOT NULL DEFAULT '',
            relationship_confidence INTEGER NOT NULL DEFAULT 0,
            last_message_ts TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (source_channel_id, source_thread_ts),
            FOREIGN KEY(case_id) REFERENCES support_cases(case_id)
        );

        CREATE TABLE IF NOT EXISTS support_case_assignments (
            case_id TEXT NOT NULL,
            slack_user_id TEXT NOT NULL,
            user_label TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'reviewer',
            status TEXT NOT NULL DEFAULT 'open',
            assigned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (case_id, slack_user_id),
            FOREIGN KEY(case_id) REFERENCES support_cases(case_id)
        );
        """
    )
    ensure_schema_columns(
        connection,
        "support_cases",
        {
            "canonical_case_id": "TEXT NOT NULL DEFAULT ''",
            "related_case_id": "TEXT NOT NULL DEFAULT ''",
            "relationship_type": "TEXT NOT NULL DEFAULT 'new'",
            "relationship_confidence": "INTEGER NOT NULL DEFAULT 0",
            "operational_object_key": "TEXT NOT NULL DEFAULT ''",
            "complaint_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "issue_category": "TEXT NOT NULL DEFAULT ''",
            "primary_owner": "TEXT NOT NULL DEFAULT ''",
            "secondary_owner": "TEXT NOT NULL DEFAULT ''",
            "waiting_on": "TEXT NOT NULL DEFAULT ''",
            "latest_evidence_summary": "TEXT NOT NULL DEFAULT ''",
        },
    )
    ensure_schema_columns(
        connection,
        "support_case_events",
        {},
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_support_cases_operational_object
        ON support_cases (operational_object_key, status)
        """
    )
    return connection


def ensure_schema_columns(connection: sqlite3.Connection, table_name: str, columns: Mapping[str, str]) -> None:
    existing = {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, ddl in columns.items():
        if column_name in existing:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def upsert_slack_channels(connection: sqlite3.Connection, slack_check: Mapping[str, Any], configured_channels: Sequence[Mapping[str, Any]]) -> None:
    visible_channels = {
        str(item.get("name", "")): str(item.get("id", ""))
        for item in slack_check.get("visible_channels", [])
        if str(item.get("name", "")).strip()
    }
    for item in configured_channels:
        connection.execute(
            """
            INSERT INTO slack_channels (
                channel_name,
                channel_id,
                visible,
                normalized_name,
                tokens_json,
                last_seen_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_name) DO UPDATE SET
                channel_id=excluded.channel_id,
                visible=excluded.visible,
                normalized_name=excluded.normalized_name,
                tokens_json=excluded.tokens_json,
                last_seen_at=CURRENT_TIMESTAMP
            """,
            (
                item["display_name"],
                visible_channels.get(item["display_name"], ""),
                1 if item["display_name"] in visible_channels else 0,
                item["normalized"][0] if item.get("normalized") else "",
                json.dumps(item.get("tokens", [])),
            ),
        )


def upsert_shopify_accounts(connection: sqlite3.Connection, records: Sequence[Mapping[str, Any]]) -> None:
    for item in records:
        connection.execute(
            """
            INSERT INTO shopify_accounts (
                store_domain,
                shop_name,
                aliases_json,
                normalized_names_json,
                tokens_json,
                last_seen_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(store_domain) DO UPDATE SET
                shop_name=excluded.shop_name,
                aliases_json=excluded.aliases_json,
                normalized_names_json=excluded.normalized_names_json,
                tokens_json=excluded.tokens_json,
                last_seen_at=CURRENT_TIMESTAMP
            """,
            (
                item["id"],
                item["display_name"],
                json.dumps(item.get("aliases", [])),
                json.dumps(item.get("normalized", [])),
                json.dumps(item.get("tokens", [])),
            ),
        )


def upsert_labelogics_accounts(connection: sqlite3.Connection, records: Sequence[Mapping[str, Any]]) -> None:
    for item in records:
        connection.execute(
            """
            INSERT INTO labelogics_accounts (
                account_id,
                account_name,
                aliases_json,
                normalized_names_json,
                tokens_json,
                last_seen_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(account_id) DO UPDATE SET
                account_name=excluded.account_name,
                aliases_json=excluded.aliases_json,
                normalized_names_json=excluded.normalized_names_json,
                tokens_json=excluded.tokens_json,
                last_seen_at=CURRENT_TIMESTAMP
            """,
            (
                item["id"],
                item["display_name"],
                json.dumps(item.get("aliases", [])),
                json.dumps(item.get("normalized", [])),
                json.dumps(item.get("tokens", [])),
            ),
        )


def upsert_connection_matches(connection: sqlite3.Connection, matches: Sequence[Mapping[str, Any]]) -> None:
    for item in matches:
        shopify_match = item.get("shopify_match", {})
        labelogics_match = item.get("labelogics_match", {})
        status = "matched" if shopify_match and labelogics_match else "partial" if shopify_match or labelogics_match else "unmatched"
        connection.execute(
            """
            INSERT INTO connection_matches (
                slack_channel,
                slack_tokens_json,
                shopify_store_domain,
                shopify_display_name,
                shopify_score,
                labelogics_account_id,
                labelogics_display_name,
                labelogics_score,
                status,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(slack_channel) DO UPDATE SET
                slack_tokens_json=excluded.slack_tokens_json,
                shopify_store_domain=excluded.shopify_store_domain,
                shopify_display_name=excluded.shopify_display_name,
                shopify_score=excluded.shopify_score,
                labelogics_account_id=excluded.labelogics_account_id,
                labelogics_display_name=excluded.labelogics_display_name,
                labelogics_score=excluded.labelogics_score,
                status=excluded.status,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                item["slack_channel"],
                json.dumps(item.get("tokens", [])),
                shopify_match.get("id", ""),
                shopify_match.get("display_name", ""),
                int(shopify_match.get("score", 0) or 0),
                labelogics_match.get("id", ""),
                labelogics_match.get("display_name", ""),
                int(labelogics_match.get("score", 0) or 0),
                status,
            ),
        )


def sync_connections_database(
    db_path: Path,
    *,
    matching_summary: Mapping[str, Any],
    slack_check: Mapping[str, Any],
    slack_records: Sequence[Mapping[str, Any]],
    shopify_records: Sequence[Mapping[str, Any]],
    labelogics_records: Sequence[Mapping[str, Any]],
    status: str,
) -> Dict[str, Any]:
    connection = open_connections_db(db_path)
    try:
        connection.execute("INSERT INTO sync_runs (status) VALUES (?)", (status,))
        upsert_slack_channels(connection, slack_check, slack_records)
        upsert_shopify_accounts(connection, shopify_records)
        upsert_labelogics_accounts(connection, labelogics_records)
        upsert_connection_matches(connection, matching_summary.get("candidate_matches", []))
        counts = current_db_counts(connection)
        connection.commit()
        return {"path": str(db_path), "counts": counts}
    finally:
        connection.close()


def current_db_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    return {
        "sync_runs": connection.execute("SELECT COUNT(*) FROM sync_runs").fetchone()[0],
        "slack_channels": connection.execute("SELECT COUNT(*) FROM slack_channels").fetchone()[0],
        "shopify_accounts": connection.execute("SELECT COUNT(*) FROM shopify_accounts").fetchone()[0],
        "labelogics_accounts": connection.execute("SELECT COUNT(*) FROM labelogics_accounts").fetchone()[0],
        "connection_matches": connection.execute("SELECT COUNT(*) FROM connection_matches").fetchone()[0],
        "support_cases": connection.execute("SELECT COUNT(*) FROM support_cases").fetchone()[0],
        "support_case_threads": connection.execute("SELECT COUNT(*) FROM support_case_threads").fetchone()[0],
        "support_case_events": connection.execute("SELECT COUNT(*) FROM support_case_events").fetchone()[0],
        "support_case_assignments": connection.execute("SELECT COUNT(*) FROM support_case_assignments").fetchone()[0],
    }


def schedule_summary(config: Mapping[str, Any]) -> Dict[str, Any]:
    schedule = config.get("schedule", {})
    if not isinstance(schedule, Mapping):
        schedule = {}
    weekday_days = schedule.get("weekday_days", ["MO", "TU", "WE", "TH", "FR"])
    weekday_hours = schedule.get("weekday_hours", [8, 10, 12, 14, 16, 18])
    return {
        "weekday_interval_hours": int(schedule.get("weekday_interval_hours", 2)),
        "weekday_days": [str(item) for item in weekday_days],
        "weekday_hours": [int(item) for item in weekday_hours],
    }


def schedule_window_status(config: Mapping[str, Any], *, now: datetime) -> Dict[str, Any]:
    schedule = schedule_summary(config)
    weekday_code = now.strftime("%a").upper()[:2]
    allowed = weekday_code in schedule["weekday_days"] and now.hour in schedule["weekday_hours"]
    return {
        "allowed": allowed,
        "timezone": str(now.tzinfo),
        "current_time": now.isoformat(),
        "weekday_code": weekday_code,
        "allowed_hours": schedule["weekday_hours"],
        "allowed_days": schedule["weekday_days"],
    }


def escalation_owner(config: Mapping[str, Any]) -> str:
    response_policy = config.get("response_policy", {})
    if not isinstance(response_policy, Mapping):
        response_policy = {}
    owner_env = str(response_policy.get("default_escalation_owner_env", "SUPPORT_ESCALATION_DEFAULT_OWNER"))
    return env_value(owner_env)


def build_summary(
    config: Mapping[str, Any],
    config_path: Path,
    directories: Mapping[str, Path],
    *,
    include_live_checks: bool,
) -> Dict[str, Any]:
    channels = config.get("channels", {})
    systems = config.get("systems", {})
    if not isinstance(channels, Mapping):
        channels = {}
    if not isinstance(systems, Mapping):
        systems = {}
    missing_channels = enabled_missing_env(channels)
    missing_systems = enabled_missing_env(systems)
    missing_env = {**missing_channels, **missing_systems}
    timezone_value = env_value("SUPPORT_TIMEZONE") or str(config.get("timezone", "America/Denver"))
    matching_summary = account_matching_summary(config, directories)
    summary = {
        "status": "ready" if not missing_env else "blocked",
        "agent_name": str(config.get("agent_name", "Fulfillment Customer Service Agent")),
        "config_path": str(config_path),
        "workspace_root": str(resolve_workspace_root(config)),
        "agent_root": str(resolve_agent_root(config, resolve_workspace_root(config))),
        "timezone": timezone_value,
        "schedule": schedule_summary(config),
        "agent_runtime": agent_runtime_config(config),
        "channels": enabled_names(channels),
        "channel_config": channel_summary(),
        "systems": enabled_names(systems),
        "directories": {name: str(path) for name, path in directories.items()},
        "missing_env": missing_env,
        "account_matching": matching_summary,
        "default_escalation_owner": escalation_owner(config),
    }
    if include_live_checks:
        summary["live_checks"] = live_checks()
    connections_db_path = directories.get("connections_db")
    if connections_db_path is None:
        knowledge_root = directories.get("knowledge", DEFAULT_WORKSPACE_ROOT / "support-agent" / "knowledge")
        connections_db_path = Path(knowledge_root) / "connections.sqlite3"
    summary["connections_db"] = sync_connections_database(
        connections_db_path,
        matching_summary=matching_summary,
        slack_check=summary.get("live_checks", {}).get("slack", {}),
        slack_records=matching_summary.get("slack_records", []),
        shopify_records=matching_summary.get("shopify_records", []),
        labelogics_records=matching_summary.get("labelogics_records", []),
        status=summary["status"],
    )
    summary["account_matching"] = {
        key: value
        for key, value in matching_summary.items()
        if key not in {"slack_records", "shopify_records", "labelogics_records"}
    }
    return summary


def row_to_dict(row: sqlite3.Row | None) -> Dict[str, Any]:
    return dict(row) if row is not None else {}


def get_connection_match(connection: sqlite3.Connection, channel_name: str) -> Dict[str, Any]:
    row = connection.execute("SELECT * FROM connection_matches WHERE slack_channel = ?", (channel_name,)).fetchone()
    return row_to_dict(row)


def get_case(connection: sqlite3.Connection, case_id: str) -> Dict[str, Any]:
    row = connection.execute("SELECT * FROM support_cases WHERE case_id = ?", (case_id,)).fetchone()
    return row_to_dict(row)


def get_case_thread(connection: sqlite3.Connection, channel_id: str, thread_ts: str) -> Dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM support_case_threads WHERE source_channel_id = ? AND source_thread_ts = ?",
        (channel_id, thread_ts),
    ).fetchone()
    return row_to_dict(row)


def upsert_case_thread(
    connection: sqlite3.Connection,
    *,
    channel_id: str,
    thread_ts: str,
    case_id: str,
    relationship_type: str,
    related_case_id: str,
    relationship_confidence: int,
    last_message_ts: str,
) -> None:
    connection.execute(
        """
        INSERT INTO support_case_threads (
            source_channel_id,
            source_thread_ts,
            case_id,
            relationship_type,
            related_case_id,
            relationship_confidence,
            last_message_ts,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source_channel_id, source_thread_ts) DO UPDATE SET
            case_id=excluded.case_id,
            relationship_type=excluded.relationship_type,
            related_case_id=excluded.related_case_id,
            relationship_confidence=excluded.relationship_confidence,
            last_message_ts=excluded.last_message_ts,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            channel_id,
            thread_ts,
            case_id,
            relationship_type,
            related_case_id,
            relationship_confidence,
            last_message_ts,
        ),
    )


def list_open_cases(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    cursor = connection.execute(
        """
        SELECT * FROM support_cases
        WHERE status IN ({})
        ORDER BY updated_at ASC
        """.format(",".join("?" for _ in DEFAULT_CASE_STATUSES_OPEN)),
        DEFAULT_CASE_STATUSES_OPEN,
    )
    return [dict(row) for row in cursor.fetchall()]


def list_recent_cases(
    connection: sqlite3.Connection,
    *,
    channel_name: str,
    brand_name: str,
) -> List[Dict[str, Any]]:
    cutoff = (utc_now() - timedelta(days=CASE_LOOKBACK_DAYS)).isoformat()
    cursor = connection.execute(
        """
        SELECT * FROM support_cases
        WHERE updated_at >= ?
          AND (source_channel_name = ? OR brand_name = ?)
        ORDER BY updated_at DESC
        LIMIT 50
        """,
        (cutoff, channel_name, brand_name),
    )
    return [dict(row) for row in cursor.fetchall()]


def upsert_support_case(connection: sqlite3.Connection, payload: Mapping[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO support_cases (
            case_id,
            canonical_case_id,
            related_case_id,
            relationship_type,
            relationship_confidence,
            source_channel_name,
            source_channel_id,
            source_thread_ts,
            latest_message_ts,
            status,
            brand_name,
            connection_status,
            shopify_store_domain,
            labelogics_account_id,
            operational_object_key,
            complaint_fingerprint,
            issue_category,
            primary_owner,
            secondary_owner,
            waiting_on,
            customer_question_summary,
            customer_facing_reply,
            latest_evidence_summary,
            latest_resolution_summary,
            escalation_channel_name,
            escalation_channel_id,
            escalation_thread_ts,
            escalation_reason,
            resolved_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(case_id) DO UPDATE SET
            canonical_case_id=excluded.canonical_case_id,
            related_case_id=excluded.related_case_id,
            relationship_type=excluded.relationship_type,
            relationship_confidence=excluded.relationship_confidence,
            source_channel_name=excluded.source_channel_name,
            source_channel_id=excluded.source_channel_id,
            source_thread_ts=excluded.source_thread_ts,
            latest_message_ts=excluded.latest_message_ts,
            status=excluded.status,
            brand_name=excluded.brand_name,
            connection_status=excluded.connection_status,
            shopify_store_domain=excluded.shopify_store_domain,
            labelogics_account_id=excluded.labelogics_account_id,
            operational_object_key=excluded.operational_object_key,
            complaint_fingerprint=excluded.complaint_fingerprint,
            issue_category=excluded.issue_category,
            primary_owner=excluded.primary_owner,
            secondary_owner=excluded.secondary_owner,
            waiting_on=excluded.waiting_on,
            customer_question_summary=excluded.customer_question_summary,
            customer_facing_reply=excluded.customer_facing_reply,
            latest_evidence_summary=excluded.latest_evidence_summary,
            latest_resolution_summary=excluded.latest_resolution_summary,
            escalation_channel_name=excluded.escalation_channel_name,
            escalation_channel_id=excluded.escalation_channel_id,
            escalation_thread_ts=excluded.escalation_thread_ts,
            escalation_reason=excluded.escalation_reason,
            resolved_at=excluded.resolved_at,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            payload.get("case_id", ""),
            payload.get("canonical_case_id", payload.get("case_id", "")),
            payload.get("related_case_id", ""),
            payload.get("relationship_type", DEFAULT_RELATIONSHIP_TYPE),
            int(payload.get("relationship_confidence", 0) or 0),
            payload.get("source_channel_name", ""),
            payload.get("source_channel_id", ""),
            payload.get("source_thread_ts", ""),
            payload.get("latest_message_ts", ""),
            payload.get("status", "new"),
            payload.get("brand_name", ""),
            payload.get("connection_status", ""),
            payload.get("shopify_store_domain", ""),
            payload.get("labelogics_account_id", ""),
            payload.get("operational_object_key", ""),
            payload.get("complaint_fingerprint", ""),
            payload.get("issue_category", ""),
            payload.get("primary_owner", ""),
            payload.get("secondary_owner", ""),
            payload.get("waiting_on", ""),
            payload.get("customer_question_summary", ""),
            payload.get("customer_facing_reply", ""),
            payload.get("latest_evidence_summary", ""),
            payload.get("latest_resolution_summary", ""),
            payload.get("escalation_channel_name", ""),
            payload.get("escalation_channel_id", ""),
            payload.get("escalation_thread_ts", ""),
            payload.get("escalation_reason", ""),
            payload.get("resolved_at"),
        ),
    )


def record_case_event(
    connection: sqlite3.Connection,
    *,
    event_key: str,
    case_id: str,
    event_type: str,
    slack_channel_id: str = "",
    slack_message_ts: str = "",
    payload: Mapping[str, Any] | None = None,
) -> bool:
    try:
        connection.execute(
            """
            INSERT INTO support_case_events (
                event_key,
                case_id,
                event_type,
                slack_channel_id,
                slack_message_ts,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_key,
                case_id,
                event_type,
                slack_channel_id,
                slack_message_ts,
                json.dumps(payload or {}, sort_keys=True),
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def upsert_case_assignments(connection: sqlite3.Connection, case_id: str, slack_user_ids: Sequence[str]) -> int:
    assigned = 0
    for slack_user_id in slack_user_ids:
        connection.execute(
            """
            INSERT INTO support_case_assignments (
                case_id,
                slack_user_id,
                user_label,
                role,
                status,
                updated_at
            ) VALUES (?, ?, ?, 'reviewer', 'open', CURRENT_TIMESTAMP)
            ON CONFLICT(case_id, slack_user_id) DO UPDATE SET
                status='open',
                updated_at=CURRENT_TIMESTAMP
            """,
            (case_id, slack_user_id, f"<@{slack_user_id}>"),
        )
        assigned += 1
    return assigned


def message_text(message: Mapping[str, Any]) -> str:
    return str(message.get("text", "")).strip()


def normalize_support_text(text: str) -> str:
    normalized = text.lower()
    normalized = re.sub(r"<@[A-Z0-9]+>", " ", normalized)
    normalized = re.sub(r"https?://\S+", " ", normalized)
    normalized = re.sub(r"[^a-z0-9#\s-]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def support_text_tokens(text: str) -> List[str]:
    normalized = normalize_support_text(text)
    tokens: List[str] = []
    for token in re.findall(r"[a-z0-9-]+", normalized):
        if token.isdigit():
            continue
        if len(token) <= 2:
            continue
        if token in SUPPORT_TEXT_STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def complaint_fingerprint(text: str) -> str:
    tokens = support_text_tokens(text)
    if not tokens:
        return ""
    return " ".join(tokens[:12])


def token_similarity(left: str, right: str) -> float:
    left_tokens = set(support_text_tokens(left))
    right_tokens = set(support_text_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = left_tokens & right_tokens
    union = left_tokens | right_tokens
    return len(overlap) / len(union)


def is_resolution_marker(text: str, markers: Sequence[str]) -> bool:
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    return any(marker in normalized for marker in markers)


def is_internal_update_message(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    if any(pattern in normalized for pattern in INTERNAL_UPDATE_PATTERNS):
        return True
    if normalized.startswith("@ashley") or normalized.startswith("@von"):
        if "#" in normalized and ("resolved" in normalized or "fulfilled" in normalized):
            return True
    return False


def looks_like_support_request(text: str) -> bool:
    normalized = normalize_support_text(text)
    if not normalized:
        return False
    if any(pattern in normalized for pattern in FOLLOW_UP_PATTERNS):
        return True
    if CANDIDATE_KEYWORDS.search(normalized):
        return True
    identifiers = extract_order_identifiers(text)
    if any(identifiers.get(key) for key in ("order_numbers", "tracking_numbers", "po_numbers", "shipment_ids", "reference_ids")):
        return True
    starts_with_question = normalized.startswith(("where", "what", "why", "when", "how", "can", "did", "does", "is", "are"))
    return starts_with_question and ("?" in text or "order" in normalized or "ship" in normalized or "track" in normalized)


def is_low_signal_acknowledgment(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9\s]+", "", text.strip().lower())
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized in CANDIDATE_ACKNOWLEDGMENTS


def derive_operational_object_key(
    *,
    brand_name: str,
    connection_match: Mapping[str, Any],
    identifiers: Mapping[str, Sequence[str]],
) -> str:
    namespace = (
        str(connection_match.get("labelogics_account_id", "")).strip()
        or str(connection_match.get("shopify_store_domain", "")).strip()
        or normalize_identifier(brand_name, alias_overrides={})
    )
    for key in ("tracking_numbers", "order_numbers", "po_numbers", "shipment_ids", "reference_ids"):
        values = identifiers.get(key, [])
        if values:
            return f"{namespace}:{key}:{str(values[0]).upper()}"
    return ""


def detect_issue_category(
    text: str,
    *,
    identifiers: Mapping[str, Sequence[str]],
    signals: Mapping[str, bool],
) -> str:
    normalized = normalize_support_text(text)
    if any(phrase in normalized for phrase in ("delivered but", "marked delivered", "says delivered", "not received", "didnt receive", "didn't receive")):
        return "delivery_exception"
    if any(phrase in normalized for phrase in ("wrong item", "damaged", "broken", "missing item", "replacement", "return", "refund")):
        return "wrong_item_or_damage"
    if any(phrase in normalized for phrase in ("wrong address", "change address", "address change")):
        return "address_change"
    if signals.get("mentions_po") or signals.get("mentions_inbound") or signals.get("mentions_inventory"):
        return "inventory_po"
    if any(phrase in normalized for phrase in ("pick", "pack", "warehouse", "manifest", "packed", "stuck", "label created")):
        return "warehouse_execution"
    if any(phrase in normalized for phrase in ("tracking", "where is", "shipment", "shipped", "in transit", "delivery", "delayed", "eta")):
        return "shipment_stuck"
    if any(phrase in normalized for phrase in FOLLOW_UP_PATTERNS):
        return "client_coordination"
    if not any(identifiers.get(key) for key in ("order_numbers", "tracking_numbers", "po_numbers", "shipment_ids", "reference_ids")):
        return "waiting_on_customer"
    return "general_support"


def resolve_escalation_owners(
    *,
    issue_category: str,
    escalation_reason: str,
    operator_user_ids: Sequence[str],
) -> Dict[str, str]:
    von_id = env_value("SUPPORT_ESCALATION_VON_ID") or (operator_user_ids[0] if operator_user_ids else "")
    ashley_id = env_value("SUPPORT_ESCALATION_ASHLEY_ID") or (operator_user_ids[1] if len(operator_user_ids) > 1 else "")
    if issue_category in DUAL_ESCALATION_CATEGORIES:
        if issue_category == "system_conflict":
            return {"primary_owner": ashley_id, "secondary_owner": von_id}
        return {"primary_owner": von_id, "secondary_owner": ashley_id}
    if issue_category in VON_PRIMARY_CATEGORIES:
        return {"primary_owner": von_id, "secondary_owner": ashley_id if issue_category == "inventory_po" else ""}
    if issue_category in ASHLEY_PRIMARY_CATEGORIES or escalation_reason in {"match_missing", "missing_identifiers", "shopify_not_configured"}:
        return {"primary_owner": ashley_id, "secondary_owner": ""}
    return {"primary_owner": ashley_id or von_id, "secondary_owner": ""}


def message_from_external_team(message: Mapping[str, Any], team_id: str) -> bool:
    user_team = str(message.get("user_team", "")).strip()
    source_team = str(message.get("source_team", "")).strip()
    return bool((user_team and user_team != team_id) or (source_team and source_team != team_id))


def should_ignore_slack_message(message: Mapping[str, Any], *, bot_user_id: str) -> bool:
    if not str(message.get("ts", "")).strip():
        return True
    subtype = str(message.get("subtype", "")).strip()
    if subtype:
        return True
    if bot_user_id and str(message.get("user", "")).strip() == bot_user_id:
        return True
    if str(message.get("bot_id", "")).strip():
        return True
    text = message_text(message)
    if not text:
        return True
    if is_internal_update_message(text):
        return True
    return False


def should_track_support_message(
    connection: sqlite3.Connection,
    *,
    channel_id: str,
    message: Mapping[str, Any],
    bot_user_id: str,
) -> bool:
    if should_ignore_slack_message(message, bot_user_id=bot_user_id):
        return False
    text = message_text(message)
    if not text or is_low_signal_acknowledgment(text):
        return False
    thread_ts = str(message.get("thread_ts") or message.get("ts") or "").strip()
    if thread_ts and get_case_thread(connection, channel_id, thread_ts):
        return True
    return looks_like_support_request(text)


def summarize_questions(messages: Sequence[Mapping[str, Any]]) -> str:
    prompts: List[str] = []
    for message in messages:
        text = message_text(message)
        if not text:
            continue
        question_bits = [segment.strip() for segment in re.split(r"(?<=\?)\s+", text) if segment.strip()]
        picked = [segment for segment in question_bits if segment.endswith("?")]
        if picked:
            prompts.extend(picked)
        else:
            prompts.append(text)
    if not prompts:
        return ""
    unique_prompts: List[str] = []
    seen: set[str] = set()
    for item in prompts:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_prompts.append(item)
    return " | ".join(unique_prompts[:2])[:500]


def extract_order_identifiers(text: str) -> Dict[str, List[str]]:
    cleaned_text = re.sub(r"<@[A-Z0-9]+>", " ", text)
    po_numbers = sorted(
        {
            match.group(1).upper()
            for match in re.finditer(r"\b(?:purchase\s*order|po)\b\s*#?:?\s*([A-Z0-9-]{3,})", cleaned_text, flags=re.IGNORECASE)
        }
    )
    order_numbers = sorted(
        {
            (match.group(1) or match.group(2)).upper()
            for match in re.finditer(r"(?:\border\b\s*#?:?\s*([A-Z0-9-]{3,})|#([0-9]{4,}))", cleaned_text, flags=re.IGNORECASE)
            if any(char.isdigit() for char in (match.group(1) or match.group(2) or ""))
        }
    )
    tracking_numbers = sorted(
        {
            match.group(1).upper()
            for match in re.finditer(r"\b([A-Z0-9]{8,})\b", cleaned_text)
            if any(char.isdigit() for char in match.group(1))
            and not re.fullmatch(r"[UCW][A-Z0-9]{8,}", match.group(1))
        }
    )
    shipment_ids = sorted(
        {
            match.group(1).upper()
            for match in re.finditer(r"\b(?:shipment|ship)\b\s*#?:?\s*([A-Z0-9-]{3,})", cleaned_text, flags=re.IGNORECASE)
        }
    )
    reference_ids = sorted(
        {
            match.group(1).upper()
            for match in re.finditer(r"\b(?:reference|ref)\b\s*#?:?\s*([A-Z0-9-]{3,})", cleaned_text, flags=re.IGNORECASE)
        }
    )
    return {
        "po_numbers": po_numbers[:5],
        "order_numbers": order_numbers[:5],
        "tracking_numbers": tracking_numbers[:5],
        "shipment_ids": shipment_ids[:5],
        "reference_ids": reference_ids[:5],
    }


def extract_support_signals(text: str) -> Dict[str, bool]:
    normalized = text.lower()
    return {
        "mentions_po": bool(re.search(r"\b(po|purchase order)\b", normalized)),
        "mentions_tracking": "tracking" in normalized,
        "mentions_inbound": any(token in normalized for token in ("received", "arrive", "arrival", "pallet", "inbound")),
        "mentions_inventory": any(token in normalized for token in ("inventory", "stock", "units available", "sku")),
        "mentions_delivery_exception": any(token in normalized for token in ("delivered but", "not received", "missing package", "where is my package")),
        "mentions_refund_or_replacement": any(token in normalized for token in ("refund", "replacement", "wrong item", "damaged", "return")),
    }


def compact_json_summary(payload: Any, *, limit: int = 280) -> str:
    try:
        rendered = json.dumps(payload, sort_keys=True)
    except TypeError:
        rendered = str(payload)
    return rendered[:limit]


def parse_status_hint(payload: Any) -> str:
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            lowered = str(key).lower()
            if "status" in lowered and isinstance(value, (str, int, float)):
                return str(value)
            nested = parse_status_hint(value)
            if nested:
                return nested
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        for item in payload:
            nested = parse_status_hint(item)
            if nested:
                return nested
    return ""


def labelogics_lookup(identifiers: Mapping[str, Sequence[str]], connection_match: Mapping[str, Any]) -> Dict[str, Any]:
    account_id = str(connection_match.get("labelogics_account_id", "")).strip()
    if not account_id:
        return {"source": "labelogics", "confidence": "none", "reason": "missing_account_match"}
    if account_id.startswith("pending:"):
        return {"source": "labelogics", "confidence": "none", "reason": "placeholder_account_id"}
    tracking_numbers = list(identifiers.get("tracking_numbers", []))
    order_numbers = list(identifiers.get("order_numbers", []))
    if not tracking_numbers and not order_numbers:
        return {"source": "labelogics", "confidence": "none", "reason": "missing_identifiers"}
    token_data = labelogics_access_token()
    if not token_data.get("ok") or not token_data.get("token"):
        return {"source": "labelogics", "confidence": "none", "reason": "auth_failed", "error": token_data.get("error", "")}
    app_url = env_value("LABELOGICS_APP_URL").rstrip("/")
    headers = {
        "Authorization": f"Bearer {token_data.get('token', '')}",
        "AccountID": account_id,
    }
    response: Dict[str, Any] = {}
    reference = ""
    api_path = ""
    if tracking_numbers:
        reference = tracking_numbers[0]
        api_path = "/api/label/track"
        response = json_body_request(
            f"{app_url}{api_path}",
            headers=headers,
            body={"tracking_number": reference},
        )
    if (not response or not (response.get("result") == "success" or response.get("data"))) and order_numbers:
        reference = order_numbers[0]
        api_path = "/api/order/tracking"
        response = json_body_request(
            f"{app_url}{api_path}",
            headers=headers,
            body={"order_number": reference},
        )
    status_hint = parse_status_hint(response.get("data") or response)
    ok = bool(response.get("result") == "success" or response.get("data"))
    resolution_type = ""
    lowered_hint = status_hint.lower()
    if any(token in lowered_hint for token in ("deliver", "received")):
        resolution_type = "delivered"
    elif any(token in lowered_hint for token in ("transit", "route", "carrier")):
        resolution_type = "in_transit"
    elif any(token in lowered_hint for token in ("packed", "manifest", "label", "ready")):
        resolution_type = "fulfillment_in_progress"
    return {
        "source": "labelogics",
        "confidence": "high" if ok else "none",
        "reason": "" if ok else response.get("error") or response.get("message") or "not_found",
        "reference": reference,
        "summary": f"Shipping OS shows {reference} with status '{status_hint}'." if ok and status_hint else "",
        "status_hint": status_hint,
        "resolution_type": resolution_type,
        "api_path": api_path,
        "raw": response,
    }


def shopify_graphql_request(store_domain: str, query: str, variables: Mapping[str, Any]) -> Dict[str, Any]:
    access_token = env_value("SHOPIFY_ADMIN_API_ACCESS_TOKEN")
    api_version = env_value("SHOPIFY_API_VERSION") or "2026-01"
    return json_body_request(
        f"https://{store_domain}/admin/api/{api_version}/graphql.json",
        headers={"X-Shopify-Access-Token": access_token},
        body={"query": query, "variables": variables},
    )


def shopify_lookup(identifiers: Mapping[str, Sequence[str]], connection_match: Mapping[str, Any]) -> Dict[str, Any]:
    store_domain = str(connection_match.get("shopify_store_domain", "")).strip()
    if not store_domain:
        return {"source": "shopify", "confidence": "none", "reason": "missing_store_match"}
    if not can_run_required_env(("SHOPIFY_ADMIN_API_ACCESS_TOKEN", "SHOPIFY_API_VERSION")):
        return {"source": "shopify", "confidence": "none", "reason": "shopify_not_configured"}
    order_numbers = list(identifiers.get("order_numbers", []))
    if not order_numbers:
        return {"source": "shopify", "confidence": "none", "reason": "missing_identifiers"}
    query = """
    query SupportAgentOrders($search: String!) {
      orders(first: 3, query: $search, reverse: true) {
        edges {
          node {
            id
            name
            displayFulfillmentStatus
            displayFinancialStatus
          }
        }
      }
    }
    """
    reference = order_numbers[0]
    response = shopify_graphql_request(store_domain, query, {"search": f"name:{reference}"})
    edges = response.get("data", {}).get("orders", {}).get("edges", [])
    if not edges:
        return {
            "source": "shopify",
            "confidence": "none",
            "reason": response.get("errors", [{}])[0].get("message", "not_found") if response.get("errors") else "not_found",
            "reference": reference,
            "raw": response,
        }
    node = edges[0].get("node", {})
    status_hint = str(node.get("displayFulfillmentStatus", ""))
    return {
        "source": "shopify",
        "confidence": "high",
        "reference": reference,
        "summary": f"Shopify shows order {node.get('name', reference)} with fulfillment status '{status_hint}'.",
        "status_hint": status_hint,
        "raw": response,
    }


def customer_reply_for_resolution(summary: str) -> str:
    return f"Here’s what I found: {summary}".strip()


def customer_reply_for_escalation() -> str:
    return (
        "I’m looking into this now. I’ve escalated it with our fulfillment team and will update this thread as soon as I have more information."
    )


def customer_reply_for_missing_info(
    identifiers: Mapping[str, Sequence[str]],
    *,
    text: str = "",
) -> str:
    signals = extract_support_signals(text)
    if signals["mentions_po"] or signals["mentions_inbound"]:
        return "Can you send the PO number or shipment reference so I can pull this up?"
    if signals["mentions_tracking"]:
        return "Can you send the tracking number so I can pull this up?"
    if not identifiers.get("order_numbers") and not identifiers.get("tracking_numbers"):
        return "Can you send the order number so I can pull this up?"
    return "Can you send the tracking number or order number so I can pull this up?"


def format_operator_mentions(slack_user_ids: Sequence[str]) -> str:
    return " ".join(f"<@{user_id}>" for user_id in slack_user_ids if user_id)


def build_brand_name(connection_match: Mapping[str, Any], fallback_channel: str) -> str:
    for key in ("shopify_display_name", "labelogics_display_name"):
        value = str(connection_match.get(key, "")).strip()
        if value:
            return value
    return fallback_channel


def evidence_conflicts(labelogics_evidence: Mapping[str, Any], shopify_evidence: Mapping[str, Any]) -> bool:
    if labelogics_evidence.get("confidence") != "high" or shopify_evidence.get("confidence") != "high":
        return False
    left = str(labelogics_evidence.get("status_hint", "")).strip().lower()
    right = str(shopify_evidence.get("status_hint", "")).strip().lower()
    return bool(left and right and left != right)


def decide_case_action(
    case_row: Mapping[str, Any],
    *,
    question_summary: str,
    source_text: str = "",
    identifiers: Mapping[str, Sequence[str]],
    connection_match: Mapping[str, Any],
    labelogics_evidence: Mapping[str, Any],
    shopify_evidence: Mapping[str, Any],
) -> Dict[str, Any]:
    brand_name = build_brand_name(connection_match, str(case_row.get("source_channel_name", "")))
    issue_category = detect_issue_category(source_text or question_summary, identifiers=identifiers, signals=extract_support_signals(source_text or question_summary))
    if not identifiers.get("order_numbers") and not identifiers.get("tracking_numbers"):
        return {
            "status": "investigating",
            "reply_type": "clarifying",
            "customer_reply": customer_reply_for_missing_info(identifiers, text=source_text),
            "should_escalate": False,
            "escalation_reason": "",
            "resolution_summary": "",
            "brand_name": brand_name,
            "issue_category": issue_category,
            "waiting_on": "customer",
        }
    if str(connection_match.get("status", "unmatched")) == "unmatched":
        return {
            "status": "waiting_human",
            "reply_type": "investigating",
            "customer_reply": customer_reply_for_escalation(),
            "should_escalate": True,
            "escalation_reason": "match_missing",
            "resolution_summary": "",
            "brand_name": brand_name,
            "issue_category": "match_missing",
            "waiting_on": "human",
        }
    if evidence_conflicts(labelogics_evidence, shopify_evidence):
        return {
            "status": "waiting_human",
            "reply_type": "investigating",
            "customer_reply": customer_reply_for_escalation(),
            "should_escalate": True,
            "escalation_reason": "system_conflict",
            "resolution_summary": "",
            "brand_name": brand_name,
            "issue_category": "system_conflict",
            "waiting_on": "human",
        }
    if labelogics_evidence.get("confidence") == "high" and labelogics_evidence.get("summary"):
        return {
            "status": "responded",
            "reply_type": "resolution",
            "customer_reply": customer_reply_for_resolution(str(labelogics_evidence.get("summary", ""))),
            "should_escalate": False,
            "escalation_reason": "",
            "resolution_summary": str(labelogics_evidence.get("summary", "")),
            "brand_name": brand_name,
            "issue_category": issue_category,
            "waiting_on": "",
        }
    if shopify_evidence.get("confidence") == "high" and shopify_evidence.get("summary"):
        return {
            "status": "responded",
            "reply_type": "resolution",
            "customer_reply": customer_reply_for_resolution(str(shopify_evidence.get("summary", ""))),
            "should_escalate": False,
            "escalation_reason": "",
            "resolution_summary": str(shopify_evidence.get("summary", "")),
            "brand_name": brand_name,
            "issue_category": issue_category,
            "waiting_on": "",
        }
    reason = (
        str(labelogics_evidence.get("reason", "")).strip()
        or str(shopify_evidence.get("reason", "")).strip()
        or "insufficient_data"
    )
    return {
        "status": "waiting_human",
        "reply_type": "investigating",
        "customer_reply": customer_reply_for_escalation(),
        "should_escalate": True,
        "escalation_reason": reason,
        "resolution_summary": "",
        "brand_name": brand_name,
        "question_summary": question_summary,
        "issue_category": issue_category if issue_category != "general_support" else reason,
        "waiting_on": "human",
    }


def format_escalation_message(
    case_row: Mapping[str, Any],
    *,
    question_summary: str,
    connection_match: Mapping[str, Any],
    labelogics_evidence: Mapping[str, Any],
    shopify_evidence: Mapping[str, Any],
    escalation_reason: str,
    team_url: str,
    operator_mentions: str,
) -> str:
    permalink = format_slack_permalink(team_url, str(case_row.get("source_channel_id", "")), str(case_row.get("source_thread_ts", "")))
    brand_name = build_brand_name(connection_match, str(case_row.get("source_channel_name", "")))
    lines = [f"*Escalation:* {case_row.get('case_id', '')}"]
    if operator_mentions:
        lines.append(operator_mentions)
    if permalink:
        lines.append(f"Source thread: {permalink}")
    lines.append(f"Customer question: {question_summary or 'Missing question summary'}")
    lines.append(f"Matched brand: {brand_name}")
    primary_owner = str(case_row.get("primary_owner", "")).strip()
    secondary_owner = str(case_row.get("secondary_owner", "")).strip()
    if primary_owner:
        owners = [f"Primary owner: <@{primary_owner}>"]
        if secondary_owner:
            owners.append(f"Secondary owner: <@{secondary_owner}>")
        lines.extend(owners)
    lines.append(f"Labelogics: {labelogics_evidence.get('summary') or labelogics_evidence.get('reason') or 'No evidence'}")
    lines.append(f"Shopify: {shopify_evidence.get('summary') or shopify_evidence.get('reason') or 'No evidence'}")
    lines.append(f"Reason: {escalation_reason}")
    lines.append("Next action: Review the order, confirm system status, and reply in-thread when resolved.")
    return "\n".join(lines)


def format_resolution_update(case_id: str, resolution_summary: str) -> str:
    return f"Resolution update for {case_id}: {resolution_summary}"


def find_related_case(
    connection: sqlite3.Connection,
    *,
    channel_name: str,
    brand_name: str,
    operational_object_key: str,
    complaint_text: str,
) -> Dict[str, Any]:
    candidates = list_recent_cases(connection, channel_name=channel_name, brand_name=brand_name)
    normalized_text = normalize_support_text(complaint_text)
    follow_up_signal = any(pattern in normalized_text for pattern in FOLLOW_UP_PATTERNS)
    best_case: Dict[str, Any] = {}
    best_score = 0.0
    for candidate in candidates:
        score = 0.0
        if operational_object_key and operational_object_key == str(candidate.get("operational_object_key", "")).strip():
            score += 1.0
        similarity = token_similarity(complaint_text, str(candidate.get("complaint_fingerprint", "")))
        score += similarity
        if channel_name and channel_name == str(candidate.get("source_channel_name", "")):
            score += 0.15
        if brand_name and brand_name == str(candidate.get("brand_name", "")):
            score += 0.15
        if score > best_score:
            best_score = score
            best_case = candidate
    if not best_case:
        return {}
    relationship_type = ""
    same_object = operational_object_key and operational_object_key == str(best_case.get("operational_object_key", "")).strip()
    if best_case.get("status") == "resolved" and (best_score >= SIMILARITY_REOPEN_THRESHOLD or same_object):
        relationship_type = "reopened"
    elif follow_up_signal and (same_object or best_score >= SIMILARITY_REOPEN_THRESHOLD):
        relationship_type = "follow_up"
    elif best_score >= SIMILARITY_DUPLICATE_THRESHOLD:
        relationship_type = "duplicate"
    elif same_object:
        relationship_type = "duplicate"
    if not relationship_type:
        return {}
    return {
        "case_row": best_case,
        "relationship_type": relationship_type,
        "relationship_confidence": int(round(best_score * 100)),
    }


def sync_case_from_message(
    connection: sqlite3.Connection,
    *,
    channel_name: str,
    channel_id: str,
    message: Mapping[str, Any],
    connection_match: Mapping[str, Any],
) -> Dict[str, Any]:
    thread_ts = str(message.get("thread_ts") or message.get("ts") or "").strip()
    latest_ts = str(message.get("ts", "")).strip()
    text = message_text(message)
    summary = summarize_questions([message]) or text
    identifiers = extract_order_identifiers(text)
    signals = extract_support_signals(text)
    operational_key = derive_operational_object_key(
        brand_name=build_brand_name(connection_match, channel_name),
        connection_match=connection_match,
        identifiers=identifiers,
    )
    fingerprint = complaint_fingerprint(summary or text)
    brand_name = build_brand_name(connection_match, channel_name)
    issue_category = detect_issue_category(text, identifiers=identifiers, signals=signals)
    thread_link = get_case_thread(connection, channel_id, thread_ts)
    relationship_type = DEFAULT_RELATIONSHIP_TYPE
    relationship_confidence = 100
    related_case_id = ""
    created = False
    preserve_primary_thread = False

    if thread_link:
        case_id = str(thread_link.get("case_id", "")).strip()
        existing = get_case(connection, case_id)
        existing_latest_ts = str(existing.get("latest_message_ts", "")).strip()
        if existing_latest_ts and float(latest_ts or 0) <= float(existing_latest_ts or 0):
            relationship_type = str(existing.get("relationship_type", "")).strip() or str(thread_link.get("relationship_type", "")).strip() or DEFAULT_RELATIONSHIP_TYPE
            related_case_id = str(existing.get("related_case_id", "")).strip() or str(thread_link.get("related_case_id", "")).strip()
            relationship_confidence = max(int(thread_link.get("relationship_confidence", 0) or 0), 90)
        elif existing.get("status") == "resolved":
            relationship_type = "reopened"
            related_case_id = case_id
        else:
            relationship_type = "follow_up"
            related_case_id = case_id
        relationship_confidence = max(int(thread_link.get("relationship_confidence", 0) or 0), 90)
    else:
        related = find_related_case(
            connection,
            channel_name=channel_name,
            brand_name=brand_name,
            operational_object_key=operational_key,
            complaint_text=text or summary or fingerprint,
        )
        if related:
            existing = dict(related["case_row"])
            case_id = str(existing.get("case_id", "")).strip()
            relationship_type = str(related["relationship_type"])
            relationship_confidence = int(related["relationship_confidence"])
            related_case_id = case_id
            preserve_primary_thread = True
        else:
            case_id = case_id_for_thread(channel_id, thread_ts)
            existing = get_case(connection, case_id)
            created = not bool(existing)
            relationship_type = DEFAULT_RELATIONSHIP_TYPE
            relationship_confidence = 100

    next_status = existing.get("status", "new") or "new"
    resolved_at = existing.get("resolved_at")
    if relationship_type == "reopened" or (existing and existing.get("status") == "resolved"):
        next_status = "investigating"
        resolved_at = None
    source_channel_name = str(existing.get("source_channel_name", "")) if preserve_primary_thread else channel_name
    source_channel_id = str(existing.get("source_channel_id", "")) if preserve_primary_thread else channel_id
    source_thread_ts = str(existing.get("source_thread_ts", "")) if preserve_primary_thread else thread_ts
    canonical_case_id = str(existing.get("canonical_case_id", "")).strip() or case_id
    payload = {
        "case_id": case_id,
        "canonical_case_id": canonical_case_id,
        "related_case_id": related_case_id,
        "relationship_type": relationship_type,
        "relationship_confidence": relationship_confidence,
        "source_channel_name": source_channel_name,
        "source_channel_id": source_channel_id,
        "source_thread_ts": source_thread_ts,
        "latest_message_ts": latest_ts if not existing else max_slack_ts(str(existing.get("latest_message_ts", "")), latest_ts),
        "status": next_status,
        "brand_name": brand_name,
        "connection_status": str(connection_match.get("status", "unmatched")),
        "shopify_store_domain": str(connection_match.get("shopify_store_domain", "")),
        "labelogics_account_id": str(connection_match.get("labelogics_account_id", "")),
        "operational_object_key": operational_key or str(existing.get("operational_object_key", "")),
        "complaint_fingerprint": fingerprint or str(existing.get("complaint_fingerprint", "")),
        "issue_category": issue_category or str(existing.get("issue_category", "")),
        "primary_owner": str(existing.get("primary_owner", "")),
        "secondary_owner": str(existing.get("secondary_owner", "")),
        "waiting_on": str(existing.get("waiting_on", "")),
        "customer_question_summary": summary or str(existing.get("customer_question_summary", "")),
        "customer_facing_reply": str(existing.get("customer_facing_reply", "")),
        "latest_evidence_summary": str(existing.get("latest_evidence_summary", "")),
        "latest_resolution_summary": str(existing.get("latest_resolution_summary", "")),
        "escalation_channel_name": str(existing.get("escalation_channel_name", "")),
        "escalation_channel_id": str(existing.get("escalation_channel_id", "")),
        "escalation_thread_ts": str(existing.get("escalation_thread_ts", "")),
        "escalation_reason": str(existing.get("escalation_reason", "")),
        "resolved_at": resolved_at,
    }
    upsert_support_case(connection, payload)
    upsert_case_thread(
        connection,
        channel_id=channel_id,
        thread_ts=thread_ts,
        case_id=case_id,
        relationship_type=relationship_type,
        related_case_id=related_case_id,
        relationship_confidence=relationship_confidence,
        last_message_ts=latest_ts,
    )
    return {
        "case_id": case_id,
        "created": created,
        "relationship_type": relationship_type,
        "related_case_id": related_case_id,
        "canonical_case_id": canonical_case_id,
        "relationship_confidence": relationship_confidence,
    }


def evidence_summary(labelogics_evidence: Mapping[str, Any], shopify_evidence: Mapping[str, Any]) -> str:
    parts: List[str] = []
    for label, payload in (("Shipping OS", labelogics_evidence), ("Shopify", shopify_evidence)):
        summary = str(payload.get("summary", "")).strip()
        reason = str(payload.get("reason", "")).strip()
        rendered = summary or reason
        if rendered:
            parts.append(f"{label}: {rendered}")
    return " | ".join(parts)


def hydrate_case_context(
    connection: sqlite3.Connection,
    case_row: Mapping[str, Any],
    *,
    bot_user_id: str,
    resolution_markers: Sequence[str],
) -> Dict[str, Any]:
    try:
        thread_messages = slack_thread_messages(str(case_row.get("source_channel_id", "")), str(case_row.get("source_thread_ts", "")))
    except Exception:
        thread_messages = []
    if not thread_messages and str(case_row.get("customer_question_summary", "")).strip():
        thread_messages = [
            {
                "ts": str(case_row.get("latest_message_ts", case_row.get("source_thread_ts", ""))),
                "thread_ts": str(case_row.get("source_thread_ts", "")),
                "user": "",
                "text": str(case_row.get("customer_question_summary", "")),
            }
        ]
    customer_messages = [message for message in thread_messages if not should_ignore_slack_message(message, bot_user_id=bot_user_id)]
    question_summary = summarize_questions(customer_messages) or str(case_row.get("customer_question_summary", ""))
    connection_match = get_connection_match(connection, str(case_row.get("source_channel_name", "")))
    identifiers = extract_order_identifiers("\n".join(message_text(message) for message in customer_messages))
    labelogics_evidence = labelogics_lookup(identifiers, connection_match)
    shopify_evidence = shopify_lookup(identifiers, connection_match)
    resolved_by_human = any(
        is_resolution_marker(message_text(message), resolution_markers)
        for message in customer_messages
    )
    return {
        "thread_messages": thread_messages,
        "customer_messages": customer_messages,
        "question_summary": question_summary,
        "connection_match": connection_match,
        "identifiers": identifiers,
        "labelogics_evidence": labelogics_evidence,
        "shopify_evidence": shopify_evidence,
        "resolved_by_human": resolved_by_human,
    }


def evaluate_case(
    connection: sqlite3.Connection,
    *,
    case_row: Mapping[str, Any],
    bot_user_id: str,
    operator_user_ids: Sequence[str],
    resolution_markers: Sequence[str],
) -> Dict[str, Any]:
    context = hydrate_case_context(
        connection,
        case_row,
        bot_user_id=bot_user_id,
        resolution_markers=resolution_markers,
    )
    case_id = str(case_row.get("case_id", ""))
    current = get_case(connection, case_id) or dict(case_row)
    if context["resolved_by_human"]:
        current["status"] = "resolved"
        current["resolved_at"] = utc_now().isoformat()
        return {"current": current, "context": context, "action": {}, "resolved_by_human": True}

    source_text = "\n".join(message_text(message) for message in context["customer_messages"])
    action = decide_case_action(
        case_row,
        question_summary=context["question_summary"],
        source_text=source_text,
        identifiers=context["identifiers"],
        connection_match=context["connection_match"],
        labelogics_evidence=context["labelogics_evidence"],
        shopify_evidence=context["shopify_evidence"],
    )
    owners = resolve_escalation_owners(
        issue_category=str(action.get("issue_category", "")),
        escalation_reason=str(action.get("escalation_reason", "")),
        operator_user_ids=operator_user_ids,
    )
    current["status"] = action["status"]
    current["brand_name"] = action["brand_name"]
    current["connection_status"] = str(context["connection_match"].get("status", "unmatched"))
    current["shopify_store_domain"] = str(context["connection_match"].get("shopify_store_domain", ""))
    current["labelogics_account_id"] = str(context["connection_match"].get("labelogics_account_id", ""))
    current["customer_question_summary"] = context["question_summary"]
    current["latest_resolution_summary"] = str(action.get("resolution_summary", ""))
    current["issue_category"] = str(action.get("issue_category", current.get("issue_category", "")))
    current["primary_owner"] = owners.get("primary_owner", "")
    current["secondary_owner"] = owners.get("secondary_owner", "")
    current["waiting_on"] = str(action.get("waiting_on", ""))
    current["latest_evidence_summary"] = evidence_summary(context["labelogics_evidence"], context["shopify_evidence"])
    current["escalation_reason"] = str(action.get("escalation_reason", "")) if action.get("should_escalate") else ""
    return {"current": current, "context": context, "action": action, "resolved_by_human": False}


def process_case(
    connection: sqlite3.Connection,
    *,
    case_row: Mapping[str, Any],
    queue_channel_name: str,
    queue_channel_id: str,
    team_url: str,
    bot_user_id: str,
    operator_user_ids: Sequence[str],
    resolution_markers: Sequence[str],
) -> Dict[str, Any]:
    evaluated = evaluate_case(
        connection,
        case_row=case_row,
        bot_user_id=bot_user_id,
        operator_user_ids=operator_user_ids,
        resolution_markers=resolution_markers,
    )
    context = evaluated["context"]
    case_id = str(case_row.get("case_id", ""))
    if evaluated["resolved_by_human"]:
        upsert_support_case(connection, evaluated["current"])
        record_case_event(
            connection,
            event_key=f"resolved:{case_id}",
            case_id=case_id,
            event_type="resolved",
            payload={"source": "thread_marker"},
        )
        return {"resolved_cases": 1, "customer_replies": 0, "escalations_posted": 0, "queue_updates": 0}

    action = evaluated["action"]
    current = evaluated["current"]
    counts = {"resolved_cases": 0, "customer_replies": 0, "escalations_posted": 0, "queue_updates": 0}

    if action["customer_reply"] and action["customer_reply"] != str(current.get("customer_facing_reply", "")):
        response = slack_post_message(
            str(current.get("source_channel_id", "")),
            action["customer_reply"],
            thread_ts=str(current.get("source_thread_ts", "")),
        )
        if response.get("ok"):
            counts["customer_replies"] += 1
            record_case_event(
                connection,
                event_key=f"customer_reply:{case_id}:{response.get('ts', '')}",
                case_id=case_id,
                event_type="customer_reply",
                slack_channel_id=str(current.get("source_channel_id", "")),
                slack_message_ts=str(response.get("ts", "")),
                payload={"text": action["customer_reply"], "reply_type": action["reply_type"]},
            )
            current["customer_facing_reply"] = action["customer_reply"]

    if action["should_escalate"]:
        operator_mentions = format_operator_mentions(operator_user_ids)
        escalation_message = format_escalation_message(
            current,
            question_summary=context["question_summary"],
            connection_match=context["connection_match"],
            labelogics_evidence=context["labelogics_evidence"],
            shopify_evidence=context["shopify_evidence"],
            escalation_reason=str(action["escalation_reason"]),
            team_url=team_url,
            operator_mentions=operator_mentions,
        )
        if not str(current.get("escalation_thread_ts", "")) and queue_channel_id:
            response = slack_post_message(queue_channel_id, escalation_message)
            if response.get("ok"):
                counts["escalations_posted"] += 1
                current["escalation_thread_ts"] = str(response.get("ts", ""))
                current["escalation_channel_id"] = queue_channel_id
                current["escalation_channel_name"] = queue_channel_name
                record_case_event(
                    connection,
                    event_key=f"escalation:{case_id}:{response.get('ts', '')}",
                    case_id=case_id,
                    event_type="escalation",
                    slack_channel_id=queue_channel_id,
                    slack_message_ts=str(response.get("ts", "")),
                    payload={"text": escalation_message, "reason": action["escalation_reason"]},
                )
        upsert_case_assignments(connection, case_id, operator_user_ids)
        current["escalation_reason"] = str(action["escalation_reason"])
    elif (
        str(current.get("escalation_thread_ts", ""))
        and action["resolution_summary"]
        and action["resolution_summary"] != str(current.get("latest_resolution_summary", ""))
    ):
        queue_update = format_resolution_update(case_id, action["resolution_summary"])
        response = slack_post_message(
            str(current.get("escalation_channel_id", "")),
            queue_update,
            thread_ts=str(current.get("escalation_thread_ts", "")),
        )
        if response.get("ok"):
            counts["queue_updates"] += 1
            record_case_event(
                connection,
                event_key=f"queue_update:{case_id}:{response.get('ts', '')}",
                case_id=case_id,
                event_type="queue_update",
                slack_channel_id=str(current.get("escalation_channel_id", "")),
                slack_message_ts=str(response.get("ts", "")),
                payload={"text": queue_update},
            )

    upsert_support_case(connection, current)
    return counts


def ingest_support_messages(
    connection: sqlite3.Connection,
    *,
    visible_channels: Mapping[str, str],
    channel_names: Sequence[str],
    bot_user_id: str,
    oldest: str,
) -> Dict[str, Any]:
    created_cases = 0
    new_events = 0
    missing_channels: List[str] = []
    tracked_messages = 0
    for channel_name in channel_names:
        channel_id = visible_channels.get(channel_name, "")
        if not channel_id:
            missing_channels.append(channel_name)
            continue
        history = slack_channel_history(channel_id, oldest=oldest)
        connection_match = get_connection_match(connection, channel_name)
        for message in history:
            if not should_track_support_message(connection, channel_id=channel_id, message=message, bot_user_id=bot_user_id):
                continue
            tracked_messages += 1
            synced = sync_case_from_message(
                connection,
                channel_name=channel_name,
                channel_id=channel_id,
                message=message,
                connection_match=connection_match,
            )
            if synced["created"]:
                created_cases += 1
            inserted = record_case_event(
                connection,
                event_key=f"slack_message:{channel_id}:{message.get('ts', '')}",
                case_id=synced["case_id"],
                event_type="intake_message",
                slack_channel_id=channel_id,
                slack_message_ts=str(message.get("ts", "")),
                payload={
                    "text": message_text(message),
                    "thread_ts": str(message.get("thread_ts", "")),
                    "relationship_type": synced["relationship_type"],
                    "related_case_id": synced["related_case_id"],
                },
            )
            if inserted:
                new_events += 1
    return {
        "created_cases": created_cases,
        "new_events": new_events,
        "missing_channels": missing_channels,
        "tracked_messages": tracked_messages,
    }


def review_case_payload(
    connection: sqlite3.Connection,
    *,
    case_row: Mapping[str, Any],
    team_url: str,
    bot_user_id: str,
    operator_user_ids: Sequence[str],
    resolution_markers: Sequence[str],
) -> Dict[str, Any]:
    evaluated = evaluate_case(
        connection,
        case_row=case_row,
        bot_user_id=bot_user_id,
        operator_user_ids=operator_user_ids,
        resolution_markers=resolution_markers,
    )
    current = evaluated["current"]
    upsert_support_case(connection, current)
    assignment_ids = [owner for owner in (str(current.get("primary_owner", "")), str(current.get("secondary_owner", ""))) if owner]
    if assignment_ids:
        upsert_case_assignments(connection, str(current.get("case_id", "")), assignment_ids)
    action = evaluated["action"]
    context = evaluated["context"]
    return {
        "case_id": str(current.get("case_id", "")),
        "channel": str(current.get("source_channel_name", "")),
        "channel_id": str(current.get("source_channel_id", "")),
        "ts": str(current.get("latest_message_ts", "")),
        "thread_ts": str(current.get("source_thread_ts", "")),
        "permalink": format_slack_permalink(team_url, str(current.get("source_channel_id", "")), str(current.get("source_thread_ts", ""))),
        "text": str(current.get("customer_question_summary", ""))[:300],
        "question_summary": str(current.get("customer_question_summary", "")),
        "identifiers": context["identifiers"],
        "connection_status": str(current.get("connection_status", "")),
        "brand_name": str(current.get("brand_name", "")),
        "relationship_type": str(current.get("relationship_type", "")),
        "related_case_id": str(current.get("related_case_id", "")),
        "issue_category": str(current.get("issue_category", "")),
        "primary_owner": str(current.get("primary_owner", "")),
        "secondary_owner": str(current.get("secondary_owner", "")),
        "waiting_on": str(current.get("waiting_on", "")),
        "recommended_action": {
            "status": action.get("status", str(current.get("status", ""))),
            "reply_type": action.get("reply_type", ""),
            "customer_reply": action.get("customer_reply", str(current.get("customer_facing_reply", ""))),
            "should_escalate": bool(action.get("should_escalate", str(current.get("status", "")) in {"escalated", "waiting_human"})),
            "escalation_reason": action.get("escalation_reason", str(current.get("escalation_reason", ""))),
        },
        "evidence": {
            "labelogics": {
                "confidence": context["labelogics_evidence"].get("confidence", ""),
                "reason": context["labelogics_evidence"].get("reason", ""),
                "summary": context["labelogics_evidence"].get("summary", ""),
            },
            "shopify": {
                "confidence": context["shopify_evidence"].get("confidence", ""),
                "reason": context["shopify_evidence"].get("reason", ""),
                "summary": context["shopify_evidence"].get("summary", ""),
            },
        },
    }


def run_agent(
    config: Mapping[str, Any],
    directories: Mapping[str, Path],
    *,
    now: datetime,
    force_run: bool,
    live_check_payload: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    runtime = agent_runtime_config(config)
    schedule_state = schedule_window_status(config, now=now)
    if not schedule_state["allowed"] and not force_run:
        return {"status": "skipped", "reason": "outside_schedule_window", "schedule": schedule_state}

    live_checks_payload = dict(live_check_payload or {})
    slack_state = live_checks_payload.get("slack") or slack_live_check()
    if not slack_state.get("ok"):
        return {"status": "blocked", "reason": "slack_unavailable", "slack": slack_state, "schedule": schedule_state}

    visible_channels = {
        str(item.get("name", "")): str(item.get("id", ""))
        for item in slack_state.get("visible_channels", [])
        if str(item.get("name", "")).strip()
    }
    queue_channel_name = runtime["queue_channel"]
    queue_channel_id = visible_channels.get(queue_channel_name, "")
    operator_user_ids = split_csv_env(runtime["escalation_slack_user_ids_env"])
    bot_user_id = str(slack_state.get("user_id", ""))
    team_url = str(slack_state.get("team_url", ""))
    oldest = str((now.astimezone(timezone.utc) - timedelta(hours=runtime["lookback_hours"])).timestamp())

    connection = open_connections_db(directories["connections_db"])
    try:
        ingest_result = ingest_support_messages(
            connection,
            visible_channels=visible_channels,
            channel_names=split_csv_env("SUPPORT_SLACK_CHANNELS"),
            bot_user_id=bot_user_id,
            oldest=oldest,
        )
        customer_replies = 0
        escalations_posted = 0
        queue_updates = 0
        resolved_cases = 0

        for case_row in list_open_cases(connection):
            processed = process_case(
                connection,
                case_row=case_row,
                queue_channel_name=queue_channel_name,
                queue_channel_id=queue_channel_id,
                team_url=team_url,
                bot_user_id=bot_user_id,
                operator_user_ids=operator_user_ids,
                resolution_markers=runtime["resolution_markers"],
            )
            customer_replies += processed["customer_replies"]
            escalations_posted += processed["escalations_posted"]
            queue_updates += processed["queue_updates"]
            resolved_cases += processed["resolved_cases"]

        connection.commit()
        return {
            "status": "completed",
            "schedule": schedule_state,
            "queue_channel": {"name": queue_channel_name, "id": queue_channel_id},
            "missing_visible_channels": ingest_result["missing_channels"],
            "operator_user_ids": operator_user_ids,
            "counts": {
                "created_cases": ingest_result["created_cases"],
                "new_events": ingest_result["new_events"],
                "tracked_messages": ingest_result["tracked_messages"],
                "customer_replies": customer_replies,
                "escalations_posted": escalations_posted,
                "queue_updates": queue_updates,
                "resolved_cases": resolved_cases,
                "open_cases": connection.execute(
                    "SELECT COUNT(*) FROM support_cases WHERE status IN ({})".format(",".join("?" for _ in DEFAULT_CASE_STATUSES_OPEN)),
                    DEFAULT_CASE_STATUSES_OPEN,
                ).fetchone()[0],
            },
            "db_counts": current_db_counts(connection),
        }
    finally:
        connection.close()


def review_candidates(
    config: Mapping[str, Any],
    directories: Mapping[str, Path],
    *,
    now: datetime,
    live_check_payload: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    runtime = agent_runtime_config(config)
    live_checks_payload = dict(live_check_payload or {})
    slack_state = live_checks_payload.get("slack") or slack_live_check()
    if not slack_state.get("ok"):
        return {"status": "blocked", "reason": "slack_unavailable", "slack": slack_state}

    visible_channels = {
        str(item.get("name", "")): str(item.get("id", ""))
        for item in slack_state.get("visible_channels", [])
        if str(item.get("name", "")).strip()
    }
    bot_user_id = str(slack_state.get("user_id", ""))
    team_url = str(slack_state.get("team_url", ""))
    oldest = str((now.astimezone(timezone.utc) - timedelta(hours=runtime["lookback_hours"])).timestamp())
    candidates: List[Dict[str, Any]] = []
    operator_user_ids = split_csv_env(runtime["escalation_slack_user_ids_env"])

    connection = open_connections_db(directories["connections_db"])
    try:
        ingest_result = ingest_support_messages(
            connection,
            visible_channels=visible_channels,
            channel_names=split_csv_env("SUPPORT_SLACK_CHANNELS"),
            bot_user_id=bot_user_id,
            oldest=oldest,
        )
        for case_row in list_open_cases(connection):
            candidates.append(
                review_case_payload(
                    connection,
                    case_row=case_row,
                    team_url=team_url,
                    bot_user_id=bot_user_id,
                    operator_user_ids=operator_user_ids,
                    resolution_markers=runtime["resolution_markers"],
                )
            )
        connection.commit()
        candidates.sort(key=lambda item: float(item.get("ts", "0") or 0), reverse=True)
        return {
            "status": "ready",
            "count": len(candidates),
            "ingest": ingest_result,
            "candidates": candidates[:25],
        }
    finally:
        connection.close()


def print_summary(summary: Mapping[str, Any]) -> None:
    print(json.dumps(summary, indent=2, sort_keys=True))


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    env_path = Path(args.env_file).expanduser().resolve()
    load_env_file(env_path)
    config = read_config(config_path)
    workspace_root = resolve_workspace_root(config)
    directories = resolve_directories(config, workspace_root)
    if not args.validate_only:
        ensure_directories(directories)

    include_live_checks = args.check_live or args.run_agent or args.review_candidates
    summary = build_summary(config, config_path, directories, include_live_checks=include_live_checks)
    if args.run_agent and summary["status"] == "ready":
        timezone_name = summary["timezone"]
        runtime_now = parse_runtime_now(args.now, timezone_name)
        summary["agent_run"] = run_agent(
            config,
            directories,
            now=runtime_now,
            force_run=args.force_run,
            live_check_payload=summary.get("live_checks", {}),
        )
    if args.review_candidates and summary["status"] == "ready":
        timezone_name = summary["timezone"]
        runtime_now = parse_runtime_now(args.now, timezone_name)
        summary["review_candidates"] = review_candidates(
            config,
            directories,
            now=runtime_now,
            live_check_payload=summary.get("live_checks", {}),
        )

    print_summary(summary)
    if summary["status"] != "ready":
        raise SystemExit(1)
    if args.run_agent and summary.get("agent_run", {}).get("status") == "blocked":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
