#!/usr/bin/env python3
"""Minimal QuickBooks Online vendor sync helper."""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


TOKEN_ENDPOINT = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
API_BASE = "https://quickbooks.api.intuit.com"
DEFAULT_TOKEN_FILENAME = "qbo_tokens.json"
_VENDOR_CACHE: Dict[str, Any] = {"path": None, "vendors": None, "loaded_at": 0.0}


def normalize_spaces(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def token_store_path(configured_path: Optional[Path] = None) -> Optional[Path]:
    if configured_path:
        return configured_path
    env_path = os.getenv("QBO_TOKEN_STORE_PATH", "").strip()
    return Path(env_path) if env_path else None


def token_store_exists(configured_path: Optional[Path] = None) -> bool:
    path = token_store_path(configured_path)
    return bool(path and path.exists())


def load_token_store(configured_path: Optional[Path] = None) -> Dict[str, Any]:
    path = token_store_path(configured_path)
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def save_token_store(payload: Dict[str, Any], configured_path: Optional[Path] = None) -> None:
    path = token_store_path(configured_path)
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def qbo_client_id() -> str:
    return os.getenv("QBO_CLIENT_ID", "").strip()


def qbo_client_secret() -> str:
    return os.getenv("QBO_CLIENT_SECRET", "").strip()


def qbo_realm_id(configured_path: Optional[Path] = None) -> str:
    stored = load_token_store(configured_path)
    return str(stored.get("realm_id") or os.getenv("QBO_REALM_ID", "")).strip()


def qbo_refresh_token(configured_path: Optional[Path] = None) -> str:
    stored = load_token_store(configured_path)
    return str(stored.get("refresh_token") or os.getenv("QBO_REFRESH_TOKEN", "")).strip()


def qbo_is_configured(configured_path: Optional[Path] = None) -> bool:
    return bool(qbo_client_id() and qbo_client_secret() and qbo_realm_id(configured_path) and qbo_refresh_token(configured_path))


def bearer_headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/text",
    }


def refresh_access_token(configured_path: Optional[Path] = None) -> Dict[str, Any]:
    if not qbo_is_configured(configured_path):
        raise RuntimeError("QBO vendor sync is not configured.")

    credentials = f"{qbo_client_id()}:{qbo_client_secret()}".encode("utf-8")
    basic = base64.b64encode(credentials).decode("ascii")
    data = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": qbo_refresh_token(configured_path),
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        TOKEN_ENDPOINT,
        data=data,
        headers={
            "Authorization": f"Basic {basic}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"QBO token refresh failed: {exc.code} {detail or exc.reason}") from exc
    expires_in = int(payload.get("expires_in", 3600))
    stored = load_token_store(configured_path)
    stored.update(
        {
            "access_token": payload.get("access_token", ""),
            "refresh_token": payload.get("refresh_token") or qbo_refresh_token(configured_path),
            "realm_id": stored.get("realm_id") or os.getenv("QBO_REALM_ID", "").strip(),
            "expires_at": int(time.time()) + expires_in - 60,
            "refreshed_at": int(time.time()),
        }
    )
    save_token_store(stored, configured_path)
    return stored


def access_token(configured_path: Optional[Path] = None) -> str:
    stored = load_token_store(configured_path)
    current = str(stored.get("access_token", "")).strip()
    expires_at = int(stored.get("expires_at", 0) or 0)
    if current and expires_at > int(time.time()):
        return current
    refreshed = refresh_access_token(configured_path)
    return str(refreshed.get("access_token", "")).strip()


def query_qbo(sql: str, configured_path: Optional[Path] = None) -> Dict[str, Any]:
    realm_id = qbo_realm_id(configured_path)
    request = urllib.request.Request(
        f"{API_BASE}/v3/company/{realm_id}/query?minorversion=75",
        data=sql.encode("utf-8"),
        headers=bearer_headers(access_token(configured_path)),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"QBO query failed: {exc.code} {detail or exc.reason}") from exc


def fetch_vendors(configured_path: Optional[Path] = None, *, force_refresh: bool = False) -> List[Dict[str, Any]]:
    path_key = str(token_store_path(configured_path) or "")
    cache_is_warm = _VENDOR_CACHE["path"] == path_key and _VENDOR_CACHE["vendors"] is not None and not force_refresh
    if cache_is_warm and time.time() - float(_VENDOR_CACHE["loaded_at"] or 0.0) < 300:
        return list(_VENDOR_CACHE["vendors"])
    if not qbo_is_configured(configured_path):
        return []

    vendors: List[Dict[str, Any]] = []
    start = 1
    while True:
        payload = query_qbo(
            f"select Id, DisplayName, FullyQualifiedName, PrintOnCheckName, Active from Vendor startposition {start} maxresults 1000",
            configured_path,
        )
        batch = payload.get("QueryResponse", {}).get("Vendor", []) or []
        vendors.extend(batch)
        if len(batch) < 1000:
            break
        start += len(batch)

    _VENDOR_CACHE.update({"path": path_key, "vendors": list(vendors), "loaded_at": time.time()})
    return vendors


def build_vendor_aliases(vendors: List[Dict[str, Any]]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for vendor in vendors:
        canonical = normalize_spaces(
            vendor.get("DisplayName")
            or vendor.get("FullyQualifiedName")
            or vendor.get("PrintOnCheckName")
            or ""
        )
        if not canonical:
            continue
        for candidate in {
            canonical,
            normalize_spaces(vendor.get("FullyQualifiedName", "")),
            normalize_spaces(vendor.get("PrintOnCheckName", "")),
            canonical.replace("&", "and"),
        }:
            if candidate:
                aliases[candidate] = canonical
    return aliases


def enrich_rules_with_qbo(rules: Dict[str, Any], configured_path: Optional[Path] = None) -> Dict[str, Any]:
    enriched = {
        key: (dict(value) if isinstance(value, dict) else list(value) if isinstance(value, list) else value)
        for key, value in rules.items()
    }
    vendors = fetch_vendors(configured_path)
    if vendors:
        enriched.setdefault("vendor_aliases", {}).update(build_vendor_aliases(vendors))
    return enriched


def connection_status(configured_path: Optional[Path] = None) -> Dict[str, Any]:
    if not (qbo_client_id() and qbo_client_secret()):
        return {"configured": False, "connected": False, "vendor_count": 0, "message": "QBO credentials not configured."}
    if not (qbo_realm_id(configured_path) and qbo_refresh_token(configured_path)):
        return {"configured": True, "connected": False, "vendor_count": 0, "message": "QBO realm or refresh token missing."}
    try:
        vendors = fetch_vendors(configured_path)
    except Exception as exc:
        return {"configured": True, "connected": False, "vendor_count": 0, "message": str(exc)}
    return {"configured": True, "connected": True, "vendor_count": len(vendors), "message": "Vendor sync active."}
