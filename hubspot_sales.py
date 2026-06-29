#!/usr/bin/env python3
"""Minimal HubSpot sales helpers for deal creation."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_RULES_PATH = ROOT_DIR / "config" / "hubspot_sales_rules.json"
DEFAULT_API_BASE = "https://api.hubapi.com"
DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID = 3
DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID = 5


class HubSpotSalesError(Exception):
    def __init__(self, message: str, *, status_code: int = 502, payload: Optional[Mapping[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = dict(payload or {})


@dataclass(frozen=True)
class DealCreateRequest:
    properties: Dict[str, str]
    company_id: str = ""
    contact_id: str = ""


def env_value(name: str) -> str:
    return os.getenv(name, "").strip()


def hubspot_access_token() -> str:
    for name in ("HUBSPOT_PRIVATE_APP_TOKEN", "HUBSPOT_ACCESS_TOKEN", "HS_PRIVATE_APP_TOKEN"):
        value = env_value(name)
        if value:
            return value
    return ""


def hubspot_api_base() -> str:
    return env_value("HUBSPOT_API_BASE_URL").rstrip("/") or DEFAULT_API_BASE


def read_sales_rules(path: Optional[Path | str] = None) -> Dict[str, Any]:
    rules_path = Path(path).expanduser() if path else DEFAULT_RULES_PATH
    try:
        return json.loads(rules_path.read_text())
    except FileNotFoundError as exc:
        raise HubSpotSalesError(f"HubSpot sales rules file not found: {rules_path}", status_code=500) from exc
    except json.JSONDecodeError as exc:
        raise HubSpotSalesError(f"HubSpot sales rules file is not valid JSON: {rules_path}", status_code=500) from exc


def deal_rules(rules: Mapping[str, Any]) -> Mapping[str, Any]:
    objects = rules.get("objects", {})
    if not isinstance(objects, Mapping):
        return {}
    deal = objects.get("deal", {})
    return deal if isinstance(deal, Mapping) else {}


def deal_required_properties(rules: Mapping[str, Any]) -> List[str]:
    raw = deal_rules(rules).get("required_properties", [])
    if not isinstance(raw, list):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]


def deal_allowed_properties(rules: Mapping[str, Any]) -> List[str]:
    deal = deal_rules(rules)
    configured: List[str] = []
    for key in ("required_properties", "recommended_properties"):
        raw = deal.get(key, [])
        if isinstance(raw, list):
            configured.extend(str(item).strip() for item in raw if str(item).strip())
    extra = [item.strip() for item in env_value("HUBSPOT_DEAL_EXTRA_PROPERTIES").split(",") if item.strip()]
    seen = set()
    allowed = []
    for item in configured + extra:
        if item not in seen:
            seen.add(item)
            allowed.append(item)
    return allowed


def deal_required_associations(rules: Mapping[str, Any]) -> List[str]:
    required: List[str] = []
    for rule in deal_rules(rules).get("rules", []):
        if not isinstance(rule, Mapping):
            continue
        if str(rule.get("when", "")).strip() != "always":
            continue
        association = str(rule.get("require_association", "")).strip()
        if association and association not in required:
            required.append(association)
    return required


def _with_default(value: str, env_name: str) -> str:
    return value or env_value(env_name)


def normalize_deal_create_request(raw_payload: Mapping[str, Any], rules: Mapping[str, Any]) -> DealCreateRequest:
    allowed = set(deal_allowed_properties(rules))
    aliases = {
        "deal_name": "dealname",
        "owner_id": "hubspot_owner_id",
        "service_line": "anata_service_line",
        "lead_source_detail": "anata_lead_source_detail",
        "next_step": "anata_next_step",
        "next_step_due_at": "anata_next_step_due_at",
    }
    properties: Dict[str, str] = {}
    nested = raw_payload.get("properties", {})
    if isinstance(nested, Mapping):
        for key, value in nested.items():
            prop = aliases.get(str(key), str(key))
            if prop in allowed and str(value).strip():
                properties[prop] = str(value).strip()
    for key, value in raw_payload.items():
        prop = aliases.get(str(key), str(key))
        if prop in allowed and str(value).strip():
            properties[prop] = str(value).strip()
    properties["pipeline"] = _with_default(properties.get("pipeline", ""), "HUBSPOT_DEFAULT_DEAL_PIPELINE")
    properties["dealstage"] = _with_default(properties.get("dealstage", ""), "HUBSPOT_DEFAULT_DEAL_STAGE")
    properties["hubspot_owner_id"] = _with_default(properties.get("hubspot_owner_id", ""), "HUBSPOT_DEFAULT_OWNER_ID")
    properties["anata_service_line"] = _with_default(properties.get("anata_service_line", ""), "HUBSPOT_DEFAULT_SERVICE_LINE")
    properties["anata_lead_source_detail"] = _with_default(
        properties.get("anata_lead_source_detail", ""),
        "HUBSPOT_DEFAULT_LEAD_SOURCE_DETAIL",
    )
    properties = {key: value for key, value in properties.items() if str(value).strip()}
    company_id = str(
        raw_payload.get("hubspot_company_id")
        or raw_payload.get("company_id")
        or raw_payload.get("associated_company_id")
        or ""
    ).strip()
    contact_id = str(
        raw_payload.get("hubspot_contact_id")
        or raw_payload.get("contact_id")
        or raw_payload.get("associated_contact_id")
        or ""
    ).strip()
    return DealCreateRequest(properties=properties, company_id=company_id, contact_id=contact_id)


def validate_deal_create_request(request: DealCreateRequest, rules: Mapping[str, Any]) -> List[str]:
    errors = []
    for prop in deal_required_properties(rules):
        if not request.properties.get(prop):
            errors.append(f"Missing required deal property: {prop}")
    required_associations = set(deal_required_associations(rules))
    if "company" in required_associations and not request.company_id:
        errors.append("Missing required company association: hubspot_company_id")
    if "contact" in required_associations and not request.contact_id:
        errors.append("Missing required contact association: hubspot_contact_id")
    return errors


def _association_type_id(env_name: str, default: int) -> int:
    raw = env_value(env_name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def build_deal_create_body(request: DealCreateRequest) -> Dict[str, Any]:
    body: Dict[str, Any] = {"properties": request.properties}
    associations = []
    if request.contact_id:
        associations.append(
            {
                "to": {"id": request.contact_id},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": _association_type_id(
                            "HUBSPOT_DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID",
                            DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID,
                        ),
                    }
                ],
            }
        )
    if request.company_id:
        associations.append(
            {
                "to": {"id": request.company_id},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": _association_type_id(
                            "HUBSPOT_DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID",
                            DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID,
                        ),
                    }
                ],
            }
        )
    if associations:
        body["associations"] = associations
    return body


def hubspot_deal_url(deal_id: str) -> str:
    portal_id = env_value("HUBSPOT_PORTAL_ID")
    if not portal_id or not deal_id:
        return ""
    return f"https://app.hubspot.com/contacts/{portal_id}/record/0-3/{deal_id}"


def create_deal(request: DealCreateRequest, *, token: Optional[str] = None) -> Dict[str, Any]:
    access_token = token if token is not None else hubspot_access_token()
    if not access_token:
        raise HubSpotSalesError(
            "HubSpot token is not configured. Set HUBSPOT_PRIVATE_APP_TOKEN or HUBSPOT_ACCESS_TOKEN.",
            status_code=503,
        )
    body = build_deal_create_body(request)
    encoded = json.dumps(body).encode("utf-8")
    url = f"{hubspot_api_base()}/crm/v3/objects/deals"
    api_request = urllib.request.Request(
        url,
        data=encoded,
        method="POST",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(api_request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
            payload = json.loads(response_body) if response_body else {}
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        try:
            error_payload = json.loads(error_body) if error_body else {}
        except json.JSONDecodeError:
            error_payload = {"body": error_body}
        message = str(error_payload.get("message") or error_payload.get("error") or "HubSpot deal creation failed.")
        raise HubSpotSalesError(message, status_code=502, payload=error_payload) from exc
    except urllib.error.URLError as exc:
        raise HubSpotSalesError(f"Could not reach HubSpot: {exc.reason}", status_code=502) from exc
    deal_id = str(payload.get("id", "")).strip()
    return {
        "id": deal_id,
        "properties": payload.get("properties", {}),
        "created_at": payload.get("createdAt", ""),
        "updated_at": payload.get("updatedAt", ""),
        "archived": payload.get("archived", False),
        "hubspot_url": hubspot_deal_url(deal_id),
        "raw": payload,
    }
