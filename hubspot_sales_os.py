#!/usr/bin/env python3
"""HubSpot-backed sales operator runtime for agent.anatainc.com."""

from __future__ import annotations

import json
import os
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import hubspot_sales

try:
    import certifi
except ImportError:  # pragma: no cover - optional dependency
    certifi = None


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_PIPELINE_MODEL_PATH = ROOT_DIR / "config" / "hubspot_sales_pipeline.json"
DEFAULT_OBJECT_MODEL_PATH = ROOT_DIR / "config" / "hubspot_sales_object_model.json"
DEFAULT_AUTONOMY_PATH = ROOT_DIR / "config" / "hubspot_agent_autonomy.json"
DEFAULT_API_BASE = "https://api.hubapi.com"
RATE_LIMIT_RETRY_DELAYS = (0.4, 0.9, 1.5)
SNAPSHOT_TTL_SECONDS = 30
DEAL_PROPERTY_NAMES = [
    "dealname",
    "pipeline",
    "dealstage",
    "amount",
    "hubspot_owner_id",
    "service_type",
    "agency",
    "fulfillment",
    "shipping_os",
    "hs_next_step",
    "hs_lastmodifieddate",
    "createdate",
]
NOTE_TO_DEAL_ASSOCIATION_TYPE_ID = 214
TASK_TO_DEAL_ASSOCIATION_TYPE_ID = 216
HIGH_CONFIDENCE_DEFAULT = 0.85
MEDIUM_CONFIDENCE_DEFAULT = 0.65
OUTBOUND_EMAIL_DEFAULT = False
LINKED_DEAL_CREATION_DEFAULT = True
DEFAULT_DUPLICATE_SEND_WINDOW_MINUTES = 240
PRIMARY_PIPELINE_ENV_NAMES = ("HUBSPOT_PRIMARY_PIPELINE_ID", "HUBSPOT_DEFAULT_DEAL_PIPELINE")

_cached_snapshot: Optional[Dict[str, Any]] = None
_cached_snapshot_expires_at = 0.0


def _env_value(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_flag(name: str, default: bool = False) -> bool:
    value = _env_value(name)
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = _env_value(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = _env_value(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _pipeline_id() -> str:
    for name in PRIMARY_PIPELINE_ENV_NAMES:
        value = _env_value(name)
        if value:
            return value
    return ""


def _portal_id() -> str:
    return _env_value("HUBSPOT_PORTAL_ID")


def _hubspot_api_base() -> str:
    return _env_value("HUBSPOT_API_BASE_URL").rstrip("/") or DEFAULT_API_BASE


def _high_confidence_threshold() -> float:
    return _env_float("ANATA_AGENT_HIGH_CONFIDENCE_THRESHOLD", HIGH_CONFIDENCE_DEFAULT)


def _medium_confidence_threshold() -> float:
    return _env_float("ANATA_AGENT_MEDIUM_CONFIDENCE_THRESHOLD", MEDIUM_CONFIDENCE_DEFAULT)


def _duplicate_send_window_minutes() -> int:
    return _env_int("ANATA_AGENT_DUPLICATE_SEND_WINDOW_MINUTES", DEFAULT_DUPLICATE_SEND_WINDOW_MINUTES)


def _outbound_email_enabled() -> bool:
    return _env_flag("ANATA_AGENT_OUTBOUND_EMAIL_ENABLED", OUTBOUND_EMAIL_DEFAULT)


def _linked_deal_creation_enabled() -> bool:
    return _env_flag("ANATA_AGENT_LINKED_DEAL_CREATION_ENABLED", LINKED_DEAL_CREATION_DEFAULT)


def _hubspot_request(path: str, *, method: str = "GET", payload: Optional[Mapping[str, Any]] = None, attempt: int = 0) -> Any:
    access_token = hubspot_sales.hubspot_access_token()
    if not access_token:
        raise hubspot_sales.HubSpotSalesError(
            "HubSpot token is not configured. Set HUBSPOT_PRIVATE_APP_TOKEN or HUBSPOT_ACCESS_TOKEN.",
            status_code=503,
        )
    encoded_body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        f"{_hubspot_api_base()}{path}",
        data=encoded_body,
        method=method,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if payload is not None else {}),
        },
    )
    try:
        ssl_context = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()
        with urllib.request.urlopen(request, timeout=30, context=ssl_context) as response:
            response_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        try:
            error_payload = json.loads(error_body) if error_body else {}
        except json.JSONDecodeError:
            error_payload = {"body": error_body}
        if exc.code == 429 and attempt < len(RATE_LIMIT_RETRY_DELAYS):
            retry_after = exc.headers.get("retry-after", "").strip()
            delay = RATE_LIMIT_RETRY_DELAYS[attempt]
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = RATE_LIMIT_RETRY_DELAYS[attempt]
            time.sleep(delay)
            return _hubspot_request(path, method=method, payload=payload, attempt=attempt + 1)
        message = str(error_payload.get("message") or error_payload.get("error") or f"HubSpot request failed for {path}.")
        raise hubspot_sales.HubSpotSalesError(message, status_code=502, payload=error_payload) from exc
    except urllib.error.URLError as exc:
        raise hubspot_sales.HubSpotSalesError(f"Could not reach HubSpot: {exc.reason}", status_code=502) from exc
    if not response_body:
        return {}
    try:
        return json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise hubspot_sales.HubSpotSalesError(f"HubSpot returned invalid JSON for {path}.", status_code=502) from exc


def _post_json(path: str, payload: Mapping[str, Any]) -> Any:
    return _hubspot_request(path, method="POST", payload=payload)


def _patch_json(path: str, payload: Mapping[str, Any]) -> Any:
    return _hubspot_request(path, method="PATCH", payload=payload)


def _read_json_config(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise hubspot_sales.HubSpotSalesError(f"Sales runtime config not found: {path}", status_code=500) from exc
    except json.JSONDecodeError as exc:
        raise hubspot_sales.HubSpotSalesError(f"Sales runtime config is not valid JSON: {path}", status_code=500) from exc


def load_pipeline_model() -> Dict[str, Any]:
    return _read_json_config(DEFAULT_PIPELINE_MODEL_PATH)


def load_object_model() -> Dict[str, Any]:
    return _read_json_config(DEFAULT_OBJECT_MODEL_PATH)


def load_autonomy_model() -> Dict[str, Any]:
    return _read_json_config(DEFAULT_AUTONOMY_PATH)


def normalize(value: str) -> str:
    return "".join(char.lower() if char.isalnum() else " " for char in value).strip()


def parse_multi_value(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(";") if item.strip()]


def _count_keyword_matches(haystack: str, keywords: List[str]) -> int:
    return sum(1 for keyword in keywords if keyword in haystack)


def _clamp(value: float, minimum: float = 0.0, maximum: float = 0.99) -> float:
    return max(minimum, min(maximum, value))


def _offer_to_deal_service_type(offer_id: str) -> Optional[str]:
    if offer_id == "amazon_marketing_service":
        return "Amazon"
    if offer_id == "fulfillment":
        return "Fulfillment"
    if offer_id in {"shipping_os", "anata_intelligence"}:
        return "Software"
    return None


def _offer_labels() -> Dict[str, str]:
    return {
        "amazon_marketing_service": "Amazon Marketing Service",
        "fulfillment": "Fulfillment",
        "shipping_os": "Shipping OS",
        "anata_intelligence": "Anata Intelligence",
        "unknown": "Unclassified",
    }


def _overlay_labels() -> Dict[str, str]:
    return {
        "amazon_marketing_service": "Anata Intelligence",
        "fulfillment": "Shipping OS",
        "shipping_os": "Shipping OS",
        "anata_intelligence": "Anata Intelligence",
    }


def _stage_probability(stage: Mapping[str, Any]) -> float:
    metadata = stage.get("metadata", {})
    raw_probability = ""
    if isinstance(metadata, Mapping):
        raw_probability = str(metadata.get("probability", "") or "")
    try:
        return float(raw_probability)
    except ValueError:
        return 0.0


def get_stage_status(stage: Mapping[str, Any]) -> str:
    label = normalize(str(stage.get("label", "")))
    probability = _stage_probability(stage)
    if "nurture" in label or "follow up" in label:
        return "nurture"
    if "lost" in label or probability == 0.0:
        return "lost"
    if "won" in label or probability == 1.0:
        return "won"
    return "open"


def infer_offer(deal: Mapping[str, Any], company: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    labels = _offer_labels()
    overlays = _overlay_labels()
    signal_counts: Dict[str, float] = {}
    reasons_by_offer: Dict[str, List[str]] = {}
    deal_props = deal.get("properties", {}) if isinstance(deal.get("properties"), Mapping) else {}
    company_props = company.get("properties", {}) if isinstance(company and company.get("properties"), Mapping) else {}
    service_type_values = parse_multi_value(str(deal_props.get("service_type", "") or ""))
    company_service_type_values = parse_multi_value(str(company_props.get("service_type", "") or ""))
    name_haystack = normalize(" ".join(filter(None, [str(deal_props.get("dealname", "") or ""), str(company_props.get("name", "") or "")])))
    fulfillment_keywords = ["fulfillment", "3pl", "warehouse", "ship"]
    amazon_keywords = ["amazon", "marketing", "ads", "advertising"]
    shipping_os_keywords = ["shipping os", "shippingos"]
    intelligence_keywords = ["saas", "software", "intelligence"]

    def add_signal(offer_id: str, score: float, reason: str) -> None:
        signal_counts[offer_id] = signal_counts.get(offer_id, 0.0) + score
        reasons_by_offer.setdefault(offer_id, []).append(reason)

    for value in service_type_values:
        normalized_value = normalize(value)
        if normalized_value == "amazon":
            add_signal("amazon_marketing_service", 0.42, "deal service_type already signals Amazon")
        if normalized_value == "fulfillment":
            add_signal("fulfillment", 0.42, "deal service_type already signals Fulfillment")
        if normalized_value == "software":
            shipping_field = str(deal_props.get("shipping_os", "") or "").strip()
            if shipping_field:
                add_signal("shipping_os", 0.48, "deal service_type plus shipping_os signals Shipping OS")
            else:
                add_signal("anata_intelligence", 0.22, "deal service_type signals Software")

    for value in company_service_type_values:
        normalized_value = normalize(value)
        if normalized_value == "amazon":
            add_signal("amazon_marketing_service", 0.22, "company service_type signals Amazon")
        if normalized_value == "fulfillment":
            add_signal("fulfillment", 0.22, "company service_type signals Fulfillment")
        if normalized_value == "software":
            shipping_field = str(deal_props.get("shipping_os", "") or "").strip()
            if shipping_field:
                add_signal("shipping_os", 0.24, "company service_type plus shipping_os signals Shipping OS")
            else:
                add_signal("anata_intelligence", 0.12, "company service_type signals Software")

    if str(deal_props.get("agency", "") or "").strip():
        add_signal("amazon_marketing_service", 0.98, "agency progress field is populated")
    if str(deal_props.get("fulfillment", "") or "").strip():
        add_signal("fulfillment", 0.98, "fulfillment progress field is populated")
    if str(deal_props.get("shipping_os", "") or "").strip():
        add_signal("shipping_os", 0.99, "shipping_os progress field is populated")

    fulfillment_matches = _count_keyword_matches(name_haystack, fulfillment_keywords)
    amazon_matches = _count_keyword_matches(name_haystack, amazon_keywords)
    shipping_matches = _count_keyword_matches(name_haystack, shipping_os_keywords)
    intelligence_matches = _count_keyword_matches(name_haystack, intelligence_keywords)

    if fulfillment_matches >= 2:
        add_signal("fulfillment", 0.93, "deal and company naming strongly imply Fulfillment")
    elif fulfillment_matches == 1:
        add_signal("fulfillment", 0.63, "deal naming weakly implies Fulfillment")

    if amazon_matches >= 2:
        add_signal("amazon_marketing_service", 0.91, "deal and company naming strongly imply Amazon Marketing Service")
    elif amazon_matches == 1:
        add_signal("amazon_marketing_service", 0.58, "deal naming weakly implies Amazon Marketing Service")

    if shipping_matches >= 1:
        add_signal("shipping_os", 0.94, "deal naming explicitly references Shipping OS")

    if intelligence_matches >= 1 and shipping_matches == 0:
        add_signal("anata_intelligence", 0.55, "deal naming implies a software or intelligence offer")

    ranked = sorted(signal_counts.items(), key=lambda item: item[1], reverse=True)
    if not ranked:
        return {
            "primary_offer": "unknown",
            "primary_offer_label": labels["unknown"],
            "overlay": None,
            "signal_count": 0,
            "confidence": 0.0,
            "reasons": ["no deterministic service signals were found"],
            "deal_service_type_value": None,
        }

    primary_offer, primary_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    ambiguity_penalty = 0.14 if primary_score - second_score < 0.18 else 0.0
    confidence = _clamp(primary_score - ambiguity_penalty)
    signal_count = sum(1 for _, score in ranked if score >= 0.25)
    return {
        "primary_offer": primary_offer,
        "primary_offer_label": labels.get(primary_offer, labels["unknown"]),
        "overlay": overlays.get(primary_offer),
        "signal_count": signal_count,
        "confidence": confidence,
        "reasons": reasons_by_offer.get(primary_offer, []),
        "deal_service_type_value": _offer_to_deal_service_type(primary_offer),
    }


def build_suggested_next_step(stage: Optional[Mapping[str, Any]], inference: Mapping[str, Any]) -> Dict[str, Any]:
    offer_text = "opportunity"
    primary_offer = str(inference.get("primary_offer", "unknown"))
    label = str(inference.get("primary_offer_label", "") or "")
    if primary_offer != "unknown" and label:
        offer_text = label.lower()
    normalized_label = normalize(str(stage.get("label", ""))) if stage else ""
    if "new lead" in normalized_label:
        return {
            "text": f"Make the first contact on this {offer_text}, confirm it is a real lead, and capture the opening context.",
            "confidence": 0.95,
            "reasons": ["stage is New Lead", "first contact is deterministic"],
        }
    if "contacted" in normalized_label:
        return {
            "text": f"Confirm the response details for this {offer_text} lead and decide whether it should move into qualification.",
            "confidence": 0.95,
            "reasons": ["stage is Contacted", "qualification decision is the next action"],
        }
    if "qualified" in normalized_label:
        return {
            "text": f"Confirm requirements for the {offer_text}, fill the qualification gaps, and prepare the proposal path.",
            "confidence": 0.96,
            "reasons": ["stage is Qualified", "next step template is deterministic"],
        }
    if "audit or deck in progress" in normalized_label:
        return {
            "text": f"Finish the {offer_text} proposal deck and audit so scope, pricing, and recommendations are ready.",
            "confidence": 0.96,
            "reasons": ["stage is Audit Or Deck In Progress", "proposal artifact creation is active work"],
        }
    if "proposal ready" in normalized_label:
        return {
            "text": f"Review the {offer_text} proposal internally, finalize the send package, and confirm it is ready to send.",
            "confidence": 0.95,
            "reasons": ["stage is Proposal Ready", "proposal should be finalized before send"],
        }
    if "proposal sent" in normalized_label or "offered" in normalized_label:
        return {
            "text": f"Follow up on the sent {offer_text} proposal and confirm questions, objections, and timeline.",
            "confidence": 0.95,
            "reasons": ["stage is Proposal Sent", "post-send follow-up is deterministic"],
        }
    if "negotiation" in normalized_label or "negotiating" in normalized_label:
        return {
            "text": f"Resolve the open negotiation points on the {offer_text} proposal and confirm the path to close.",
            "confidence": 0.95,
            "reasons": ["stage is Negotiation", "deal is in active negotiation"],
        }
    if "nurture" in normalized_label or "follow up" in normalized_label:
        return {
            "text": f"Send the next follow-up on this {offer_text} and confirm whether the deal stays active or remains in nurture.",
            "confidence": 0.94,
            "reasons": ["stage is Nurture", "deal explicitly needs follow-up"],
        }
    return {
        "text": f"Review this {offer_text} and define the next commercial action.",
        "confidence": 0.90,
        "reasons": ["fallback next-step template"],
    }


def _to_number(value: Optional[str]) -> Optional[float]:
    if value in {None, ""}:
        return None
    try:
        return float(str(value))
    except ValueError:
        return None


def _format_owner(owner: Mapping[str, Any]) -> str:
    full_name = " ".join(part for part in [str(owner.get("firstName", "") or "").strip(), str(owner.get("lastName", "") or "").strip()] if part).strip()
    return full_name or str(owner.get("email", "") or "").strip() or f"Owner {owner.get('id', '')}"


def _format_company(company: Optional[Mapping[str, Any]]) -> str:
    if not company:
        return "No company"
    properties = company.get("properties", {}) if isinstance(company.get("properties"), Mapping) else {}
    return str(properties.get("name", "") or "").strip() or "No company"


def _format_contact(contact: Optional[Mapping[str, Any]]) -> str:
    if not contact:
        return "No contact"
    properties = contact.get("properties", {}) if isinstance(contact.get("properties"), Mapping) else {}
    full_name = " ".join(
        part
        for part in [str(properties.get("firstname", "") or "").strip(), str(properties.get("lastname", "") or "").strip()]
        if part
    ).strip()
    return full_name or str(properties.get("email", "") or "").strip() or "Unnamed contact"


def _deal_url(deal_id: str) -> Optional[str]:
    portal_id = _portal_id()
    if not portal_id or not deal_id:
        return None
    return f"https://app.hubspot.com/contacts/{portal_id}/record/0-3/{deal_id}"


def _select_primary_association(associations: List[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    for association in associations:
        types = association.get("associationTypes", [])
        if isinstance(types, list):
            for association_type in types:
                if isinstance(association_type, Mapping) and association_type.get("label") == "Primary":
                    return association
    return associations[0] if associations else None


def get_primary_pipeline() -> Dict[str, Any]:
    pipeline_id = _pipeline_id()
    if not pipeline_id:
        raise hubspot_sales.HubSpotSalesError(
            "HubSpot primary pipeline is not configured. Set HUBSPOT_PRIMARY_PIPELINE_ID or HUBSPOT_DEFAULT_DEAL_PIPELINE.",
            status_code=503,
        )
    return _hubspot_request(f"/crm/v3/pipelines/deals/{pipeline_id}")


def get_properties(object_type: str) -> List[Dict[str, Any]]:
    payload = _hubspot_request(f"/crm/v3/properties/{object_type}")
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def get_owners() -> List[Dict[str, Any]]:
    payload = _hubspot_request("/crm/v3/owners?limit=500&archived=false")
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def get_association_labels(from_object: str, to_object: str) -> List[Dict[str, Any]]:
    payload = _hubspot_request(f"/crm/v4/associations/{from_object}/{to_object}/labels")
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def search_all_deals() -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    after: Optional[str] = None
    while True:
        body: Dict[str, Any] = {
            "limit": 100,
            "properties": DEAL_PROPERTY_NAMES,
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
        }
        if after:
            body["after"] = after
        payload = _post_json("/crm/v3/objects/deals/search", body)
        page_results = payload.get("results", []) if isinstance(payload, Mapping) else []
        results.extend(item for item in page_results if isinstance(item, Mapping))
        paging = payload.get("paging", {}) if isinstance(payload, Mapping) else {}
        next_page = paging.get("next", {}) if isinstance(paging, Mapping) else {}
        next_after = str(next_page.get("after", "") or "").strip()
        if not next_after:
            break
        after = next_after
    return results


def search_recent_deals(limit: int = 100) -> List[Dict[str, Any]]:
    payload = _post_json(
        "/crm/v3/objects/deals/search",
        {
            "limit": max(1, min(limit, 100)),
            "properties": DEAL_PROPERTY_NAMES,
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
        },
    )
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def batch_read_deals(deal_ids: List[str]) -> List[Dict[str, Any]]:
    if not deal_ids:
        return []
    payload = _post_json(
        "/crm/v3/objects/deals/batch/read",
        {"properties": DEAL_PROPERTY_NAMES, "inputs": [{"id": deal_id} for deal_id in deal_ids]},
    )
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def batch_read_associations(deal_ids: List[str], to_object_type: str) -> List[Dict[str, Any]]:
    if not deal_ids:
        return []
    payload = _post_json(
        f"/crm/v4/associations/deals/{to_object_type}/batch/read",
        {"inputs": [{"id": deal_id} for deal_id in deal_ids]},
    )
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def batch_read_companies(company_ids: List[str]) -> List[Dict[str, Any]]:
    if not company_ids:
        return []
    payload = _post_json(
        "/crm/v3/objects/companies/batch/read",
        {"properties": ["name", "service_type"], "inputs": [{"id": company_id} for company_id in company_ids]},
    )
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def batch_read_contacts(contact_ids: List[str]) -> List[Dict[str, Any]]:
    if not contact_ids:
        return []
    payload = _post_json(
        "/crm/v3/objects/contacts/batch/read",
        {"properties": ["firstname", "lastname", "email"], "inputs": [{"id": contact_id} for contact_id in contact_ids]},
    )
    results = payload.get("results", []) if isinstance(payload, Mapping) else []
    return [item for item in results if isinstance(item, Mapping)]


def update_deal(deal_id: str, properties: Mapping[str, Any]) -> Dict[str, Any]:
    return _patch_json(f"/crm/v3/objects/deals/{deal_id}", {"properties": dict(properties)})


def create_hubspot_note(*, body: str, associated_deal_id: str, owner_id: Optional[str]) -> Dict[str, Any]:
    return _post_json(
        "/crm/v3/objects/notes",
        {
            "properties": {
                "hs_timestamp": datetime.now(timezone.utc).isoformat(),
                "hs_note_body": body,
                **({"hubspot_owner_id": owner_id} if owner_id else {}),
            },
            "associations": [
                {
                    "to": {"id": associated_deal_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": NOTE_TO_DEAL_ASSOCIATION_TYPE_ID}],
                }
            ],
        },
    )


def create_hubspot_task(*, associated_deal_id: str, owner_id: Optional[str], subject: str, body: str, due_at: str) -> Dict[str, Any]:
    return _post_json(
        "/crm/v3/objects/tasks",
        {
            "properties": {
                "hs_task_subject": subject,
                "hs_task_body": body,
                "hs_timestamp": due_at,
                "hs_task_status": "NOT_STARTED",
                "hs_task_priority": "HIGH",
                "hs_task_type": "TODO",
                **({"hubspot_owner_id": owner_id} if owner_id else {}),
            },
            "associations": [
                {
                    "to": {"id": associated_deal_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": TASK_TO_DEAL_ASSOCIATION_TYPE_ID}],
                }
            ],
        },
    )


def _map_by_id(records: List[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(record.get("id", "")): dict(record) for record in records if str(record.get("id", "")).strip()}


def _summarize_properties(properties: List[Mapping[str, Any]]) -> Dict[str, Any]:
    custom = [item for item in properties if not item.get("hubspotDefined")]
    summarized = []
    for item in custom:
        options = item.get("options", [])
        option_labels = []
        if isinstance(options, list):
            option_labels = [str(option.get("label", "") or "") for option in options if isinstance(option, Mapping) and not option.get("hidden")]
        summarized.append(
            {
                "name": str(item.get("name", "") or ""),
                "label": str(item.get("label", "") or ""),
                "type": str(item.get("type", "") or ""),
                "fieldType": str(item.get("fieldType", "") or ""),
                "optionLabels": option_labels,
            }
        )
    summarized.sort(key=lambda item: item["label"])
    return {
        "totalCount": len(properties),
        "customCount": len(custom),
        "customProperties": summarized,
    }


def _snapshot_deals_for_recent_context(recent_deals: List[Mapping[str, Any]]) -> Dict[str, Any]:
    recent_ids = [str(deal.get("id", "") or "") for deal in recent_deals if str(deal.get("id", "") or "").strip()]
    company_associations = batch_read_associations(recent_ids, "companies")
    contact_associations = batch_read_associations(recent_ids, "contacts")
    company_ids = sorted(
        {
            str(entry.get("toObjectId", "") or "")
            for association in company_associations
            for entry in (association.get("to", []) if isinstance(association.get("to"), list) else [])
            if isinstance(entry, Mapping) and str(entry.get("toObjectId", "") or "").strip()
        }
    )
    contact_ids = sorted(
        {
            str(entry.get("toObjectId", "") or "")
            for association in contact_associations
            for entry in (association.get("to", []) if isinstance(association.get("to"), list) else [])
            if isinstance(entry, Mapping) and str(entry.get("toObjectId", "") or "").strip()
        }
    )
    companies = batch_read_companies(company_ids)
    contacts = batch_read_contacts(contact_ids)
    return {
        "company_associations": company_associations,
        "contact_associations": contact_associations,
        "company_map": _map_by_id(companies),
        "contact_map": _map_by_id(contacts),
    }


def build_sales_dashboard_snapshot() -> Dict[str, Any]:
    live_pipeline = get_primary_pipeline()
    owners = get_owners()
    properties = {
        "deals": get_properties("deals"),
        "companies": get_properties("companies"),
        "contacts": get_properties("contacts"),
    }
    deal_to_company_labels = get_association_labels("deal", "company")
    deal_to_contact_labels = get_association_labels("deal", "contact")
    deals = search_all_deals()
    pipeline_stages = {
        str(stage.get("id", "") or ""): stage
        for stage in live_pipeline.get("stages", [])
        if isinstance(stage, Mapping)
    }
    owner_map = {str(owner.get("id", "")): _format_owner(owner) for owner in owners}
    recent_deals = deals[:12]
    recent_context = _snapshot_deals_for_recent_context(recent_deals)
    company_association_map = {
        str(item.get("from", {}).get("id", "") or ""): item.get("to", [])
        for item in recent_context["company_associations"]
        if isinstance(item.get("from"), Mapping)
    }
    contact_association_map = {
        str(item.get("from", {}).get("id", "") or ""): item.get("to", [])
        for item in recent_context["contact_associations"]
        if isinstance(item.get("from"), Mapping)
    }
    stage_summaries: Dict[str, Dict[str, Any]] = {}
    for stage in live_pipeline.get("stages", []):
        if not isinstance(stage, Mapping):
            continue
        stage_id = str(stage.get("id", "") or "")
        stage_summaries[stage_id] = {
            "id": stage_id,
            "label": str(stage.get("label", "") or ""),
            "probability": _stage_probability(stage),
            "status": get_stage_status(stage),
            "dealCount": 0,
            "totalAmount": 0.0,
            "needsAttentionCount": 0,
        }
    open_deals = 0
    won_deals = 0
    lost_deals = 0
    nurture_deals = 0
    open_amount = 0.0
    unclassified_deals = 0
    deals_missing_amount = 0
    deals_missing_owner = 0
    open_deals_missing_next_step = 0
    multi_offer_candidates = 0
    for deal in deals:
        deal_props = deal.get("properties", {}) if isinstance(deal.get("properties"), Mapping) else {}
        stage = pipeline_stages.get(str(deal_props.get("dealstage", "") or ""))
        stage_summary = stage_summaries.get(str(deal_props.get("dealstage", "") or ""))
        amount = _to_number(str(deal_props.get("amount", "") or ""))
        inference = infer_offer(deal, None)
        if inference["primary_offer"] == "unknown":
            unclassified_deals += 1
        if inference["signal_count"] > 1:
            multi_offer_candidates += 1
        if amount is None:
            deals_missing_amount += 1
        if not str(deal_props.get("hubspot_owner_id", "") or "").strip():
            deals_missing_owner += 1
        if not stage or not stage_summary:
            continue
        status = get_stage_status(stage)
        stage_summary["dealCount"] += 1
        stage_summary["totalAmount"] += amount or 0.0
        if status == "open":
            open_deals += 1
            open_amount += amount or 0.0
            if not str(deal_props.get("hs_next_step", "") or "").strip():
                open_deals_missing_next_step += 1
                stage_summary["needsAttentionCount"] += 1
        if status == "won":
            won_deals += 1
        if status == "lost":
            lost_deals += 1
        if status == "nurture":
            nurture_deals += 1
            if not str(deal_props.get("hs_next_step", "") or "").strip():
                stage_summary["needsAttentionCount"] += 1
    recent_summaries = []
    for deal in recent_deals:
        deal_id = str(deal.get("id", "") or "")
        deal_props = deal.get("properties", {}) if isinstance(deal.get("properties"), Mapping) else {}
        stage = pipeline_stages.get(str(deal_props.get("dealstage", "") or ""))
        company_association = _select_primary_association(company_association_map.get(deal_id, []))
        contact_association = _select_primary_association(contact_association_map.get(deal_id, []))
        company = None
        contact = None
        if company_association:
            company = recent_context["company_map"].get(str(company_association.get("toObjectId", "") or ""))
        if contact_association:
            contact = recent_context["contact_map"].get(str(contact_association.get("toObjectId", "") or ""))
        inference = infer_offer(deal, company)
        amount = _to_number(str(deal_props.get("amount", "") or ""))
        stage_status = get_stage_status(stage) if stage else "open"
        missing_fields: List[str] = []
        if inference["primary_offer"] == "unknown":
            missing_fields.append("service classification")
        if amount is None:
            missing_fields.append("amount")
        if not str(deal_props.get("hubspot_owner_id", "") or "").strip():
            missing_fields.append("owner")
        if stage_status in {"open", "nurture"} and not str(deal_props.get("hs_next_step", "") or "").strip():
            missing_fields.append("next step")
        if not company_association:
            missing_fields.append("company link")
        if not contact_association:
            missing_fields.append("contact link")
        recent_summaries.append(
            {
                "id": deal_id,
                "name": str(deal_props.get("dealname", "") or "").strip() or "Unnamed deal",
                "amount": amount,
                "owner": owner_map.get(str(deal_props.get("hubspot_owner_id", "") or ""), "Unassigned"),
                "stage": str(stage.get("label", "") or "") if stage else str(deal_props.get("dealstage", "") or "Unknown stage"),
                "stageStatus": stage_status,
                "company": _format_company(company),
                "contact": _format_contact(contact),
                "primaryOffer": inference["primary_offer_label"],
                "overlay": inference["overlay"],
                "updatedAt": str(deal.get("updatedAt", "") or ""),
                "nextStep": str(deal_props.get("hs_next_step", "") or "").strip() or None,
                "needsFollowUp": stage_status in {"open", "nurture"} and not str(deal_props.get("hs_next_step", "") or "").strip(),
                "missingFields": missing_fields,
                "url": _deal_url(deal_id),
            }
        )
    pipeline_model = load_pipeline_model()
    target_labels = [str(stage.get("label", "") or "") for stage in pipeline_model.get("stages", []) if isinstance(stage, Mapping)]
    live_labels = [str(stage.get("label", "") or "") for stage in live_pipeline.get("stages", []) if isinstance(stage, Mapping)]
    normalized_target = {normalize(label): label for label in target_labels}
    normalized_live = {normalize(label): label for label in live_labels}
    target_only = [label for label in target_labels if normalize(label) not in normalized_live]
    live_only = [label for label in live_labels if normalize(label) not in normalized_target]
    object_model = load_object_model()
    autonomy_model = load_autonomy_model()
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "portalId": _portal_id(),
        "pipeline": {
            "id": str(live_pipeline.get("id", "") or ""),
            "label": str(live_pipeline.get("label", "") or ""),
            "stages": list(stage_summaries.values()),
            "liveStageCount": len(live_labels),
            "targetStageCount": len(target_labels),
        },
        "summary": {
            "totalDeals": len(deals),
            "openDeals": open_deals,
            "wonDeals": won_deals,
            "lostDeals": lost_deals,
            "nurtureDeals": nurture_deals,
            "openAmount": open_amount,
            "unclassifiedDeals": unclassified_deals,
            "dealsMissingAmount": deals_missing_amount,
            "dealsMissingOwner": deals_missing_owner,
            "openDealsMissingNextStep": open_deals_missing_next_step,
            "multiOfferCandidates": multi_offer_candidates,
        },
        "directives": {
            "happening": [
                f"{open_deals} open opportunities are active in the live {live_pipeline.get('label', 'HubSpot')} pipeline.",
                f"{won_deals} won records and {lost_deals} lost records are already shaping current sales history.",
                f"{multi_offer_candidates} deals show multiple offer signals and are candidates for linked commercial records.",
            ],
            "broken": [
                f"{unclassified_deals} deals still lack a confident primary service or software classification.",
                f"{open_deals_missing_next_step} open deals do not have a next-step instruction.",
                f"{deals_missing_owner} deals are unassigned and {deals_missing_amount} deals are missing amount data.",
            ],
            "next": [
                "Normalize live HubSpot stages into the audited shared operating model without losing current pipeline history.",
                "Write service inference back into the deal model only when confidence is high enough to act safely.",
                "Start deck and audit sync once live artifact URLs exist on agent.anatainc.com.",
            ],
        },
        "schema": {
            "owners": [{"id": str(owner.get("id", "") or ""), "name": _format_owner(owner), "email": owner.get("email") or None} for owner in owners],
            "properties": {name: _summarize_properties(entries) for name, entries in properties.items()},
            "associationLabels": {
                "dealToCompany": [str(item.get("label", "") or f"Type {item.get('typeId', '')}") for item in deal_to_company_labels],
                "dealToContact": [str(item.get("label", "") or f"Type {item.get('typeId', '')}") for item in deal_to_contact_labels],
            },
            "confidencePolicy": {
                "highThreshold": _high_confidence_threshold(),
                "mediumThreshold": _medium_confidence_threshold(),
                "duplicateSendWindowMinutes": _duplicate_send_window_minutes(),
                "outboundEmailEnabled": _outbound_email_enabled(),
                "linkedDealCreationEnabled": _linked_deal_creation_enabled(),
            },
        },
        "objectDefinitions": object_model.get("objects", {}),
        "offers": object_model.get("classification", {}),
        "autonomy": autonomy_model,
        "stageDrift": {"targetOnly": target_only, "liveOnly": live_only},
        "recentDeals": recent_summaries,
    }


def invalidate_sales_dashboard_snapshot() -> None:
    global _cached_snapshot, _cached_snapshot_expires_at
    _cached_snapshot = None
    _cached_snapshot_expires_at = 0.0


def get_sales_dashboard_snapshot(*, force_refresh: bool = False) -> Dict[str, Any]:
    global _cached_snapshot, _cached_snapshot_expires_at
    if not force_refresh and _cached_snapshot and _cached_snapshot_expires_at > time.time():
        return _cached_snapshot
    snapshot = build_sales_dashboard_snapshot()
    _cached_snapshot = snapshot
    _cached_snapshot_expires_at = time.time() + SNAPSHOT_TTL_SECONDS
    return snapshot


def _due_date_for_stage_status(stage_status: str) -> str:
    days = 2 if stage_status == "nurture" else 1
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


def _build_reasoning_note_html(*, header: str, deal_id: str, actions: List[Mapping[str, Any]], reasons: List[str]) -> str:
    deal_link = _deal_url(deal_id) or deal_id
    action_lines = "".join(f"<li><strong>{action.get('type', '')}</strong>: {action.get('reason', '')}</li>" for action in actions)
    reason_lines = "".join(f"<li>{reason}</li>" for reason in reasons)
    return "".join(
        [
            f"<p><strong>{header}</strong></p>",
            f"<p>Deal: <a href=\"{deal_link}\">{deal_id}</a></p>",
            "<p>Actions:</p>",
            f"<ul>{action_lines or '<li>No direct actions were applied.</li>'}</ul>",
            "<p>Signals:</p>",
            f"<ul>{reason_lines or '<li>No reasoning signals recorded.</li>'}</ul>",
        ]
    )


def run_writeback(*, mode: str = "preview", deal_ids: Optional[List[str]] = None, limit: int = 10) -> Dict[str, Any]:
    normalized_mode = "apply" if mode == "apply" else "preview"
    high_threshold = _high_confidence_threshold()
    medium_threshold = _medium_confidence_threshold()
    pipeline = get_primary_pipeline()
    pipeline_stages = {
        str(stage.get("id", "") or ""): stage
        for stage in pipeline.get("stages", [])
        if isinstance(stage, Mapping)
    }
    if deal_ids:
        deals = batch_read_deals([deal_id for deal_id in deal_ids if deal_id.strip()])
    else:
        candidate_deals = search_recent_deals(100)
        filtered: List[Dict[str, Any]] = []
        for deal in candidate_deals:
            deal_props = deal.get("properties", {}) if isinstance(deal.get("properties"), Mapping) else {}
            stage = pipeline_stages.get(str(deal_props.get("dealstage", "") or ""))
            if not stage:
                continue
            stage_status = get_stage_status(stage)
            needs_classification = not str(deal_props.get("service_type", "") or "").strip()
            needs_next_step = stage_status in {"open", "nurture"} and not str(deal_props.get("hs_next_step", "") or "").strip()
            if needs_classification or needs_next_step:
                filtered.append(deal)
        deals = filtered[: max(1, min(limit, 25))]
    company_associations = batch_read_associations([str(deal.get("id", "") or "") for deal in deals], "companies")
    primary_company_ids = []
    company_association_map: Dict[str, Optional[Mapping[str, Any]]] = {}
    for association in company_associations:
        from_id = str(association.get("from", {}).get("id", "") or "")
        entries = association.get("to", []) if isinstance(association.get("to"), list) else []
        primary = entries[0] if entries else None
        company_association_map[from_id] = primary
        if isinstance(primary, Mapping):
            company_id = str(primary.get("toObjectId", "") or "")
            if company_id:
                primary_company_ids.append(company_id)
    company_map = _map_by_id(batch_read_companies(sorted(set(primary_company_ids))))
    applied_actions = 0
    deferred_actions = 0
    note_count = 0
    task_count = 0
    results = []
    for deal in deals:
        deal_id = str(deal.get("id", "") or "")
        deal_props = deal.get("properties", {}) if isinstance(deal.get("properties"), Mapping) else {}
        stage = pipeline_stages.get(str(deal_props.get("dealstage", "") or ""))
        stage_status = get_stage_status(stage) if stage else "open"
        company_association = company_association_map.get(deal_id)
        company = company_map.get(str(company_association.get("toObjectId", "") or "")) if isinstance(company_association, Mapping) else None
        inference = infer_offer(deal, company)
        next_step = build_suggested_next_step(stage, inference)
        actions: List[Dict[str, Any]] = []
        high_confidence_actions: List[Dict[str, Any]] = []
        medium_reasons: List[str] = []
        low_reasons: List[str] = []
        if not str(deal_props.get("service_type", "") or "").strip() and inference.get("deal_service_type_value"):
            if float(inference.get("confidence", 0.0)) >= high_threshold:
                high_confidence_actions.append(
                    {
                        "type": "update_deal_service_type",
                        "payload": {"service_type": str(inference["deal_service_type_value"])},
                        "confidence": float(inference.get("confidence", 0.0)),
                        "reason": f"set deal service_type to {inference['deal_service_type_value']}",
                    }
                )
            elif float(inference.get("confidence", 0.0)) >= medium_threshold:
                medium_reasons.append(
                    f"deal service_type likely should be {inference['deal_service_type_value']} but confidence is only {round(float(inference.get('confidence', 0.0)) * 100)}%"
                )
            else:
                low_reasons.append(f"service classification is still uncertain ({round(float(inference.get('confidence', 0.0)) * 100)}% confidence)")
        if stage_status in {"open", "nurture"} and not str(deal_props.get("hs_next_step", "") or "").strip():
            if float(next_step.get("confidence", 0.0)) >= high_threshold:
                high_confidence_actions.append(
                    {
                        "type": "update_next_step",
                        "payload": {"hs_next_step": str(next_step.get("text", "") or "")},
                        "confidence": float(next_step.get("confidence", 0.0)),
                        "reason": "set deterministic next step from current stage",
                    }
                )
            elif float(next_step.get("confidence", 0.0)) >= medium_threshold:
                medium_reasons.append(
                    f"next step suggestion exists but confidence is only {round(float(next_step.get('confidence', 0.0)) * 100)}%"
                )
            else:
                low_reasons.append("next step could not be set with enough confidence")
        if not high_confidence_actions and not medium_reasons and not low_reasons:
            continue
        if normalized_mode == "apply" and high_confidence_actions:
            merged_update: Dict[str, Any] = {}
            for action in high_confidence_actions:
                merged_update.update(action.get("payload", {}))
            update_deal(deal_id, merged_update)
            for action in high_confidence_actions:
                actions.append(
                    {
                        "type": action["type"],
                        "status": "applied",
                        "confidence": action["confidence"],
                        "reason": action["reason"],
                        "payload": action["payload"],
                    }
                )
            applied_actions += len(high_confidence_actions)
            create_hubspot_note(
                body=_build_reasoning_note_html(
                    header="Anata agent applied high-confidence deal updates.",
                    deal_id=deal_id,
                    actions=actions,
                    reasons=[*list(inference.get("reasons", [])), *list(next_step.get("reasons", []))],
                ),
                associated_deal_id=deal_id,
                owner_id=str(deal_props.get("hubspot_owner_id", "") or "").strip() or None,
            )
            note_count += 1
            actions.append(
                {
                    "type": "create_internal_note",
                    "status": "applied",
                    "confidence": 1.0,
                    "reason": "logged reasoning note for applied write-back actions",
                }
            )
            applied_actions += 1
        else:
            for action in high_confidence_actions:
                actions.append(
                    {
                        "type": action["type"],
                        "status": "preview",
                        "confidence": action["confidence"],
                        "reason": action["reason"],
                        "payload": action["payload"],
                    }
                )
        if normalized_mode == "apply" and (medium_reasons or low_reasons):
            create_hubspot_note(
                body=_build_reasoning_note_html(
                    header="Anata agent deferred write-back actions.",
                    deal_id=deal_id,
                    actions=[],
                    reasons=[*medium_reasons, *low_reasons, *list(inference.get("reasons", []))],
                ),
                associated_deal_id=deal_id,
                owner_id=str(deal_props.get("hubspot_owner_id", "") or "").strip() or None,
            )
            note_count += 1
            actions.append(
                {
                    "type": "create_internal_note",
                    "status": "applied",
                    "confidence": 1.0,
                    "reason": "logged deferred write-back reasoning on the deal record",
                }
            )
            applied_actions += 1
            deferred_actions += len(medium_reasons) + len(low_reasons)
            if medium_reasons:
                create_hubspot_task(
                    associated_deal_id=deal_id,
                    owner_id=str(deal_props.get("hubspot_owner_id", "") or "").strip() or None,
                    subject=f"Review deferred sales write-back for {str(deal_props.get('dealname', '') or deal_id).strip() or deal_id}",
                    body="\n".join(
                        [
                            "The Anata agent found candidate updates that were below the high-confidence threshold.",
                            *[f"- {reason}" for reason in medium_reasons],
                        ]
                    ),
                    due_at=_due_date_for_stage_status(stage_status),
                )
                task_count += 1
                actions.append(
                    {
                        "type": "create_follow_up_task",
                        "status": "applied",
                        "confidence": medium_threshold,
                        "reason": "created internal review task for medium-confidence actions",
                    }
                )
                applied_actions += 1
        else:
            for reason in medium_reasons:
                actions.append(
                    {
                        "type": "create_internal_note",
                        "status": "deferred",
                        "confidence": medium_threshold,
                        "reason": reason,
                    }
                )
            for reason in low_reasons:
                actions.append(
                    {
                        "type": "create_internal_note",
                        "status": "deferred",
                        "confidence": 0.0,
                        "reason": reason,
                    }
                )
            deferred_actions += len(medium_reasons) + len(low_reasons)
        results.append(
            {
                "dealId": deal_id,
                "dealName": str(deal_props.get("dealname", "") or "").strip() or "Unnamed deal",
                "companyName": _format_company(company),
                "stage": str(stage.get("label", "") or "") if stage else str(deal_props.get("dealstage", "") or "Unknown stage"),
                "stageStatus": stage_status,
                "current": {
                    "serviceType": str(deal_props.get("service_type", "") or "").strip() or None,
                    "nextStep": str(deal_props.get("hs_next_step", "") or "").strip() or None,
                },
                "inference": {
                    "primaryOffer": inference.get("primary_offer_label"),
                    "confidence": inference.get("confidence", 0.0),
                    "reasons": inference.get("reasons", []),
                    "targetDealServiceType": inference.get("deal_service_type_value"),
                },
                "actions": actions,
            }
        )
    if normalized_mode == "apply":
        invalidate_sales_dashboard_snapshot()
    return {
        "mode": normalized_mode,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "evaluatedDeals": len(deals),
            "candidateDeals": len(results),
            "appliedActions": applied_actions,
            "deferredActions": deferred_actions,
            "noteCount": note_count,
            "taskCount": task_count,
        },
        "deals": results,
    }
