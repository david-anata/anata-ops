#!/usr/bin/env python3
"""Read-only support-agent collectors and reporting helpers."""

from __future__ import annotations

import html as html_module
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Mapping, Optional

from scripts import run_fulfillment_support


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SUPPORT_AGENT_ROOT = ROOT_DIR / "support-agent"
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "fulfillment_support.json"
DEFAULT_REPORT_TITLE = "Fulfillment CS Review"
SCHEMA_VERSION = "1.0"
MAX_PREVIEW_CANDIDATES = 6
REPORTS_ROUTE_ROOT = "/admin/fulfillment-cs/reports"
LIFECYCLE_STATES = ("new", "investigating", "responded", "escalated", "waiting_human", "resolved")
UI_RECOMMENDATION_STATES = ("clarifying", "investigating", "ready_to_answer", "escalated", "resolved")


@dataclass(frozen=True)
class SupportAgentConfig:
    support_agent_root: Path = DEFAULT_SUPPORT_AGENT_ROOT
    reports_dir: Path = DEFAULT_SUPPORT_AGENT_ROOT / "reports"
    report_title: str = DEFAULT_REPORT_TITLE


def read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def load_config(
    config_path: Optional[Path | str] = None,
    *,
    overrides: Optional[Mapping[str, Any]] = None,
) -> SupportAgentConfig:
    path = Path(config_path).expanduser() if config_path else DEFAULT_CONFIG_PATH
    config_data = read_json_file(path) if path.exists() else {}
    paths = config_data.get("paths", {}) if isinstance(config_data.get("paths", {}), Mapping) else {}
    support_agent_root = Path(
        os.getenv("SUPPORT_AGENT_ROOT", "").strip()
        or config_data.get("agent_root", DEFAULT_SUPPORT_AGENT_ROOT)
    )
    if not support_agent_root.is_absolute():
        support_agent_root = (ROOT_DIR / support_agent_root).resolve()
    reports_dir = support_agent_root / "reports"
    if str(paths.get("runs", "")).strip():
        candidate = Path(str(paths["runs"])).expanduser()
        reports_dir = candidate if candidate.is_absolute() else (ROOT_DIR / candidate).resolve()
    reports_dir_override = os.getenv("SUPPORT_AGENT_REPORTS_DIR", "").strip()
    if reports_dir_override:
        reports_dir = Path(reports_dir_override).expanduser()
    report_title = os.getenv("SUPPORT_AGENT_REPORT_TITLE", "").strip() or DEFAULT_REPORT_TITLE
    if overrides:
        if "support_agent_root" in overrides:
            support_agent_root = Path(str(overrides["support_agent_root"])).expanduser()
        if "reports_dir" in overrides:
            reports_dir = Path(str(overrides["reports_dir"])).expanduser()
        if "report_title" in overrides:
            report_title = str(overrides["report_title"]).strip() or report_title
    return SupportAgentConfig(
        support_agent_root=support_agent_root,
        reports_dir=reports_dir,
        report_title=report_title,
    )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _report_id_for_timestamp(generated_at: datetime) -> str:
    return generated_at.isoformat().replace(":", "-")


def _report_slug(report_id: str) -> str:
    return f"support-review-{report_id}"


def _count_list(counter: Counter[str], *, label_key: str) -> List[Dict[str, Any]]:
    items = []
    for label, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        if not label:
            continue
        items.append({label_key: label, "count": count})
    return items


def _account_count_list(counter: Counter[tuple[str, str]]) -> List[Dict[str, Any]]:
    items = []
    for (account_name, account_id), count in sorted(counter.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
        if not account_name and not account_id:
            continue
        items.append({"account_name": account_name, "account_id": account_id, "count": count})
    return items


def _load_case_rows(connections_db_path: Optional[Path | str]) -> List[Dict[str, Any]]:
    if not connections_db_path:
        return []
    path = Path(connections_db_path)
    if not path.exists():
        return []
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute("SELECT * FROM support_cases").fetchall()
        return [dict(row) for row in rows]
    except sqlite3.Error:
        return []
    finally:
        connection.close()


def _load_case_lookup(connections_db_path: Optional[Path | str]) -> Dict[tuple[str, str], Dict[str, Any]]:
    return {
        (str(row.get("source_channel_id", "")), str(row.get("source_thread_ts", ""))): row
        for row in _load_case_rows(connections_db_path)
        if str(row.get("source_channel_id", "")).strip() and str(row.get("source_thread_ts", "")).strip()
    }


def _derive_lifecycle_state(case_row: Mapping[str, Any], action: Mapping[str, Any]) -> str:
    state = str(case_row.get("status", "")).strip()
    if state in LIFECYCLE_STATES:
        return state
    action_state = str(action.get("status", "")).strip()
    if action_state in LIFECYCLE_STATES:
        return action_state
    return "new"


def _derive_ui_recommendation(
    lifecycle_state: str,
    action: Mapping[str, Any],
    identifiers: Mapping[str, Any],
) -> str:
    if lifecycle_state == "resolved":
        return "resolved"
    if lifecycle_state in {"escalated", "waiting_human"} or bool(action.get("should_escalate")):
        return "escalated"
    reply_type = str(action.get("reply_type", "")).strip()
    if reply_type == "clarifying":
        return "clarifying"
    order_numbers = identifiers.get("order_numbers", []) if isinstance(identifiers, Mapping) else []
    tracking_numbers = identifiers.get("tracking_numbers", []) if isinstance(identifiers, Mapping) else []
    po_numbers = identifiers.get("po_numbers", []) if isinstance(identifiers, Mapping) else []
    if not order_numbers and not tracking_numbers and not po_numbers:
        return "clarifying"
    if lifecycle_state == "responded" or reply_type == "resolution":
        return "ready_to_answer"
    return "investigating"


def _candidate_evidence_summary(candidate: Mapping[str, Any]) -> str:
    evidence = candidate.get("evidence", {})
    if not isinstance(evidence, Mapping):
        return ""
    parts: List[str] = []
    for source_name in ("labelogics", "shopify"):
        source = evidence.get(source_name, {})
        if not isinstance(source, Mapping):
            continue
        summary = str(source.get("summary", "")).strip()
        reason = str(source.get("reason", "")).strip()
        rendered = summary or reason
        if rendered:
            parts.append(f"{source_name.title()}: {rendered}")
    return " | ".join(parts)


def _normalize_candidate(
    candidate: Mapping[str, Any],
    *,
    case_lookup: Mapping[tuple[str, str], Mapping[str, Any]],
    generated_at: datetime,
) -> Dict[str, Any]:
    channel_name = str(candidate.get("channel", "")).strip()
    channel_id = str(candidate.get("channel_id", "")).strip()
    thread_ts = str(candidate.get("thread_ts", candidate.get("ts", ""))).strip()
    case_id = run_fulfillment_support.case_id_for_thread(channel_id, thread_ts) if channel_id and thread_ts else ""
    case_row = dict(case_lookup.get((channel_id, thread_ts), {}))
    action = candidate.get("recommended_action", {})
    if not isinstance(action, Mapping):
        action = {}
    identifiers = candidate.get("identifiers", {})
    if not isinstance(identifiers, Mapping):
        identifiers = {}
    lifecycle_state = _derive_lifecycle_state(case_row, action)
    ui_recommendation = _derive_ui_recommendation(lifecycle_state, action, identifiers)
    brand = str(case_row.get("brand_name", "")).strip() or str(candidate.get("brand_name", "")).strip() or channel_name
    account_id = str(case_row.get("labelogics_account_id", "")).strip()
    account_name = str(case_row.get("brand_name", "")).strip() or brand
    updated_at = str(case_row.get("updated_at", "")).strip() or generated_at.isoformat()
    escalation_reason = str(case_row.get("escalation_reason", "")).strip() or str(action.get("escalation_reason", "")).strip()
    evidence_summary = _candidate_evidence_summary(candidate)
    normalized = dict(candidate)
    normalized.update(
        {
            "case_id": str(case_row.get("case_id", case_id)),
            "channel_name": channel_name,
            "customer_thread_link": str(candidate.get("permalink", "")).strip(),
            "lifecycle_state": lifecycle_state,
            "ui_recommendation": ui_recommendation,
            "brand": brand,
            "account_id": account_id,
            "account_name": account_name,
            "shopify_store_domain": str(case_row.get("shopify_store_domain", "")).strip(),
            "draft_reply": str(action.get("customer_reply", "")).strip(),
            "escalation_reason": escalation_reason or None,
            "evidence_summary": evidence_summary,
            "updated_at": updated_at,
            "source_message_ts": str(candidate.get("ts", "")).strip(),
            "relationship_type": str(case_row.get("relationship_type", "")).strip() or str(candidate.get("relationship_type", "")).strip(),
            "related_case_id": str(case_row.get("related_case_id", "")).strip() or str(candidate.get("related_case_id", "")).strip(),
            "issue_category": str(case_row.get("issue_category", "")).strip() or str(candidate.get("issue_category", "")).strip(),
            "primary_owner": str(case_row.get("primary_owner", "")).strip() or str(candidate.get("primary_owner", "")).strip(),
            "secondary_owner": str(case_row.get("secondary_owner", "")).strip() or str(candidate.get("secondary_owner", "")).strip(),
            "waiting_on": str(case_row.get("waiting_on", "")).strip() or str(candidate.get("waiting_on", "")).strip(),
        }
    )
    return normalized


def _normalize_case_row(case_row: Mapping[str, Any]) -> Dict[str, Any]:
    lifecycle_state = str(case_row.get("status", "")).strip() or "new"
    if lifecycle_state == "resolved":
        ui_recommendation = "resolved"
    elif lifecycle_state in {"escalated", "waiting_human"}:
        ui_recommendation = "escalated"
    elif str(case_row.get("waiting_on", "")).strip() == "customer":
        ui_recommendation = "clarifying"
    elif lifecycle_state == "responded":
        ui_recommendation = "ready_to_answer"
    else:
        ui_recommendation = "investigating"
    return {
        "case_id": str(case_row.get("case_id", "")),
        "channel_name": str(case_row.get("source_channel_name", "")),
        "channel_id": str(case_row.get("source_channel_id", "")),
        "thread_ts": str(case_row.get("source_thread_ts", "")),
        "customer_thread_link": "",
        "question_summary": str(case_row.get("customer_question_summary", "")),
        "lifecycle_state": lifecycle_state,
        "ui_recommendation": ui_recommendation,
        "brand": str(case_row.get("brand_name", "")).strip() or str(case_row.get("source_channel_name", "")),
        "account_id": str(case_row.get("labelogics_account_id", "")),
        "account_name": str(case_row.get("brand_name", "")).strip() or str(case_row.get("source_channel_name", "")),
        "shopify_store_domain": str(case_row.get("shopify_store_domain", "")),
        "draft_reply": str(case_row.get("customer_facing_reply", "")),
        "escalation_reason": str(case_row.get("escalation_reason", "")) or None,
        "evidence_summary": str(case_row.get("latest_evidence_summary", "")),
        "updated_at": str(case_row.get("updated_at", "")),
        "relationship_type": str(case_row.get("relationship_type", "")),
        "related_case_id": str(case_row.get("related_case_id", "")),
        "issue_category": str(case_row.get("issue_category", "")),
        "primary_owner": str(case_row.get("primary_owner", "")),
        "secondary_owner": str(case_row.get("secondary_owner", "")),
        "waiting_on": str(case_row.get("waiting_on", "")),
    }


def _report_links(report_slug: str) -> Dict[str, str]:
    return {
        "self_json": f"{REPORTS_ROUTE_ROOT}/{report_slug}.json",
        "self_html": f"{REPORTS_ROUTE_ROOT}/{report_slug}",
        "reports_index": f"{REPORTS_ROUTE_ROOT}/",
        "latest": f"{REPORTS_ROUTE_ROOT}/latest",
    }


def build_candidate_report(
    review_payload: Mapping[str, Any],
    *,
    generated_at: Optional[datetime] = None,
    title: str = DEFAULT_REPORT_TITLE,
    connections_db_path: Optional[Path | str] = None,
) -> Dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc)
    review_section = review_payload.get("review_candidates", {})
    candidates = review_section.get("candidates", []) if isinstance(review_section, Mapping) else []
    case_lookup = _load_case_lookup(connections_db_path)
    case_rows = _load_case_rows(connections_db_path)
    normalized_candidates = [
        _normalize_candidate(item, case_lookup=case_lookup, generated_at=generated_at)
        for item in candidates
        if isinstance(item, Mapping)
    ]
    seen_case_ids = {str(item.get("case_id", "")).strip() for item in normalized_candidates if str(item.get("case_id", "")).strip()}
    for case_row in case_rows:
        case_id = str(case_row.get("case_id", "")).strip()
        if not case_id or case_id in seen_case_ids:
            continue
        normalized_candidates.append(_normalize_case_row(case_row))
    action_counts = Counter(str(item.get("ui_recommendation", "")).strip() for item in normalized_candidates if str(item.get("ui_recommendation", "")).strip())
    lifecycle_counts = Counter(str(item.get("lifecycle_state", "")).strip() for item in normalized_candidates if str(item.get("lifecycle_state", "")).strip())
    reply_type_counts = Counter(
        str(item.get("recommended_action", {}).get("reply_type", "")).strip()
        for item in normalized_candidates
        if isinstance(item.get("recommended_action", {}), Mapping)
        and str(item.get("recommended_action", {}).get("reply_type", "")).strip()
    )
    brand_counts = Counter(str(item.get("brand", "")).strip() for item in normalized_candidates if str(item.get("brand", "")).strip())
    account_counts = Counter(
        (str(item.get("account_name", "")).strip(), str(item.get("account_id", "")).strip())
        for item in normalized_candidates
        if str(item.get("account_name", "")).strip() or str(item.get("account_id", "")).strip()
    )
    escalation_items = [
        {
            "case_id": item.get("case_id", ""),
            "reason": item.get("escalation_reason"),
            "channel_name": item.get("channel_name", ""),
            "customer_thread_link": item.get("customer_thread_link", ""),
        }
        for item in normalized_candidates
        if item.get("ui_recommendation") == "escalated"
    ]
    warnings: List[str] = []
    status = str(review_section.get("status", "unknown")).strip() or "unknown"
    if status != "ready":
        warnings.append(f"Review candidate status is '{status}'.")
    report_id = _report_id_for_timestamp(generated_at)
    report_slug = _report_slug(report_id)
    summary = {
        "candidate_count": len(normalized_candidates),
        "action_counts": {state: int(action_counts.get(state, 0)) for state in UI_RECOMMENDATION_STATES},
        "lifecycle_counts": {state: int(lifecycle_counts.get(state, 0)) for state in LIFECYCLE_STATES},
        "brand_counts": _count_list(brand_counts, label_key="brand"),
        "account_counts": _account_count_list(account_counts),
        "escalation_count": len(escalation_items),
        "unresolved_count": sum(1 for item in normalized_candidates if item.get("lifecycle_state") != "resolved"),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": report_id,
        "report_slug": report_slug,
        "title": title,
        "generated_at": generated_at.isoformat(),
        "status": status,
        "summary": summary,
        "recent_candidates": normalized_candidates[:MAX_PREVIEW_CANDIDATES],
        "candidates": normalized_candidates,
        "escalations": escalation_items,
        "links": _report_links(report_slug),
        "warnings": warnings,
        "candidate_count": summary["candidate_count"],
        "action_counts": summary["action_counts"],
        "lifecycle_counts": summary["lifecycle_counts"],
        "reply_type_counts": dict(sorted(reply_type_counts.items())),
    }


def render_candidate_report_markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    lines = [
        f"# {report.get('title', DEFAULT_REPORT_TITLE)}",
        "",
        f"- Generated at: {report.get('generated_at', '')}",
        f"- Status: {report.get('status', '')}",
        f"- Candidate threads: {summary.get('candidate_count', report.get('candidate_count', 0))}",
        f"- Unresolved: {summary.get('unresolved_count', 0)}",
        f"- Escalated: {summary.get('escalation_count', 0)}",
        "",
    ]
    action_counts = summary.get("action_counts", report.get("action_counts", {}))
    if isinstance(action_counts, Mapping):
        lines.append("## Action Counts")
        lines.append("")
        for key in UI_RECOMMENDATION_STATES:
            if int(action_counts.get(key, 0) or 0):
                lines.append(f"- {key}: {action_counts[key]}")
        lines.append("")
    lifecycle_counts = summary.get("lifecycle_counts", report.get("lifecycle_counts", {}))
    if isinstance(lifecycle_counts, Mapping):
        lines.append("## Lifecycle Counts")
        lines.append("")
        for key in LIFECYCLE_STATES:
            if int(lifecycle_counts.get(key, 0) or 0):
                lines.append(f"- {key}: {lifecycle_counts[key]}")
        lines.append("")
    lines.append("## Candidates")
    lines.append("")
    for item in report.get("candidates", []):
        lines.append(f"### {item.get('brand', item.get('channel_name', 'Unknown'))}")
        lines.append(f"- Case ID: {item.get('case_id', '')}")
        lines.append(f"- Channel: {item.get('channel_name', item.get('channel', ''))}")
        lines.append(f"- Link: {item.get('customer_thread_link', item.get('permalink', ''))}")
        lines.append(f"- Lifecycle: {item.get('lifecycle_state', '')}")
        lines.append(f"- Recommendation: {item.get('ui_recommendation', '')}")
        lines.append(f"- Summary: {item.get('question_summary', '')}")
        lines.append(f"- Draft reply: {item.get('draft_reply', item.get('recommended_action', {}).get('customer_reply', ''))}")
        if item.get("evidence_summary"):
            lines.append(f"- Evidence: {item.get('evidence_summary', '')}")
        if item.get("escalation_reason"):
            lines.append(f"- Escalation reason: {item.get('escalation_reason', '')}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_candidate_report_html(report: Mapping[str, Any]) -> str:
    summary = report.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    rows: List[str] = []
    for item in report.get("candidates", []):
        rows.append(
            "<tr>"
            f"<td>{html_module.escape(str(item.get('case_id', '')))}</td>"
            f"<td>{html_module.escape(str(item.get('brand', item.get('brand_name', ''))))}</td>"
            f"<td>{html_module.escape(str(item.get('channel_name', item.get('channel', ''))))}</td>"
            f"<td><a href=\"{html_module.escape(str(item.get('customer_thread_link', item.get('permalink', ''))), quote=True)}\">thread</a></td>"
            f"<td>{html_module.escape(str(item.get('lifecycle_state', '')))}</td>"
            f"<td>{html_module.escape(str(item.get('ui_recommendation', '')))}</td>"
            f"<td>{html_module.escape(str(item.get('question_summary', '')))}</td>"
            f"<td>{html_module.escape(str(item.get('draft_reply', item.get('recommended_action', {}).get('customer_reply', ''))))}</td>"
            "</tr>"
        )
    warnings = report.get("warnings", [])
    warning_html = ""
    if isinstance(warnings, list) and warnings:
        warning_html = "<ul>" + "".join(f"<li>{html_module.escape(str(item))}</li>" for item in warnings) + "</ul>"
    return (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        f"<title>{html_module.escape(str(report.get('title', DEFAULT_REPORT_TITLE)))}</title>"
        "<style>body{font-family:Arial,sans-serif;margin:24px;}table{border-collapse:collapse;width:100%;}"
        "th,td{border:1px solid #ccc;padding:8px;vertical-align:top;}th{text-align:left;background:#f3f3f3;}"
        ".stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:20px 0;}"
        ".card{border:1px solid #ddd;padding:12px;border-radius:8px;background:#fafafa;}"
        "</style></head><body>"
        f"<h1>{html_module.escape(str(report.get('title', DEFAULT_REPORT_TITLE)))}</h1>"
        f"<p>Generated at: {html_module.escape(str(report.get('generated_at', '')))}</p>"
        f"<p>Status: {html_module.escape(str(report.get('status', '')))}</p>"
        f"<div class=\"stats\">"
        f"<div class=\"card\"><strong>{html_module.escape(str(summary.get('candidate_count', report.get('candidate_count', 0))))}</strong><br>Candidate threads</div>"
        f"<div class=\"card\"><strong>{html_module.escape(str(summary.get('unresolved_count', 0)))}</strong><br>Unresolved</div>"
        f"<div class=\"card\"><strong>{html_module.escape(str(summary.get('escalation_count', 0)))}</strong><br>Escalated</div>"
        "</div>"
        f"{warning_html}"
        "<table><thead><tr><th>Case ID</th><th>Brand</th><th>Channel</th><th>Link</th><th>Lifecycle</th><th>Recommendation</th><th>Summary</th><th>Draft Reply</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></body></html>"
    )


def _report_entry_from_payload(path: Path, payload: Mapping[str, Any]) -> Dict[str, Any]:
    summary = payload.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    report_id = str(payload.get("report_id", "")).strip() or path.stem.replace("support-review-", "", 1)
    report_slug = str(payload.get("report_slug", "")).strip() or path.stem
    return {
        "report_id": report_id,
        "report_slug": report_slug,
        "title": str(payload.get("title", DEFAULT_REPORT_TITLE)).strip() or DEFAULT_REPORT_TITLE,
        "generated_at": str(payload.get("generated_at", "")).strip(),
        "candidate_count": _safe_int(summary.get("candidate_count", payload.get("candidate_count", 0))),
        "action_counts": summary.get("action_counts", payload.get("action_counts", {})),
        "lifecycle_counts": summary.get("lifecycle_counts", payload.get("lifecycle_counts", {})),
        "artifact_formats": [
            ext for ext in ("json", "html", "md")
            if (path.parent / f"{report_slug}.{ext}").exists()
        ],
        "links": {
            "detail": f"{REPORTS_ROUTE_ROOT}/{report_slug}",
            "json": f"{REPORTS_ROUTE_ROOT}/{report_slug}.json",
            "html": f"{REPORTS_ROUTE_ROOT}/{report_slug}.html",
            "md": f"{REPORTS_ROUTE_ROOT}/{report_slug}.md",
        },
    }


def _build_report_index(output_dir: Path) -> Dict[str, Any]:
    reports: List[Dict[str, Any]] = []
    for path in sorted(output_dir.glob("support-review-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        payload = read_json_file(path)
        if not payload:
            continue
        reports.append(_report_entry_from_payload(path, payload))
    latest_report_id = reports[0]["report_id"] if reports else ""
    latest_slug = reports[0]["report_slug"] if reports else ""
    index_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_report_id": latest_report_id,
        "reports": reports,
    }
    if latest_slug:
        index_payload["latest"] = {
            "json": str(output_dir / "latest.json"),
            "markdown": str(output_dir / "latest.md"),
            "html": str(output_dir / "latest.html"),
            "detail": f"{REPORTS_ROUTE_ROOT}/{latest_slug}",
        }
    return index_payload


def write_candidate_report_artifacts(report: Mapping[str, Any], *, output_dir: Path) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_slug = str(report.get("report_slug", "")).strip() or _report_slug(str(report.get("report_id", "")))
    json_path = output_dir / f"{report_slug}.json"
    markdown_path = output_dir / f"{report_slug}.md"
    html_path = output_dir / f"{report_slug}.html"
    latest_json_path = output_dir / "latest.json"
    latest_markdown_path = output_dir / "latest.md"
    latest_html_path = output_dir / "latest.html"
    index_path = output_dir / "index.json"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True))
    markdown_body = render_candidate_report_markdown(report)
    html_body = render_candidate_report_html(report)
    markdown_path.write_text(markdown_body)
    html_path.write_text(html_body)
    latest_json_path.write_text(json.dumps(report, indent=2, sort_keys=True))
    latest_markdown_path.write_text(markdown_body)
    latest_html_path.write_text(html_body)
    index_path.write_text(json.dumps(_build_report_index(output_dir), indent=2, sort_keys=True))
    return {
        "json": json_path,
        "markdown": markdown_path,
        "html": html_path,
        "latest_json": latest_json_path,
        "latest_markdown": latest_markdown_path,
        "latest_html": latest_html_path,
        "index": index_path,
    }


def run_candidate_review_pipeline(
    *,
    config_path: Optional[Path | str] = None,
    support_config: Optional[SupportAgentConfig] = None,
    now: Optional[datetime] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    support_config = support_config or load_config(config_path)
    config_path_obj = Path(config_path).expanduser() if config_path else run_fulfillment_support.DEFAULT_CONFIG_PATH
    run_fulfillment_support.load_env_file(run_fulfillment_support.DEFAULT_ENV_PATH)
    config = run_fulfillment_support.read_config(config_path_obj)
    workspace_root = run_fulfillment_support.resolve_workspace_root(config)
    directories = run_fulfillment_support.resolve_directories(config, workspace_root)
    run_fulfillment_support.ensure_directories(directories)
    runtime_now = now or datetime.now(timezone.utc)
    summary = run_fulfillment_support.build_summary(config, config_path_obj, directories, include_live_checks=True)
    summary["review_candidates"] = run_fulfillment_support.review_candidates(
        config,
        directories,
        now=runtime_now.astimezone(timezone.utc if runtime_now.tzinfo is None else runtime_now.tzinfo),
        live_check_payload=summary.get("live_checks", {}),
    )
    report = build_candidate_report(
        summary,
        generated_at=runtime_now,
        title=support_config.report_title,
        connections_db_path=directories.get("connections_db"),
    )
    artifacts: Dict[str, Path] = {}
    if persist:
        artifacts = write_candidate_report_artifacts(report, output_dir=support_config.reports_dir)
    return {"summary": summary, "report": report, "artifacts": artifacts}
