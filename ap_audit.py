#!/usr/bin/env python3
"""Weekly AP audit and reconciliation agent."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
from collections import defaultdict
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_VENDOR_ALIASES = {
    "intuit payroll tax": "Intuit Payroll Tax",
    "intuit payroll": "Intuit Payroll",
    "intuit transaction fee": "Intuit Transaction Fee",
    "google workspace": "Google Workspace",
    "google *workspace": "Google Workspace",
    "intuit qbooks": "QuickBooks",
    "quickbooks": "QuickBooks",
    "shopify com": "Shopify",
    "shopify": "Shopify",
    "adobe inc": "Adobe Creative Cloud",
    "adobe creative cloud": "Adobe Creative Cloud",
    "adobe *creative cloud": "Adobe Creative Cloud",
    "meta": "Meta Ads",
    "facebook ads": "Meta Ads",
    "rocky mountain power": "Rocky Mountain Power",
    "netsuite": "NetSuite",
    "uline": "ULINE",
    "canva": "Canva",
    "loom": "Loom",
    "aws": "AWS",
    "amazon web services": "AWS",
    "amzn": "AMZN",
    "xfinity": "Comcast",
    "comcast": "Comcast",
    "clickup": "ClickUp",
    "instantly": "Instantly",
    "helium10": "Helium10",
    "godaddy": "GoDaddy",
    "apollo io": "Apollo",
    "brevo": "Brevo",
    "hunter io": "Hunter",
    "slack": "Slack",
    "microsoft": "Microsoft",
    "wise us inc": "Wise",
    "bear river": "Bear River",
    "intuit": "Intuit",
    "questargas": "Questar Gas",
    "questar gas": "Questar Gas",
    "enb gas": "Enbridge Gas",
    "enbridge gas": "Enbridge Gas",
    "fora financial": "Fora Loan",
    "forafinancial": "Fora Loan",
    "forafinancial s6": "Fora Loan",
    "stripe capital": "Stripe Capital",
    "ondeck capital": "Ondeck Capital",
    "odkraod": "Ondeck Capital",
    "appfolio": "AppFolio",
    "amazon mktpl": "Amazon Marketplace",
    "amazon marketplace": "Amazon Marketplace",
    "amazon com": "Amazon Marketplace",
    "rockymtn pacific": "Rocky Mountain Power",
    "rockymtn/pacific": "Rocky Mountain Power",
    "costco whse": "Costco",
    "costco": "Costco",
    "wal mart": "Walmart",
    "walmart": "Walmart",
    "round up": "Round Up",
    "openai subscr": "OpenAI ChatGPT",
    "openai": "OpenAI",
    "perplexity ai": "Perplexity",
    "instamed": "InstaMed",
    "wyze labs": "Wyze",
    "withdrawal overd": "Overdraft Fee",
}

DEFAULT_GROUPED_VENDORS = {
    "Google Workspace": "Software | SaaS Rollup | Mar 2026",
    "Adobe Creative Cloud": "Software | SaaS Rollup | Mar 2026",
    "Canva": "Software | SaaS Rollup | Mar 2026",
    "Loom": "Software | SaaS Rollup | Mar 2026",
}

DEFAULT_CATEGORY_BY_VENDOR = {
    "Google Workspace": "Software",
    "Adobe Creative Cloud": "Software",
    "QuickBooks": "Software",
    "ULINE": "Warehouse Expense",
    "NetSuite": "ERP",
    "Rocky Mountain Power": "Utilities",
    "Canva": "Software",
    "Loom": "Software",
    "AWS": "Cloud Infrastructure",
    "Meta Ads": "Marketing",
    "Amazon Marketplace": "Operations",
    "ClickUp": "Software",
    "Instantly": "Software",
    "Helium10": "Software",
    "GoDaddy": "Software",
    "Apollo": "Software",
    "Brevo": "Software",
    "Hunter": "Software",
    "Slack": "Software",
    "Microsoft": "Software",
    "Comcast": "Utilities",
    "Questar Gas": "Utilities",
    "Enbridge Gas": "Utilities",
    "Lehi City Power": "Utilities",
    "Bear River": "Insurance",
    "Fora Loan": "Loan Payment",
    "Stripe Capital": "Loan Payment",
    "AppFolio": "Software",
    "Ondeck Capital": "Loan Payment",
    "Intuit": "Software",
    "Intuit Payroll": "Payroll Adjacent",
    "Intuit Payroll Tax": "Payroll Tax",
    "Intuit Transaction Fee": "Bank Fees",
    "Costco": "Operations",
    "Walmart": "Operations",
    "Round Up": "Operations",
    "OpenAI": "Software",
    "OpenAI ChatGPT": "Software",
    "Perplexity": "Software",
    "InstaMed": "Insurance",
    "Wyze": "Operations",
    "Overdraft Fee": "Bank Fees",
}

DEFAULT_RECURRING_VENDORS = {
    "Google Workspace": "Monthly",
    "Adobe Creative Cloud": "Monthly",
    "QuickBooks": "Monthly",
    "NetSuite": "Monthly",
    "Rocky Mountain Power": "Monthly",
    "Canva": "Monthly",
    "Loom": "Monthly",
    "AWS": "Monthly",
    "Meta Ads": "Weekly",
    "ClickUp": "Monthly",
    "Instantly": "Monthly",
    "Helium10": "Monthly",
    "Slack": "Monthly",
    "Microsoft": "Monthly",
    "Comcast": "Monthly",
    "Questar Gas": "Monthly",
    "Enbridge Gas": "Monthly",
    "Lehi City Power": "Monthly",
    "Fora Loan": "Weekly",
    "Stripe Capital": "Weekly",
    "AppFolio": "Monthly",
    "Ondeck Capital": "Weekly",
    "Intuit Payroll": "Weekly",
    "Intuit Payroll Tax": "Weekly",
    "Intuit Transaction Fee": "Weekly",
    "OpenAI": "Monthly",
    "OpenAI ChatGPT": "Monthly",
    "Perplexity": "Monthly",
}

DEFAULT_STANDALONE_VENDORS = {
    "AWS",
    "NetSuite",
    "QuickBooks",
    "ULINE",
    "Rocky Mountain Power",
    "Amazon Marketplace",
    "Equipment Finance",
    "Meta Ads",
    "Ondeck Capital",
    "Intuit Payroll",
    "Intuit Payroll Tax",
    "Intuit Transaction Fee",
}

TRANSACTION_FIELD_ALIASES = {
    "reference": ["reference", "reference number", "transaction_reference", "transaction id", "id", "txn_id"],
    "date": ["date", "transaction_date", "posted_date", "posting date", "effective_date", "effective date"],
    "vendor": ["vendor", "merchant", "description", "extended description", "extended_description", "payee", "name"],
    "amount": ["amount", "debit", "withdrawal_amount", "value"],
    "transaction_type": ["transaction_type", "transaction type", "type", "entry_type"],
    "account": ["account", "account_name", "source_account", "card", "source"],
    "memo": ["memo", "note", "notes", "details", "extended description", "extended_description"],
    "category": ["category", "expense_category", "transaction category"],
}

TRANSACTION_PRIMARY_DESCRIPTOR_FIELDS = [
    "extended description",
    "extended_description",
    "description",
    "vendor",
    "merchant",
    "payee",
    "name",
]

TRANSACTION_SECONDARY_DESCRIPTOR_FIELDS = [
    "memo",
    "details",
]

TASK_FIELD_ALIASES = {
    "task_id": ["task_id", "id"],
    "task_name": ["task_name", "name", "title"],
    "vendor_name": ["vendor_name", "vendor", "payee"],
    "category": ["category", "expense_category"],
    "amount_due": ["amount_due", "total_due", "invoice_amount"],
    "amount_paid": ["amount_paid", "paid_amount"],
    "remaining_balance": ["remaining_balance", "balance", "amount_remaining"],
    "frequency": ["frequency", "billing_frequency"],
    "due_date": ["due_date"],
    "expected_charge_date": ["expected_charge_date", "expected_withdrawal_date"],
    "status": ["status"],
    "payment_method": ["payment_method"],
    "grouped_flag": ["grouped_flag", "grouped", "is_grouped"],
    "notes": ["notes", "note"],
    "transaction_references": ["transaction_references", "transaction_reference", "refs"],
    "cashflow_priority": ["cashflow_priority", "priority"],
    "slack_warning_flag": ["slack_warning_flag", "warning_flag"],
    "last_reviewed_date": ["last_reviewed_date"],
    "ap_state": ["ap_state", "ap state"],
    "group_name": ["group_name", "group name"],
    "task_class": ["task_class", "task class"],
    "needs_human_review": ["needs_human_review", "needs human review"],
    "last_audit_result": ["last_audit_result", "last audit result"],
}

DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%b %d %Y",
    "%b %d, %Y",
    "%B %d %Y",
    "%B %d, %Y",
)

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_SCHEMA_PATH = ROOT_DIR / "config" / "clickup_ap_schema.json"
DEFAULT_AUTOMATION_PATH = ROOT_DIR / "config" / "ap_automation_config.json"
DEFAULT_RULES_PATH = ROOT_DIR / "config" / "ap_rules.json"
SLACK_WEBHOOK_ENV = "SLACK_WEBHOOK_URL"

MATERIAL_WARNING_AMOUNT = 500.0

STATE_TO_CLICKUP_STATUS = {
    "paid": "Closed",
    "removed no longer due": "Closed",
    "removed / no longer due": "Closed",
}


@dataclass
class Transaction:
    reference: str
    date: Optional[date]
    vendor_raw: str
    vendor_name: str
    amount: float
    transaction_type: str = ""
    account: str = ""
    memo: str = ""
    category: str = ""
    source_row: Dict[str, Any] = field(default_factory=dict)
    disposition: str = ""
    confidence: float = 0.0
    matched_task_id: str = ""
    matched_task_name: str = ""
    reason: str = ""


@dataclass
class ClickUpTask:
    task_id: str
    task_name: str
    vendor_name: str
    category: str
    amount_due: float
    amount_paid: float
    remaining_balance: float
    frequency: str
    due_date: Optional[date]
    expected_charge_date: Optional[date]
    status: str
    payment_method: str
    grouped_flag: bool
    notes: str
    transaction_references: List[str]
    cashflow_priority: str
    slack_warning_flag: bool
    last_reviewed_date: Optional[date]
    ap_state: str = ""
    group_name: str = ""
    task_class: str = ""
    needs_human_review: bool = False
    last_audit_result: str = ""
    clickup_status_type: str = ""
    priority: str = ""
    description: str = ""
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    source_row: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchCandidate:
    task: ClickUpTask
    score: float
    reasons: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a weekly AP reconciliation audit.")
    parser.add_argument("--transactions", help="Path to transaction export (CSV, TSV, JSON, or raw text).")
    parser.add_argument("--clickup", help="Path to ClickUp AP export (CSV, TSV, JSON, or raw text).")
    parser.add_argument("--clickup-token", help="ClickUp API token. Falls back to CLICKUP_API_TOKEN.")
    parser.add_argument("--clickup-list-id", help="ClickUp List ID for the AP dashboard. Falls back to CLICKUP_LIST_ID.")
    parser.add_argument("--clickup-view-id", help="ClickUp View ID for the AP dashboard. Falls back to CLICKUP_VIEW_ID.")
    parser.add_argument("--rules", help="Optional JSON rules file.")
    parser.add_argument("--data-dir", default="data", help="Folder used for default weekly file discovery.")
    parser.add_argument("--mode", choices=("weekly", "daily"), default="weekly", help="Audit mode.")
    parser.add_argument("--as-of-date", default=date.today().isoformat(), help="Audit date in YYYY-MM-DD format.")
    parser.add_argument("--lookback-days", type=int, default=7, help="Only review transactions within this many days up to --as-of-date.")
    parser.add_argument("--payload-out", help="Optional output path for machine payload JSON.")
    parser.add_argument("--report-out", help="Optional output path for the human-readable report.")
    parser.add_argument("--schema-report-out", help="Optional output path for ClickUp schema audit JSON.")
    parser.add_argument("--slack-payload-out", help="Optional output path for Slack payload JSON.")
    parser.add_argument("--apply-clickup-updates", action="store_true", help="Apply low-risk update_tasks and grouped_rollups to ClickUp.")
    parser.add_argument("--post-slack", action="store_true", help="Post the generated Slack payload to SLACK_WEBHOOK_URL.")
    return parser.parse_args()


def load_rules(path: Optional[str]) -> Dict[str, Any]:
    rules = {
        "vendor_aliases": dict(DEFAULT_VENDOR_ALIASES),
        "grouped_vendors": dict(DEFAULT_GROUPED_VENDORS),
        "category_by_vendor": dict(DEFAULT_CATEGORY_BY_VENDOR),
        "recurring_vendors": dict(DEFAULT_RECURRING_VENDORS),
        "standalone_vendors": sorted(DEFAULT_STANDALONE_VENDORS),
        "critical_amount": 1000.0,
        "grouped_categories": ["Software", "Marketing", "Operations"],
        "never_group_categories": ["Utilities", "Insurance", "Payroll Adjacent", "Payroll Tax", "Loan Payment", "Credit Card"],
        "material_warning_amount": MATERIAL_WARNING_AMOUNT,
    }
    default_rule_paths = [DEFAULT_RULES_PATH]
    for default_rule_path in default_rule_paths:
        if default_rule_path.exists():
            user_rules = json.loads(default_rule_path.read_text())
            for key, value in user_rules.items():
                if isinstance(value, dict) and isinstance(rules.get(key), dict):
                    rules[key].update(value)
                else:
                    rules[key] = value
    if not path:
        return rules
    user_rules = json.loads(Path(path).read_text())
    for key, value in user_rules.items():
        if isinstance(value, dict) and isinstance(rules.get(key), dict):
            rules[key].update(value)
        else:
            rules[key] = value
    return rules


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def load_schema_manifest() -> Dict[str, Any]:
    return load_json_file(DEFAULT_SCHEMA_PATH)


def load_automation_config() -> Dict[str, Any]:
    return load_json_file(DEFAULT_AUTOMATION_PATH)


def parse_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        timestamp = int(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        try:
            return datetime.utcfromtimestamp(timestamp).date()
        except (OverflowError, OSError, ValueError):
            pass
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{13}", text):
        try:
            return datetime.utcfromtimestamp(int(text) / 1000).date()
        except (OverflowError, OSError, ValueError):
            pass
    if re.fullmatch(r"\d{10}", text):
        try:
            return datetime.utcfromtimestamp(int(text)).date()
        except (OverflowError, OSError, ValueError):
            pass
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_money(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return round(abs(float(value)), 2)
    text = str(value).strip()
    if not text:
        return 0.0
    negative = text.startswith("(") and text.endswith(")")
    cleaned = re.sub(r"[^0-9.\-]", "", text)
    if cleaned in ("", "-", "."):
        return 0.0
    amount = float(cleaned)
    if negative:
        amount = -abs(amount)
    return round(abs(amount), 2)


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "grouped"}


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def pick_value(row: Dict[str, Any], aliases: Sequence[str]) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for alias in aliases:
        if alias.lower() in lowered:
            return lowered[alias.lower()]
    return ""


def row_text(row: Dict[str, Any]) -> str:
    return " ".join(normalize_spaces(str(value)) for value in row.values() if str(value or "").strip())


def descriptor_score(value: str) -> int:
    text = normalize_spaces(value)
    if not text:
        return -10_000
    score = len(text)
    lowered = text.lower()
    if any(token in lowered for token in ("co:", "entry class code", "ach trace number", "withdrawal debit", "withdrawal ach", "payment to ", "intuit service charges/fees")):
        score += 60
    if any(token in lowered for token in ("extended", "type:", "name:", "amzn.com/bill", "a2a transfer", "stripe cap", "forafinancial")):
        score += 30
    if re.fullmatch(r"withdrawal\s+ach\b.*", lowered) or re.fullmatch(r"withdrawal\s+home\b.*", lowered):
        score -= 25
    if lowered in {"amazon", "paypal", "wise", "google", "intuit", "payment", "withdrawal"}:
        score -= 20
    return score


def pick_transaction_vendor_text(row: Dict[str, Any]) -> str:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    candidates: List[str] = []
    for field_name in TRANSACTION_PRIMARY_DESCRIPTOR_FIELDS:
        value = normalize_spaces(str(lowered.get(field_name, "")))
        if value:
            candidates.append(value)
    if not candidates:
        for field_name in TRANSACTION_SECONDARY_DESCRIPTOR_FIELDS:
            value = normalize_spaces(str(lowered.get(field_name, "")))
            if value:
                candidates.append(value)
    if not candidates:
        return str(pick_value(row, TRANSACTION_FIELD_ALIASES["vendor"]))
    return max(candidates, key=descriptor_score)


def normalize_vendor(raw_vendor: str, rules: Dict[str, Any]) -> str:
    raw_vendor = normalize_spaces(raw_vendor or "")
    if not raw_vendor:
        return "Unknown Vendor"
    raw_vendor = extract_vendor_hint(raw_vendor)
    normalized = normalize_key(raw_vendor)
    alias_items = sorted(rules["vendor_aliases"].items(), key=lambda item: len(item[0]), reverse=True)
    for alias, canonical in alias_items:
        alias_key = normalize_key(alias)
        if alias_key and alias_key in normalized:
            return canonical
    normalized = re.sub(r"\b\d+\b", " ", normalized)
    normalized = normalize_spaces(normalized.replace("*", " "))
    words = normalized.split()
    if not words:
        return raw_vendor
    if len(words) >= 2:
        return " ".join(word.capitalize() for word in words[:2])
    return words[0].capitalize()


def extract_vendor_hint(raw_vendor: str) -> str:
    text = normalize_spaces(raw_vendor)
    lower = text.lower()

    if "a2a transfer" in lower:
        return "Internal Transfer"

    if lower.startswith("payment to "):
        return text[11:]
    if lower.startswith("transfer to ") or lower.startswith("from share") or lower.startswith("to share"):
        return "Internal Transfer"
    if lower.startswith("withdrawal ach") and "co:" in lower:
        type_match = re.search(r"\btype:\s*(.*?)\s+co:\s*", text, flags=re.IGNORECASE)
        company_match = re.search(
            r"\bco:\s*(.*?)(?:\s{2,}|entry class code|ach trace number|name:|$)",
            text,
            flags=re.IGNORECASE,
        )
        ach_type = normalize_spaces(type_match.group(1)) if type_match else ""
        company = normalize_spaces(company_match.group(1)) if company_match else ""
        combined = normalize_key(f"{ach_type} {company}")
        if "co: intuit" in lower and "type: payroll" in lower:
            return "Intuit Payroll"
        if "co: intuit" in lower and "type: tax" in lower:
            return "Intuit Payroll Tax"
        if "co: intuit" in lower and "type: tran fee" in lower:
            return "Intuit Transaction Fee"
        if "stripe cap" in combined:
            return "Stripe Capital"
        if "wise" in combined:
            return "Wise"
        if "bear river" in combined:
            return "Bear River"
        if "citi autopay" in combined:
            return "Citibank"
        if company and normalize_key(company) not in {"anata"}:
            return company
        company = re.split(r"entry class code|ach trace number", text, flags=re.IGNORECASE)[0]
        company = re.split(r"\bco:\b", company, flags=re.IGNORECASE)[-1]
        company = company.replace("TYPE:", "").strip()
        if company:
            return normalize_spaces(company)
    if lower.startswith("withdrawal pos"):
        merchant = re.split(r"\bcard\b", text[14:], flags=re.IGNORECASE)[0]
        merchant = merchant.replace("#", " ").strip()
        merchant = re.sub(r"\s+\d{3,}.*$", "", merchant).strip()
        merchant = re.sub(r"\b[A-Z]{2}\b$", "", merchant).strip()
        merchant = normalize_spaces(merchant)
        if merchant:
            return merchant
    if lower.startswith("withdrawal overd"):
        return "Overdraft Fee"
    if lower.startswith("withdrawal debit "):
        merchant = re.split(r"\bdate\b|\bcard\b", text[17:], flags=re.IGNORECASE)[0]
        merchant = re.sub(r"^(DNH\*|SPI\*|WWW\.|SQ \*|TST\*|POS DEBIT )", "", merchant, flags=re.IGNORECASE)
        merchant = merchant.replace("Amzn.com/bill", "").replace("WWW.", "")
        merchant = re.sub(r"\*[\w\d]+", "", merchant).strip()
        merchant = re.sub(r"\s+\d[\d\- ].*$", "", merchant).strip()
        merchant = re.sub(r"\b[A-Z]{2}\b$", "", merchant).strip()
        if merchant:
            return merchant
    if lower.startswith("withdrawal card"):
        return "VISA International Service Assessment"
    if lower.startswith("intuit service charges/fees"):
        return "Intuit"
    if lower.startswith("intuit deposit"):
        return "Intuit Deposit"
    if lower.startswith("deposit by check"):
        return "Check Deposit"
    return text


def infer_category(vendor_name: str, raw_category: str, rules: Dict[str, Any]) -> str:
    if raw_category:
        return raw_category
    return rules.get("category_by_vendor", {}).get(vendor_name, "Uncategorized")


def infer_frequency(vendor_name: str, raw_frequency: str, rules: Dict[str, Any]) -> str:
    if raw_frequency:
        return raw_frequency
    return rules.get("recurring_vendors", {}).get(vendor_name, "Ad Hoc")


def load_rows(path: str) -> List[Dict[str, Any]]:
    file_path = Path(path)
    content = file_path.read_text().strip()
    if not content:
        return []
    suffix = file_path.suffix.lower()
    if suffix == ".json":
        data = json.loads(content)
        if isinstance(data, dict):
            for key in ("transactions", "tasks", "clickup_tasks", "rows"):
                if isinstance(data.get(key), list):
                    return data[key]
            return [data]
        return data
    if suffix in {".csv", ".tsv"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        with file_path.open(newline="") as handle:
            return list(csv.DictReader(handle, delimiter=delimiter))
    try:
        sample = content.splitlines()[0]
        dialect = csv.Sniffer().sniff(sample + "\n")
        with file_path.open(newline="") as handle:
            return list(csv.DictReader(handle, dialect=dialect))
    except Exception:
        return parse_raw_blocks(content)


def discover_input_file(data_dir: Path, patterns: Sequence[str], label: str) -> str:
    candidates: List[Path] = []
    for pattern in patterns:
        candidates.extend(path for path in data_dir.glob(pattern) if path.is_file())
    if not candidates:
        raise SystemExit(
            f"Could not find a {label} file in {data_dir}. "
            f"Either pass --{label} explicitly or place a matching file in the data directory."
        )
    candidates = sorted(set(candidates), key=lambda path: path.stat().st_mtime, reverse=True)
    return str(candidates[0])


def resolve_input_paths(args: argparse.Namespace) -> Tuple[str, Optional[str], Optional[str]]:
    data_dir = Path(args.data_dir)
    clickup_list_id = getattr(args, "clickup_list_id", None) or os.getenv("CLICKUP_LIST_ID")
    clickup_view_id = getattr(args, "clickup_view_id", None) or os.getenv("CLICKUP_VIEW_ID")
    transactions = args.transactions or discover_input_file(
        data_dir,
        (
            "transactions*.csv",
            "transactions*.tsv",
            "transactions*.json",
            "transactions*.txt",
            "bank_transactions*",
            "card_transactions*",
        ),
        "transactions",
    )
    clickup = args.clickup
    if not clickup and not (clickup_list_id or clickup_view_id):
        clickup = discover_input_file(
            data_dir,
            (
                "clickup*.csv",
                "clickup*.tsv",
                "clickup*.json",
                "clickup*.txt",
                "clickup_tasks*",
                "ap_tasks*",
                "tasks*",
            ),
            "clickup",
        )
    rules = args.rules
    if not rules and data_dir.exists():
        explicit_rules = data_dir / "rules.json"
        if explicit_rules.exists():
            rules = str(explicit_rules)
        else:
            rule_matches = sorted(data_dir.glob("rules*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
            if rule_matches:
                rules = str(rule_matches[0])
    return transactions, clickup, rules


def request_json(
    method: str,
    url: str,
    *,
    token: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = token
    if extra_headers:
        headers.update(extra_headers)
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"API request failed ({exc.code}) for {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"API request failed for {url}: {exc.reason}") from exc


def fetch_json(url: str, token: str) -> Dict[str, Any]:
    return request_json("GET", url, token=token)


def custom_field_map(task_json: Dict[str, Any]) -> Dict[str, Any]:
    mapped: Dict[str, Any] = {}
    for field in task_json.get("custom_fields", []):
        name = normalize_key(str(field.get("name", "")))
        value = field.get("value")
        type_config = field.get("type_config") or {}
        if isinstance(value, dict) and "name" in value:
            value = value["name"]
        if field.get("type") == "drop_down" and isinstance(value, int):
            for option in type_config.get("options", []):
                if option.get("orderindex") == value or option.get("id") == value:
                    value = option.get("name")
                    break
        mapped[name] = value
    return mapped


def derive_vendor_from_task_name(task_name: str) -> str:
    parts = [normalize_spaces(part) for part in task_name.split("|")]
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return task_name


def clickup_task_to_row(task_json: Dict[str, Any]) -> Dict[str, Any]:
    fields = custom_field_map(task_json)
    status = task_json.get("status") or {}
    task_name = str(task_json.get("name", ""))
    derived_category = ""
    derived_grouped = ""
    derived_task_class = "Standalone AP"
    normalized_name = normalize_key(task_name)
    if normalized_name.startswith("software week"):
        derived_category = "Software"
        derived_grouped = "true"
        derived_task_class = "Grouped Rollup"
    elif normalized_name.startswith("marketing growth tools week") or normalized_name.startswith("small ops marketplace week"):
        derived_grouped = "true"
        derived_task_class = "Grouped Rollup"
    elif normalized_name in {"rent", "comcast", "enbridge gas", "lehi city power", "stripe capital loan", "fora loan"}:
        derived_category = infer_category(task_name, "", load_rules(None))
    ap_state = (
        fields.get("ap state")
        or ("Grouped" if parse_bool(derived_grouped) else "")
        or ("Paid" if normalize_key(status.get("status", "")) == "closed" else "")
    )
    return {
        "task_id": task_json.get("id", ""),
        "task_name": task_name,
        "vendor_name": (
            fields.get("vendor")
            or fields.get("vendor name")
            or fields.get("payee")
            or fields.get("vendor / payee")
            or derive_vendor_from_task_name(task_name)
        ),
        "category": fields.get("expense category") or fields.get("category") or derived_category,
        "amount_due": fields.get("amount due") or fields.get("total due") or fields.get("amount") or "",
        "amount_paid": fields.get("amount paid") or "",
        "remaining_balance": fields.get("remaining balance") or fields.get("balance") or "",
        "frequency": fields.get("billing frequency") or fields.get("frequency") or "",
        "due_date": fields.get("due date") or task_json.get("due_date") or "",
        "expected_charge_date": (
            fields.get("expected withdrawal date")
            or fields.get("expected charge date")
            or fields.get("withdrawal date")
            or ""
        ),
        "status": status.get("status") or "",
        "payment_method": fields.get("payment method") or "",
        "grouped_flag": fields.get("grouped flag") or fields.get("grouped") or derived_grouped,
        "notes": task_json.get("text_content") or task_json.get("description") or "",
        "transaction_references": fields.get("transaction references") or fields.get("transaction reference") or "",
        "cashflow_priority": fields.get("cashflow priority") or fields.get("priority") or "",
        "slack_warning_flag": fields.get("slack warning flag") or fields.get("warning flag") or "",
        "last_reviewed_date": fields.get("last reviewed date") or "",
        "ap_state": ap_state,
        "group_name": fields.get("group name") or (task_name if parse_bool(derived_grouped) else ""),
        "task_class": fields.get("task class") or derived_task_class,
        "needs_human_review": fields.get("needs human review") or "",
        "last_audit_result": fields.get("last audit result") or "",
        "clickup_status_type": status.get("type") or "",
        "priority": (task_json.get("priority") or {}).get("priority") or "",
        "description": task_json.get("description") or task_json.get("text_content") or "",
        "custom_fields": fields,
    }


def fetch_clickup_tasks(token: str, list_id: Optional[str], view_id: Optional[str]) -> List[Dict[str, Any]]:
    if bool(list_id) == bool(view_id):
        raise SystemExit("Provide exactly one of --clickup-list-id or --clickup-view-id for live ClickUp review.")

    if view_id:
        base_url = f"https://api.clickup.com/api/v2/view/{view_id}/task"
    else:
        base_url = f"https://api.clickup.com/api/v2/list/{list_id}/task"

    tasks: List[Dict[str, Any]] = []
    page = 0
    while True:
        query = urllib.parse.urlencode(
            {
                "archived": "false",
                "include_closed": "true",
                "page": page,
            }
        )
        payload = fetch_json(f"{base_url}?{query}", token)
        page_tasks = payload.get("tasks", [])
        tasks.extend(clickup_task_to_row(task_json) for task_json in page_tasks)
        if not page_tasks or len(page_tasks) < 100:
            break
        page += 1
    return tasks


def fetch_clickup_custom_fields(token: str, list_id: str) -> List[Dict[str, Any]]:
    url = f"https://api.clickup.com/api/v2/list/{list_id}/field"
    payload = fetch_json(url, token)
    return payload.get("fields", [])


def inspect_clickup_schema(fields: Sequence[Dict[str, Any]], manifest: Dict[str, Any]) -> Dict[str, Any]:
    available = {}
    for field in fields:
        available[normalize_key(str(field.get("name", "")))] = {
            "id": field.get("id"),
            "name": field.get("name"),
            "type": field.get("type"),
            "required": field.get("required", False),
        }
    required = manifest.get("required_custom_fields", [])
    missing = []
    present = []
    for item in required:
        lookup = normalize_key(item["name"])
        if lookup in available:
            present.append({"name": item["name"], "id": available[lookup]["id"], "type": available[lookup]["type"]})
        else:
            missing.append(item)
    return {
        "required_field_count": len(required),
        "present_field_count": len(present),
        "missing_field_count": len(missing),
        "present_fields": present,
        "missing_fields": missing,
        "available_fields": list(available.values()),
    }


def index_clickup_fields(fields: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    indexed = {}
    for field in fields:
        indexed[normalize_key(str(field.get("name", "")))] = field
    return indexed


def compute_grouped_rollup_totals(tasks: Sequence[ClickUpTask], grouped_rollups: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    by_task: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rollup in grouped_rollups:
        by_task[rollup["group_task_id"]].append(rollup)
    for task in tasks:
        if task.task_id in by_task:
            totals[task.task_id] = round(task.amount_due + sum(item["amount"] for item in by_task[task.task_id]), 2)
    return totals


def map_ap_state_to_clickup_status(ap_state: str) -> Optional[str]:
    return STATE_TO_CLICKUP_STATUS.get(normalize_key(ap_state))


def build_clickup_update_actions(
    tasks: Sequence[ClickUpTask],
    updates: Sequence[Dict[str, Any]],
    grouped_rollups: Sequence[Dict[str, Any]],
    schema_fields: Sequence[Dict[str, Any]],
    as_of_date: date,
) -> Dict[str, Any]:
    field_index = index_clickup_fields(schema_fields)
    task_index = {task.task_id: task for task in tasks}
    grouped_totals = compute_grouped_rollup_totals(tasks, grouped_rollups)
    actions = []
    skipped = []

    for update in updates:
        task = task_index.get(update["task_id"])
        if not task:
            skipped.append({"task_id": update["task_id"], "reason": "task not found in current ClickUp payload"})
            continue
        field_updates = update["amount_changes"]["field_updates"]
        custom_field_updates = {}
        for field_name, value in (
            ("Amount Paid", field_updates.get("amount_paid")),
            ("Remaining Balance", field_updates.get("remaining_balance")),
            ("Expected Withdrawal Date", update["due_date_changes"]["field_updates"].get("expected_charge_date")),
            ("Last Reviewed Date", field_updates.get("last_reviewed_date")),
            ("AP State", field_updates.get("status")),
            ("Canonical Vendor", update["vendor"]),
            ("Expense Category", task.category),
            ("Cashflow Priority", task.cashflow_priority),
            ("Transaction References", update["source_transaction_reference"]),
            ("Last Audit Result", "MATCHED_NEEDS_UPDATE"),
            ("Needs Human Review", "false"),
        ):
            if value in (None, "", []):
                continue
            field_meta = field_index.get(normalize_key(field_name))
            if field_meta:
                custom_field_updates[field_name] = value

        note_lines = [line for line in update["notes_to_append"] if line]
        if task.last_audit_result:
            note_lines.insert(0, f"Previous audit result: {task.last_audit_result}")
        action = {
            "task_id": task.task_id,
            "task_name": task.task_name,
            "custom_field_updates": custom_field_updates,
            "append_notes": note_lines,
            "clickup_status": map_ap_state_to_clickup_status(field_updates.get("status", "")),
            "ap_state": field_updates.get("status", task.ap_state or update["current_status"]),
            "reason": update["recommended_change"],
            "safe_to_apply": True,
        }
        actions.append(action)

    for rollup in grouped_rollups:
        existing_group_action = next((item for item in actions if item["task_id"] == rollup["group_task_id"] and item["reason"] == "grouped rollup append"), None)
        if existing_group_action:
            existing_group_action["append_notes"].append(rollup["sub_detail_note"])
            continue
        task = task_index.get(rollup["group_task_id"])
        if not task:
            skipped.append({"task_id": rollup["group_task_id"], "reason": "group task not found in current ClickUp payload"})
            continue
        custom_field_updates = {}
        amount_field = field_index.get(normalize_key("Amount*")) or field_index.get(normalize_key("Amount Due"))
        if amount_field:
            custom_field_updates["Amount*"] = grouped_totals.get(task.task_id, task.amount_due)
        for field_name, value in (
            ("Grouped Flag", True),
            ("Group Name", task.group_name or task.task_name),
            ("AP State", "Grouped"),
            ("Last Reviewed Date", as_of_date.isoformat()),
            ("Last Audit Result", "POSSIBLE_GROUPED_ITEM"),
            ("Needs Human Review", False),
        ):
            field_meta = field_index.get(normalize_key(field_name))
            if field_meta:
                custom_field_updates[field_name] = value
        actions.append(
            {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "custom_field_updates": custom_field_updates,
                "append_notes": [rollup["sub_detail_note"]],
                "clickup_status": None,
                "ap_state": "Grouped",
                "reason": "grouped rollup append",
                "safe_to_apply": True,
            }
        )

    return {"actions": actions, "skipped": skipped}


def post_clickup_comment(token: str, task_id: str, comment_text: str) -> Dict[str, Any]:
    url = f"https://api.clickup.com/api/v2/task/{task_id}/comment"
    return request_json("POST", url, token=token, body={"comment_text": comment_text, "notify_all": False})


def update_clickup_task(token: str, task_id: str, *, status: Optional[str] = None) -> Dict[str, Any]:
    body = {}
    if status:
        body["status"] = status
    if not body:
        return {}
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    return request_json("PUT", url, token=token, body=body)


def set_clickup_custom_field_value(token: str, task_id: str, field_id: str, value: Any) -> Dict[str, Any]:
    url = f"https://api.clickup.com/api/v2/task/{task_id}/field/{field_id}"
    return request_json("POST", url, token=token, body=value)


def to_clickup_custom_field_body(field_meta: Dict[str, Any], value: Any) -> Dict[str, Any]:
    field_type = field_meta.get("type")
    if field_type == "drop_down":
        options = (field_meta.get("type_config") or {}).get("options", [])
        lookup = normalize_key(str(value))
        for option in options:
            if normalize_key(str(option.get("name", ""))) == lookup:
                return {"value": option.get("id")}
        raise SystemExit(f"Could not map dropdown value '{value}' for field {field_meta.get('name')}.")
    if field_type == "checkbox":
        return {"value": bool(value) if not isinstance(value, str) else parse_bool(value)}
    if field_type == "date":
        parsed = parse_date(value)
        if not parsed:
            raise SystemExit(f"Could not parse date value '{value}' for field {field_meta.get('name')}.")
        return {
            "value": int(datetime.combine(parsed, datetime.min.time()).timestamp() * 1000),
            "value_options": {"time": False},
        }
    if field_type in {"currency", "number"}:
        return {"value": parse_money(value)}
    return {"value": value}


def apply_clickup_actions(token: str, field_index: Dict[str, Dict[str, Any]], actions: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    applied = []
    for action in actions:
        if action.get("clickup_status"):
            update_clickup_task(token, action["task_id"], status=action["clickup_status"])
        for field_name, value in action["custom_field_updates"].items():
            field_meta = field_index.get(normalize_key(field_name))
            if not field_meta:
                continue
            body = to_clickup_custom_field_body(field_meta, value)
            set_clickup_custom_field_value(token, action["task_id"], field_meta["id"], body)
        if action["append_notes"]:
            comment_text = "\n".join(action["append_notes"])
            post_clickup_comment(token, action["task_id"], comment_text)
        applied.append(
            {
                "task_id": action["task_id"],
                "task_name": action["task_name"],
                "custom_field_count": len(action["custom_field_updates"]),
                "comment_count": 1 if action["append_notes"] else 0,
                "status_updated": bool(action.get("clickup_status")),
            }
        )
    return applied


def parse_raw_blocks(content: str) -> List[Dict[str, Any]]:
    blocks = [block.strip() for block in re.split(r"\n\s*\n", content) if block.strip()]
    rows: List[Dict[str, Any]] = []
    for block in blocks:
        row: Dict[str, Any] = {}
        for line in block.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            row[key.strip()] = value.strip()
        if row:
            rows.append(row)
    return rows


def normalize_transactions(rows: Iterable[Dict[str, Any]], rules: Dict[str, Any]) -> List[Transaction]:
    transactions: List[Transaction] = []
    for index, row in enumerate(rows, start=1):
        vendor_raw = pick_transaction_vendor_text(row)
        vendor_name = normalize_vendor(vendor_raw, rules)
        reference = str(pick_value(row, TRANSACTION_FIELD_ALIASES["reference"])) or f"txn-{index:04d}"
        transaction = Transaction(
            reference=reference,
            date=parse_date(pick_value(row, TRANSACTION_FIELD_ALIASES["date"])),
            vendor_raw=vendor_raw,
            vendor_name=vendor_name,
            amount=parse_money(pick_value(row, TRANSACTION_FIELD_ALIASES["amount"])),
            transaction_type=str(pick_value(row, TRANSACTION_FIELD_ALIASES["transaction_type"])),
            account=str(pick_value(row, TRANSACTION_FIELD_ALIASES["account"])),
            memo=str(pick_value(row, TRANSACTION_FIELD_ALIASES["memo"])),
            category=infer_category(vendor_name, str(pick_value(row, TRANSACTION_FIELD_ALIASES["category"])), rules),
            source_row=row,
        )
        if include_transaction(transaction):
            transactions.append(transaction)
    return transactions


def include_transaction(transaction: Transaction) -> bool:
    text = " ".join(
        normalize_key(part)
        for part in (
            transaction.transaction_type,
            transaction.vendor_raw,
            transaction.memo,
            transaction.category,
            row_text(transaction.source_row),
        )
    )
    if transaction.vendor_name == "Internal Transfer":
        return False
    if any(term in text for term in ("credit", "deposit", "incoming", "refund", "reversal")):
        return False
    if any(
        term in text
        for term in (
            "internal transfer",
            "a2a transfer",
            "from share",
            "to share",
            "payment to citibank",
            "payment to capital one",
            "payment to chase",
            "citi autopay",
        )
    ):
        return False
    if any(term in text for term in ("debit", "withdrawal", "purchase", "payment", "card", "ach")):
        return True
    return transaction.amount > 0


def normalize_tasks(rows: Iterable[Dict[str, Any]], rules: Dict[str, Any]) -> List[ClickUpTask]:
    tasks: List[ClickUpTask] = []
    for index, row in enumerate(rows, start=1):
        vendor_name = normalize_vendor(str(pick_value(row, TASK_FIELD_ALIASES["vendor_name"])), rules)
        amount_due = parse_money(pick_value(row, TASK_FIELD_ALIASES["amount_due"]))
        amount_paid = parse_money(pick_value(row, TASK_FIELD_ALIASES["amount_paid"]))
        remaining_balance = parse_money(pick_value(row, TASK_FIELD_ALIASES["remaining_balance"]))
        status = str(pick_value(row, TASK_FIELD_ALIASES["status"])) or "Unknown"
        ap_state = str(pick_value(row, TASK_FIELD_ALIASES["ap_state"])) or ""
        if not remaining_balance and amount_due and normalize_key(status) == "closed":
            amount_paid = max(amount_paid, amount_due)
            remaining_balance = 0.0
        if not remaining_balance and amount_due:
            remaining_balance = round(max(amount_due - amount_paid, 0.0), 2)
        if not ap_state:
            if parse_bool(pick_value(row, TASK_FIELD_ALIASES["grouped_flag"])):
                ap_state = "Grouped"
            elif normalize_key(status) == "closed":
                ap_state = "Paid"
            elif remaining_balance > 0 and parse_date(pick_value(row, TASK_FIELD_ALIASES["due_date"])) and parse_date(pick_value(row, TASK_FIELD_ALIASES["due_date"])) < date.today():
                ap_state = "Overdue - Review Needed"
            else:
                ap_state = status
        references_raw = str(pick_value(row, TASK_FIELD_ALIASES["transaction_references"]))
        references = [normalize_spaces(item) for item in re.split(r"[;,|]", references_raw) if normalize_spaces(item)]
        task = ClickUpTask(
            task_id=str(pick_value(row, TASK_FIELD_ALIASES["task_id"])) or f"task-{index:04d}",
            task_name=str(pick_value(row, TASK_FIELD_ALIASES["task_name"])) or f"Untitled Task {index}",
            vendor_name=vendor_name,
            category=infer_category(vendor_name, str(pick_value(row, TASK_FIELD_ALIASES["category"])), rules),
            amount_due=amount_due,
            amount_paid=amount_paid,
            remaining_balance=remaining_balance,
            frequency=infer_frequency(vendor_name, str(pick_value(row, TASK_FIELD_ALIASES["frequency"])), rules),
            due_date=parse_date(pick_value(row, TASK_FIELD_ALIASES["due_date"])),
            expected_charge_date=parse_date(pick_value(row, TASK_FIELD_ALIASES["expected_charge_date"])),
            status=status,
            payment_method=str(pick_value(row, TASK_FIELD_ALIASES["payment_method"])),
            grouped_flag=parse_bool(pick_value(row, TASK_FIELD_ALIASES["grouped_flag"])),
            notes=str(pick_value(row, TASK_FIELD_ALIASES["notes"])),
            transaction_references=references,
            cashflow_priority=str(pick_value(row, TASK_FIELD_ALIASES["cashflow_priority"])) or "Medium",
            slack_warning_flag=parse_bool(pick_value(row, TASK_FIELD_ALIASES["slack_warning_flag"])),
            last_reviewed_date=parse_date(pick_value(row, TASK_FIELD_ALIASES["last_reviewed_date"])),
            ap_state=ap_state,
            group_name=str(pick_value(row, TASK_FIELD_ALIASES["group_name"])),
            task_class=str(pick_value(row, TASK_FIELD_ALIASES["task_class"])) or ("Grouped Rollup" if parse_bool(pick_value(row, TASK_FIELD_ALIASES["grouped_flag"])) else "Standalone AP"),
            needs_human_review=parse_bool(pick_value(row, TASK_FIELD_ALIASES["needs_human_review"])),
            last_audit_result=str(pick_value(row, TASK_FIELD_ALIASES["last_audit_result"])),
            clickup_status_type=str(row.get("clickup_status_type", "")),
            priority=str(row.get("priority", "")),
            description=str(row.get("description", "")),
            custom_fields=dict(row.get("custom_fields", {})) if isinstance(row.get("custom_fields"), dict) else {},
            source_row=row,
        )
        tasks.append(task)
    return tasks


def amount_match_score(transaction_amount: float, task: ClickUpTask) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    if not task.amount_due and not task.remaining_balance:
        return 0.0, reasons
    comparisons = [value for value in (task.amount_due, task.remaining_balance) if value > 0]
    best = 0.0
    for value in comparisons:
        diff = abs(transaction_amount - value)
        if diff <= 0.01:
            best = max(best, 25.0)
            reasons.append(f"amount exact to ${value:.2f}")
        elif diff <= max(5.0, value * 0.02):
            best = max(best, 18.0)
            reasons.append(f"amount near-match to ${value:.2f}")
        elif transaction_amount < value and diff <= value * 0.75:
            best = max(best, 12.0)
            reasons.append(f"partial-payment sized against ${value:.2f}")
    return best, reasons


def date_match_score(transaction_date: Optional[date], task: ClickUpTask) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    reference_date = task.expected_charge_date or task.due_date
    if not transaction_date or not reference_date:
        return 0.0, reasons
    delta = abs((transaction_date - reference_date).days)
    if delta <= 2:
        reasons.append("date within 2 days")
        return 15.0, reasons
    if delta <= 5:
        reasons.append("date within 5 days")
        return 10.0, reasons
    if delta <= 10:
        reasons.append("date within 10 days")
        return 6.0, reasons
    if delta <= 30:
        reasons.append("date within 30 days")
        return 2.0, reasons
    return 0.0, reasons


def vendor_match_score(transaction: Transaction, task: ClickUpTask) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    txn_vendor = normalize_key(transaction.vendor_name)
    task_vendor = normalize_key(task.vendor_name)
    raw_vendor = normalize_key(transaction.vendor_raw)
    if txn_vendor == task_vendor:
        reasons.append("vendor exact normalized match")
        return 55.0, reasons
    if txn_vendor and task_vendor and (txn_vendor in task_vendor or task_vendor in txn_vendor):
        reasons.append("vendor contained normalized match")
        return 42.0, reasons
    if raw_vendor and task_vendor and task_vendor in raw_vendor:
        reasons.append("raw merchant string contains task vendor")
        return 38.0, reasons
    task_name = normalize_key(task.task_name)
    if txn_vendor and task_name and txn_vendor in task_name:
        reasons.append("task name contains vendor")
        return 34.0, reasons
    if raw_vendor.startswith("amzn") and task_vendor in {"aws", "amazon marketplace"}:
        reasons.append("AMZN alias may map to multiple Amazon vendors")
        return 28.0, reasons
    return 0.0, reasons


def score_candidate(transaction: Transaction, task: ClickUpTask) -> MatchCandidate:
    score = 0.0
    reasons: List[str] = []

    vendor_score, vendor_reasons = vendor_match_score(transaction, task)
    score += vendor_score
    reasons.extend(vendor_reasons)

    amount_score, amount_reasons = amount_match_score(transaction.amount, task)
    score += amount_score
    reasons.extend(amount_reasons)

    date_score, date_reasons = date_match_score(transaction.date, task)
    score += date_score
    reasons.extend(date_reasons)

    if task.frequency.lower() in {"monthly", "weekly"} and vendor_score >= 34.0:
        score += 5.0
        reasons.append(f"recurring {task.frequency.lower()} pattern")

    if transaction.reference and transaction.reference in task.transaction_references:
        score += 8.0
        reasons.append("transaction reference already recorded")

    return MatchCandidate(task=task, score=score, reasons=reasons)


def choose_group_task(transaction: Transaction, tasks: Sequence[ClickUpTask], rules: Dict[str, Any]) -> Optional[ClickUpTask]:
    if transaction.vendor_name in set(rules.get("standalone_vendors", [])):
        return None
    if transaction.category in set(rules.get("never_group_categories", [])):
        return None
    group_name = rules.get("grouped_vendors", {}).get(transaction.vendor_name)
    if group_name:
        for task in tasks:
            if task.grouped_flag and normalize_key(task.task_name) == normalize_key(group_name):
                return task
    if transaction.category in set(rules.get("grouped_categories", [])):
        matching_group_tasks = [
            task for task in tasks
            if task.grouped_flag and normalize_key(task.category) == normalize_key(transaction.category)
        ]
        if transaction.date and matching_group_tasks:
            week_of_month = min(((transaction.date.day - 1) // 7) + 1, 4)
            named_match = [
                task for task in matching_group_tasks
                if f"week {week_of_month}" in normalize_key(task.task_name)
            ]
            if named_match:
                open_match = [task for task in named_match if normalize_key(task.status) != "closed"]
                return open_match[0] if open_match else named_match[0]
            dated_match = sorted(
                matching_group_tasks,
                key=lambda task: abs(((determine_due_anchor(task) or transaction.date) - transaction.date).days),
            )
            if dated_match:
                return dated_match[0]
        for task in matching_group_tasks:
            return task
    return None


def classify_confidence(best_score: float) -> float:
    return round(min(best_score / 100.0, 0.99), 2)


def is_task_paid(task: ClickUpTask) -> bool:
    state = task.ap_state or task.status
    return normalize_key(state) == "paid" or math.isclose(task.remaining_balance, 0.0, abs_tol=0.01)


def evaluate_update(task: ClickUpTask, transaction: Transaction, as_of_date: date) -> Optional[Dict[str, Any]]:
    field_updates: Dict[str, Any] = {}
    notes_to_append: List[str] = []
    reasons: List[str] = []

    outstanding = task.remaining_balance or max(task.amount_due - task.amount_paid, 0.0)
    txn_amount = transaction.amount

    if transaction.reference and transaction.reference not in task.transaction_references:
        notes_to_append.append(
            f"Transaction {transaction.reference} posted {format_date(transaction.date)} for ${txn_amount:.2f} from {transaction.vendor_raw or transaction.vendor_name}."
        )

    current_state = task.ap_state or task.status
    if task.amount_due and txn_amount > task.amount_due + max(5.0, task.amount_due * 0.02):
        field_updates["amount_due"] = txn_amount
        field_updates["amount_paid"] = txn_amount
        field_updates["remaining_balance"] = 0.0
        field_updates["status"] = "Paid"
        reasons.append("transaction exceeds recorded obligation; amount due likely changed")
    elif outstanding and math.isclose(txn_amount, outstanding, abs_tol=max(1.0, outstanding * 0.02)):
        if normalize_key(current_state) != "paid":
            field_updates["amount_paid"] = round(task.amount_paid + txn_amount, 2)
            field_updates["remaining_balance"] = 0.0
            field_updates["status"] = "Paid"
            reasons.append("transaction clears remaining balance")
    elif task.amount_due and math.isclose(txn_amount, task.amount_due, abs_tol=max(1.0, task.amount_due * 0.02)):
        if not is_task_paid(task):
            field_updates["amount_paid"] = round(txn_amount, 2)
            field_updates["remaining_balance"] = 0.0
            field_updates["status"] = "Paid"
            reasons.append("full payment found but task not marked paid")
    elif task.amount_due and 0 < txn_amount < max(outstanding, task.amount_due):
        new_paid = round(task.amount_paid + txn_amount, 2)
        new_remaining = round(max(task.amount_due - new_paid, 0.0), 2)
        if new_remaining < task.remaining_balance or normalize_key(current_state) != "partially paid":
            field_updates["amount_paid"] = new_paid
            field_updates["remaining_balance"] = new_remaining
            field_updates["status"] = "Partially Paid"
            reasons.append("partial payment detected")
            notes_to_append.append(f"Partial payment recorded: ${txn_amount:.2f}. Remaining balance ${new_remaining:.2f}.")

    txn_date = transaction.date
    reference_date = task.expected_charge_date or task.due_date
    if txn_date and reference_date and abs((txn_date - reference_date).days) > 7 and task.frequency.lower() in {"monthly", "weekly"}:
        field_updates["expected_charge_date"] = txn_date.isoformat()
        reasons.append("recurring withdrawal date shifted")

    if not reasons and not notes_to_append and not field_updates:
        return None

    if task.last_reviewed_date != as_of_date:
        field_updates["last_reviewed_date"] = as_of_date.isoformat()

    return {
        "task": task,
        "recommended_change": ", ".join(reasons) if reasons else "append transaction reference and review date",
        "field_updates": field_updates,
        "notes_to_append": notes_to_append,
        "confidence": 0.93 if reasons else 0.88,
        "ap_state": field_updates.get("status", current_state),
    }


def determine_due_anchor(task: ClickUpTask) -> Optional[date]:
    return task.expected_charge_date or task.due_date


def find_matches(transactions: Sequence[Transaction], tasks: Sequence[ClickUpTask], rules: Dict[str, Any], as_of_date: date) -> Dict[str, Any]:
    create_tasks: List[Dict[str, Any]] = []
    update_tasks: List[Dict[str, Any]] = []
    grouped_rollups: List[Dict[str, Any]] = []
    exceptions: List[Dict[str, Any]] = []
    matched_transactions: Dict[str, str] = {}

    for transaction in transactions:
        group_task = choose_group_task(transaction, tasks, rules)
        candidates = [score_candidate(transaction, task) for task in tasks if not task.grouped_flag]
        candidates.sort(key=lambda item: item.score, reverse=True)
        best = candidates[0] if candidates else None
        second = candidates[1] if len(candidates) > 1 else None

        if group_task and (not best or best.score < 75 or normalize_key(best.task.vendor_name) != normalize_key(transaction.vendor_name)):
            transaction.disposition = "POSSIBLE_GROUPED_ITEM"
            transaction.matched_task_id = group_task.task_id
            transaction.matched_task_name = group_task.task_name
            transaction.confidence = 0.92
            transaction.reason = f"grouped vendor rule maps to {group_task.task_name}"
            grouped_rollups.append(
                {
                    "group_task_id": group_task.task_id,
                    "group_task_name": group_task.task_name,
                    "vendor_name": transaction.vendor_name,
                    "amount": transaction.amount,
                    "transaction_date": format_date(transaction.date),
                    "why_grouped": "vendor/category is configured for grouped AP handling",
                    "update_needed": f"Increase grouped total by ${transaction.amount:.2f} and append itemized sub-detail.",
                    "sub_detail_note": (
                        f"{format_date(transaction.date)} | {transaction.vendor_name} | ${transaction.amount:.2f} | "
                        f"ref {transaction.reference} | acct {transaction.account or 'unknown'}"
                    ),
                    "last_audit_result": "POSSIBLE_GROUPED_ITEM",
                    "confidence": transaction.confidence,
                }
            )
            continue

        if not best or best.score < 65:
            transaction.disposition = "MISSING_CREATE_NEW"
            transaction.confidence = 0.9 if not second or second.score < 45 else 0.72
            transaction.reason = "no sufficiently strong ClickUp match"
            create_tasks.append(build_create_task(transaction, rules, as_of_date))
            continue

        if second and abs(best.score - second.score) < 8 and best.score < 85:
            transaction.disposition = "UNCLEAR_REQUIRES_REVIEW"
            transaction.confidence = classify_confidence(best.score)
            transaction.reason = "multiple plausible ClickUp matches with similar confidence"
            exceptions.append(
                {
                    "vendor": transaction.vendor_name,
                    "amount": transaction.amount,
                    "date": format_date(transaction.date),
                    "possible_matches": [best.task.task_name, second.task.task_name],
                    "why_unclear": "top candidate scores are too close to auto-match safely",
                    "recommended_human_review_step": "Confirm the underlying invoice or memo and then attach the transaction to the correct task.",
                    "confidence": transaction.confidence,
                }
            )
            continue

        transaction.matched_task_id = best.task.task_id
        transaction.matched_task_name = best.task.task_name
        transaction.confidence = classify_confidence(best.score)
        matched_transactions[transaction.reference] = best.task.task_id

        update = evaluate_update(best.task, transaction, as_of_date)
        if update:
            transaction.disposition = "MATCHED_NEEDS_UPDATE"
            transaction.reason = update["recommended_change"]
            update_tasks.append(
                {
                    "task_id": best.task.task_id,
                    "current_task_name": best.task.task_name,
                    "vendor": best.task.vendor_name,
                    "current_status": best.task.status,
                    "current_ap_state": best.task.ap_state or best.task.status,
                    "recommended_change": update["recommended_change"],
                    "why": "; ".join(best.reasons),
                    "amount_changes": {
                        "current_amount_due": best.task.amount_due,
                        "current_amount_paid": best.task.amount_paid,
                        "current_remaining_balance": best.task.remaining_balance,
                        "field_updates": update["field_updates"],
                    },
                    "due_date_changes": {
                        "current_due_date": format_date(best.task.due_date),
                        "current_expected_charge_date": format_date(best.task.expected_charge_date),
                        "field_updates": {
                            key: value
                            for key, value in update["field_updates"].items()
                            if key in {"due_date", "expected_charge_date"}
                        },
                    },
                    "partial_payment_update": "Partial payment detected" if update["field_updates"].get("status") == "Partially Paid" else "",
                    "notes_to_append": update["notes_to_append"],
                    "source_transaction_reference": transaction.reference,
                    "recommended_ap_state": update["ap_state"],
                    "needs_human_review": False,
                    "last_audit_result": "MATCHED_NEEDS_UPDATE",
                    "confidence": update["confidence"],
                }
            )
        else:
            transaction.disposition = "MATCHED"
            transaction.reason = "; ".join(best.reasons)

    return {
        "create_tasks": create_tasks,
        "update_tasks": dedupe_task_updates(update_tasks),
        "grouped_rollups": grouped_rollups,
        "exceptions": exceptions,
        "matched_transactions": matched_transactions,
    }


def build_create_task(transaction: Transaction, rules: Dict[str, Any], as_of_date: date) -> Dict[str, Any]:
    due_date = transaction.date
    frequency = infer_frequency(transaction.vendor_name, "", rules)
    category = infer_category(transaction.vendor_name, transaction.category, rules)
    billing_period = due_date.strftime("%b %Y") if due_date else as_of_date.strftime("%b %Y")
    task_name = f"{category} | {transaction.vendor_name} | {billing_period if frequency != 'Ad Hoc' else 'Due ' + format_date(due_date)}"
    ap_state = "Due This Week" if due_date and abs((as_of_date - due_date).days) <= 7 else "Scheduled"
    return {
        "task_name": task_name,
        "vendor_name": transaction.vendor_name,
        "canonical_vendor": transaction.vendor_name,
        "category": category,
        "amount_due": transaction.amount,
        "amount_paid": 0.0,
        "remaining_balance": transaction.amount,
        "due_date": due_date.isoformat() if due_date else "",
        "expected_charge_date": due_date.isoformat() if due_date else "",
        "frequency": frequency,
        "status": ap_state,
        "ap_state": ap_state,
        "payment_method": transaction.account,
        "grouped_flag": False,
        "group_name": "",
        "task_class": "Standalone AP",
        "notes": (
            f"Created from unmatched transaction {transaction.reference}. "
            f"Raw merchant: {transaction.vendor_raw}. Memo: {transaction.memo or 'n/a'}."
        ),
        "source_transaction_reference": transaction.reference,
        "last_reviewed_date": as_of_date.isoformat(),
        "cashflow_priority": infer_cashflow_priority(transaction.amount, due_date, as_of_date),
        "slack_warning_tier": warning_level((due_date - as_of_date).days, amount_due=transaction.amount, material_amount=rules.get("material_warning_amount", MATERIAL_WARNING_AMOUNT)) if due_date else "LOW",
        "last_audit_result": "MISSING_CREATE_NEW",
        "needs_human_review": True,
        "reason_for_creation": "Transaction has no strong ClickUp AP match and no grouped rollup rule.",
        "confidence": 0.9,
    }


def dedupe_task_updates(updates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for update in updates:
        existing = deduped.get(update["task_id"])
        if not existing:
            deduped[update["task_id"]] = update
            continue
        existing["notes_to_append"] = list(dict.fromkeys(existing["notes_to_append"] + update["notes_to_append"]))
        existing["amount_changes"]["field_updates"].update(update["amount_changes"]["field_updates"])
        existing["due_date_changes"]["field_updates"].update(update["due_date_changes"]["field_updates"])
        if update["source_transaction_reference"] not in normalize_spaces(existing["source_transaction_reference"]).split(", "):
            existing["source_transaction_reference"] += f", {update['source_transaction_reference']}"
        existing["confidence"] = max(existing["confidence"], update["confidence"])
        existing["recommended_change"] = normalize_spaces(
            existing["recommended_change"] + ", " + update["recommended_change"]
        )
    return list(deduped.values())


def infer_cashflow_priority(amount: float, due_date: Optional[date], as_of_date: date) -> str:
    if due_date and due_date < as_of_date:
        return "Critical"
    if amount >= 1000:
        return "High"
    if amount >= 250:
        return "Medium"
    return "Low"


def format_date(value: Optional[date]) -> str:
    return value.isoformat() if value else ""


def overdue_reviews(tasks: Sequence[ClickUpTask], transactions: Sequence[Transaction], matched_map: Dict[str, str], as_of_date: date) -> List[Dict[str, Any]]:
    reviews: List[Dict[str, Any]] = []
    for task in tasks:
        anchor = determine_due_anchor(task)
        overdue = (anchor and anchor < as_of_date and task.remaining_balance > 0.01) or "overdue" in normalize_key(task.status)
        if not overdue:
            continue

        possible = []
        for transaction in transactions:
            candidate = score_candidate(transaction, task)
            if candidate.score >= 55:
                possible.append((candidate, transaction))
        possible.sort(key=lambda item: item[0].score, reverse=True)
        best_pair = possible[0] if possible else None

        amount_paid_known = task.amount_paid
        remaining = task.remaining_balance
        match_status = "no"
        explanation = "No matching transaction this cycle; could be unpaid, paid manually, shifted, or no longer due."
        next_action = "Confirm vendor statement or bank activity, then update ClickUp status or remove the task if obsolete."
        confidence = 0.72

        if best_pair and best_pair[0].score >= 70:
            candidate, transaction = best_pair
            match_status = "yes"
            if transaction.amount >= remaining - max(1.0, remaining * 0.02):
                explanation = "Matching payment appears posted; ClickUp likely was not updated after payment."
                next_action = "Mark Paid, append transaction reference, and close the overdue item."
                confidence = 0.93
            elif 0 < transaction.amount < remaining:
                explanation = "Only a partial payment appears posted; overdue balance is still active."
                next_action = "Update amount paid, keep remaining balance visible, and maintain warning coverage."
                confidence = 0.94
            amount_paid_known = round(task.amount_paid + transaction.amount, 2)
            remaining = round(max(task.amount_due - amount_paid_known, 0.0), 2)
        elif best_pair:
            match_status = "unclear"
            explanation = "A possible matching transaction exists, but confidence is too low to auto-resolve."
            next_action = "Validate the invoice, memo, and payment source before changing status."
            confidence = 0.61

        reviews.append(
            {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "vendor": task.vendor_name,
                "original_due_date": format_date(task.due_date),
                "amount_due": task.amount_due,
                "amount_paid": amount_paid_known,
                "remaining_balance": remaining,
                "matching_transaction_found": match_status,
                "most_likely_explanation": explanation,
                "recommended_next_action": next_action,
                "confidence": confidence,
            }
        )
    return reviews


def build_slack_warnings(
    tasks: Sequence[ClickUpTask],
    updates: Sequence[Dict[str, Any]],
    creates: Sequence[Dict[str, Any]],
    as_of_date: date,
    mode: str,
    material_amount: float,
) -> List[Dict[str, Any]]:
    updated_by_id = {update["task_id"]: update for update in updates}
    warnings: List[Dict[str, Any]] = []

    for task in tasks:
        outstanding = task.remaining_balance
        if task.task_id in updated_by_id:
            outstanding = updated_by_id[task.task_id]["amount_changes"]["field_updates"].get("remaining_balance", outstanding)
        if outstanding <= 0.01:
            continue
        anchor = determine_due_anchor(task)
        if not anchor:
            continue
        delta = (anchor - as_of_date).days
        if mode == "daily":
            is_partial = bool(updated_by_id.get(task.task_id, {}).get("partial_payment_update")) or normalize_key(task.ap_state or task.status) == "partially paid"
            if delta > 5 and delta >= 0 and not is_partial and outstanding < material_amount:
                continue
        elif delta > 10 and not task.grouped_flag:
            continue
        level = warning_level(delta, amount_due=task.amount_due, material_amount=material_amount)
        ap_state = updated_by_id.get(task.task_id, {}).get("recommended_ap_state", task.ap_state or task.status)
        action = "Review" if "overdue" in normalize_key(ap_state) or delta < 0 else "Pay"
        if updated_by_id.get(task.task_id, {}).get("partial_payment_update") or normalize_key(ap_state) == "partially paid":
            action = "Confirm"
        warning = warning_record(
            level=level,
            vendor=task.vendor_name,
            amount_due=task.amount_due,
            due_date=anchor,
            status=ap_state,
            remaining_balance=outstanding,
            category=task.category,
            priority=task.cashflow_priority or infer_cashflow_priority(task.amount_due, anchor, as_of_date),
            grouped=task.grouped_flag,
            action=action,
            notes=build_warning_note(task, updated_by_id.get(task.task_id)),
            task_name=task.task_name,
            ap_state=ap_state,
        )
        warnings.append(warning)

    for create in creates:
        due_date = parse_date(create["due_date"])
        if not due_date:
            continue
        delta = (due_date - as_of_date).days
        if mode == "daily" and delta > 5 and create["amount_due"] < material_amount:
            continue
        if mode != "daily" and delta > 10:
            continue
        warnings.append(
            warning_record(
                level=warning_level(delta, amount_due=create["amount_due"], material_amount=material_amount),
                vendor=create["vendor_name"],
                amount_due=create["amount_due"],
                due_date=due_date,
                status=create["status"],
                remaining_balance=create["remaining_balance"],
                category=create["category"],
                priority=create["cashflow_priority"],
                grouped=create["grouped_flag"],
                action="Create",
                notes="New obligation found from transaction feed; create ClickUp task before next cash review.",
                task_name=create["task_name"],
                ap_state=create.get("ap_state", create["status"]),
            )
        )
    warnings.sort(key=lambda item: (warning_rank(item["level"]), item["due_date"] or ""))
    return warnings


def build_warning_note(task: ClickUpTask, update: Optional[Dict[str, Any]]) -> str:
    if update and update.get("partial_payment_update"):
        return "Partial payment recorded; remaining balance still needs cash planning."
    if task.grouped_flag:
        return "Grouped AP rollup should stay itemized in task notes."
    if "overdue" in normalize_key(task.status):
        return "Past due item still open in ClickUp."
    return "Upcoming obligation requires active AP review."


def warning_level(delta_days: int, *, amount_due: float, material_amount: float) -> str:
    if delta_days <= 2:
        return "CRITICAL"
    if amount_due >= material_amount and delta_days <= 5:
        return "CRITICAL"
    if delta_days <= 5:
        return "HIGH"
    if delta_days <= 10:
        return "MEDIUM"
    return "LOW"


def warning_rank(level: str) -> int:
    return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}.get(level, 4)


def warning_record(
    *,
    level: str,
    vendor: str,
    amount_due: float,
    due_date: Optional[date],
    status: str,
    remaining_balance: float,
    category: str,
    priority: str,
    grouped: bool,
    action: str,
    notes: str,
    task_name: str,
    ap_state: str,
) -> Dict[str, Any]:
    due_text = format_date(due_date)
    message = (
        f"[{level}] {vendor} - ${amount_due:.2f} due on {due_text}. Status: {status}. "
        f"Remaining balance: ${remaining_balance:.2f}. Category: {category}. Action: {action}. Notes: {notes}"
    )
    return {
        "level": level,
        "message": message,
        "vendor": vendor,
        "amount_due": amount_due,
        "due_date": due_text,
        "status": status,
        "remaining_balance": remaining_balance,
        "category": category,
        "priority": priority,
        "grouped": grouped,
        "action": action,
        "notes": notes,
        "task_name": task_name,
        "ap_state": ap_state,
    }


def improvement_notes(
    transactions: Sequence[Transaction],
    tasks: Sequence[ClickUpTask],
    exceptions: Sequence[Dict[str, Any]],
    rules: Dict[str, Any],
) -> List[str]:
    notes: List[str] = []
    alias_candidates = {}
    for transaction in transactions:
        if normalize_key(transaction.vendor_raw) != normalize_key(transaction.vendor_name):
            alias_candidates[transaction.vendor_raw] = transaction.vendor_name
    for raw_vendor, canonical in sorted(alias_candidates.items()):
        notes.append(f"Add vendor alias mapping: {raw_vendor} -> {canonical}.")
    if any("AMZN" in item["vendor"] or "Amazon" in item["vendor"] for item in exceptions):
        notes.append("Split AMZN aliases into AWS vs Amazon Marketplace using memo text or card source to reduce false positives.")
    if any(task.grouped_flag and not task.notes for task in tasks):
        notes.append("Require itemized notes on grouped ClickUp tasks so software rollups remain auditable.")
    if any(not task.last_reviewed_date for task in tasks):
        notes.append("Backfill ClickUp last_reviewed_date on AP tasks to support clean weekly exception aging.")
    if any(not task.payment_method for task in tasks):
        notes.append("Require payment_method on ClickUp AP tasks to improve bank-vs-card reconciliation.")
    notes.append("Automate transaction reference appends when a payment closes an AP task to reduce repeated overdue false positives.")
    return notes


def build_weekly_summary(
    *,
    transactions: Sequence[Transaction],
    tasks: Sequence[ClickUpTask],
    creates: Sequence[Dict[str, Any]],
    updates: Sequence[Dict[str, Any]],
    grouped: Sequence[Dict[str, Any]],
    overdue: Sequence[Dict[str, Any]],
    warnings: Sequence[Dict[str, Any]],
    exceptions: Sequence[Dict[str, Any]],
    new_charge_alerts: Sequence[Dict[str, Any]],
    as_of_date: date,
) -> Dict[str, Any]:
    partial_task_balances = {
        task.task_id: task.remaining_balance
        for task in tasks
        if normalize_key(task.ap_state or task.status) == "partially paid" and task.remaining_balance > 0
    }
    for update in updates:
        if update["partial_payment_update"]:
            partial_task_balances[update["task_id"]] = update["amount_changes"]["field_updates"].get("remaining_balance", update["amount_changes"]["current_remaining_balance"])
    return {
        "as_of_date": as_of_date.isoformat(),
        "transactions_reviewed": len(transactions),
        "new_items_to_create": len(creates),
        "existing_items_to_update": len(updates),
        "grouped_rollups": len(grouped),
        "overdue_review_queue": len(overdue),
        "warnings": len(warnings),
        "exceptions": len(exceptions),
        "new_charge_alerts": len(new_charge_alerts),
        "material_new_charge_alerts": sum(1 for item in new_charge_alerts if item["material"]),
        "unknown_new_charges": sum(1 for item in new_charge_alerts if item["alert_type"] == "UNKNOWN_REQUIRES_REVIEW"),
        "total_due_this_week": round(
            sum(item["remaining_balance"] for item in warnings if parse_date(item["due_date"]) and 0 <= (parse_date(item["due_date"]) - as_of_date).days <= 7),
            2,
        ),
        "total_overdue": round(sum(item["remaining_balance"] for item in overdue if item["remaining_balance"] > 0), 2),
        "total_partial_balances": round(sum(partial_task_balances.values()), 2),
    }


def build_leadership_summary(
    *,
    weekly_summary: Dict[str, Any],
    warnings: Sequence[Dict[str, Any]],
    overdue: Sequence[Dict[str, Any]],
    new_charge_alerts: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    top_cash_items = sorted(warnings, key=lambda item: item["remaining_balance"], reverse=True)[:10]
    return {
        "as_of_date": weekly_summary["as_of_date"],
        "total_due_this_week": weekly_summary["total_due_this_week"],
        "total_overdue": weekly_summary["total_overdue"],
        "total_partial_balances": weekly_summary["total_partial_balances"],
        "material_new_charges": [item for item in new_charge_alerts if item["material"]][:10],
        "critical_items": [
            {
                "vendor": item["vendor"],
                "amount_due": item["amount_due"],
                "remaining_balance": item["remaining_balance"],
                "due_date": item["due_date"],
                "action": item["action"],
                "level": item["level"],
            }
            for item in top_cash_items
            if item["level"] in {"CRITICAL", "HIGH"}
        ],
        "overdue_exposure": overdue,
    }


def build_bookkeeper_action_queue(
    creates: Sequence[Dict[str, Any]],
    updates: Sequence[Dict[str, Any]],
    grouped: Sequence[Dict[str, Any]],
    overdue: Sequence[Dict[str, Any]],
    exceptions: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    queue = []
    for item in creates:
        queue.append(
            {
                "queue_type": "create_task",
                "task_name": item["task_name"],
                "vendor": item["vendor_name"],
                "amount_due": item["amount_due"],
                "due_date": item["due_date"] or item["expected_charge_date"],
                "action": "Create ClickUp task",
                "source_transaction_reference": item["source_transaction_reference"],
                "notes": item["notes"],
            }
        )
    for item in updates:
        queue.append(
            {
                "queue_type": "update_task",
                "task_id": item["task_id"],
                "task_name": item["current_task_name"],
                "vendor": item["vendor"],
                "action": item["recommended_change"],
                "source_transaction_reference": item["source_transaction_reference"],
                "field_updates": item["amount_changes"]["field_updates"],
                "notes": item["notes_to_append"],
            }
        )
    for item in grouped:
        queue.append(
            {
                "queue_type": "grouped_rollup",
                "task_id": item["group_task_id"],
                "task_name": item["group_task_name"],
                "vendor": item["vendor_name"],
                "action": item["update_needed"],
                "source_transaction_reference": item["sub_detail_note"],
                "notes": item["sub_detail_note"],
            }
        )
    for item in overdue:
        queue.append(
            {
                "queue_type": "overdue_review",
                "task_id": item["task_id"],
                "task_name": item["task_name"],
                "vendor": item["vendor"],
                "action": item["recommended_next_action"],
                "source_transaction_reference": "",
                "notes": item["most_likely_explanation"],
            }
        )
    for item in exceptions:
        queue.append(
            {
                "queue_type": "exception_review",
                "task_name": item["possible_matches"][0] if item["possible_matches"] else item["vendor"],
                "vendor": item["vendor"],
                "action": item["recommended_human_review_step"],
                "source_transaction_reference": "",
                "notes": item["why_unclear"],
            }
        )
    return queue


def build_new_charge_alerts(
    *,
    transactions: Sequence[Transaction],
    tasks: Sequence[ClickUpTask],
    creates: Sequence[Dict[str, Any]],
    exceptions: Sequence[Dict[str, Any]],
    material_amount: float,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    task_vendor_keys = {normalize_key(task.vendor_name) for task in tasks}
    transaction_by_reference = {transaction.reference: transaction for transaction in transactions}

    for item in creates:
        vendor_key = normalize_key(item["vendor_name"])
        transaction = transaction_by_reference.get(item["source_transaction_reference"])
        if vendor_key in task_vendor_keys:
            alert_type = "NEW_CHARGE_PATTERN" if item["frequency"] == "Ad Hoc" else "NEW_UNMAPPED_RECURRING_RISK"
            why_new = "Vendor exists in AP, but this transaction did not reconcile to an active ClickUp item."
            existing_match = "unclear"
        else:
            alert_type = "NEW_VENDOR"
            why_new = "Vendor does not appear in the current AP dashboard."
            existing_match = "no"
        alerts.append(
            {
                "vendor": item["vendor_name"],
                "amount": item["amount_due"],
                "date": item["due_date"] or item["expected_charge_date"],
                "alert_type": alert_type,
                "why_new": why_new,
                "possible_classification": item["category"],
                "existing_match_found": existing_match,
                "recommended_next_action": "Review the charge, confirm whether it should recur, and add or map the AP record before close.",
                "confidence": item["confidence"],
                "material": item["amount_due"] >= material_amount,
                "source_transaction_reference": item["source_transaction_reference"],
                "memo": transaction.memo if transaction else "",
            }
        )

    for item in exceptions:
        alerts.append(
            {
                "vendor": item["vendor"],
                "amount": item["amount"],
                "date": item["date"],
                "alert_type": "UNKNOWN_REQUIRES_REVIEW",
                "why_new": item["why_unclear"],
                "possible_classification": "Unknown",
                "existing_match_found": "unclear",
                "recommended_next_action": item["recommended_human_review_step"],
                "confidence": item["confidence"],
                "material": item["amount"] >= material_amount,
                "source_transaction_reference": "",
                "memo": "",
            }
        )

    alerts.sort(key=lambda item: (not item["material"], -item["amount"], item["vendor"]))
    return alerts


def slim_daily_slack_warnings(
    warnings: Sequence[Dict[str, Any]],
    *,
    as_of_date: date,
) -> List[Dict[str, Any]]:
    urgent: List[Dict[str, Any]] = []
    for item in warnings:
        due_date = parse_date(item["due_date"])
        delta = (due_date - as_of_date).days if due_date else 999
        ap_state_key = normalize_key(item.get("ap_state", item.get("status", "")))
        is_overdue = (due_date and due_date < as_of_date) or "overdue" in ap_state_key
        is_partial = ap_state_key == "partially paid"
        is_material_new = item["action"] == "Create" and item["level"] in {"CRITICAL", "HIGH"}
        if is_overdue or delta <= 2 or (is_partial and delta <= 7) or is_material_new:
            urgent.append(item)
    urgent.sort(key=lambda item: (warning_rank(item["level"]), item["due_date"] or "", -item["remaining_balance"]))
    return urgent[:12]


def build_slack_payload(
    *,
    mode: str,
    weekly_summary: Dict[str, Any],
    leadership_summary: Dict[str, Any],
    warnings: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    if mode == "daily":
        as_of = parse_date(weekly_summary["as_of_date"])
        slim_items = slim_daily_slack_warnings(warnings, as_of_date=as_of) if as_of else list(warnings)
        buckets = {
            "overdue review needed": [item for item in slim_items if parse_date(item["due_date"]) and parse_date(item["due_date"]) < parse_date(weekly_summary["as_of_date"])],
            "due in 1-2 days": [item for item in slim_items if parse_date(item["due_date"]) and 0 <= (parse_date(item["due_date"]) - parse_date(weekly_summary["as_of_date"])).days <= 2],
            "partial balances still open": [item for item in slim_items if normalize_key(item.get("ap_state", "")) == "partially paid"],
            "material new obligations found by audit": [item for item in slim_items if item["action"] == "Create" and item["level"] in {"CRITICAL", "HIGH"}],
        }
        sections = [
            {
                "title": title,
                "items": [
                    {
                        "vendor": item["vendor"],
                        "amount_due": item["amount_due"],
                        "remaining_balance": item["remaining_balance"],
                        "due_date": item["due_date"],
                        "message": item["message"],
                    }
                    for item in items
                ],
            }
            for title, items in buckets.items()
            if items
        ]
        text = "\n".join(item["message"] for item in slim_items) if slim_items else "No urgent AP items today."
        return {"mode": mode, "text": text, "sections": sections}

    summary_line = (
        f"Weekly AP Review for {weekly_summary['as_of_date']}: "
        f"${weekly_summary['total_due_this_week']:.2f} due this week, "
        f"${weekly_summary['total_overdue']:.2f} overdue, "
        f"${weekly_summary['total_partial_balances']:.2f} partial balances."
    )
    return {
        "mode": mode,
        "text": summary_line,
        "summary": weekly_summary,
        "leadership_summary": leadership_summary,
        "top_items": leadership_summary["critical_items"],
    }


def post_slack_payload(webhook_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return request_json("POST", webhook_url, body=payload, extra_headers={"Content-Type": "application/json"})


def render_report(
    *,
    mode: str,
    transactions: Sequence[Transaction],
    tasks: Sequence[ClickUpTask],
    creates: Sequence[Dict[str, Any]],
    updates: Sequence[Dict[str, Any]],
    grouped: Sequence[Dict[str, Any]],
    overdue: Sequence[Dict[str, Any]],
    warnings: Sequence[Dict[str, Any]],
    exceptions: Sequence[Dict[str, Any]],
    new_charge_alerts: Sequence[Dict[str, Any]],
    improvements: Sequence[str],
    weekly_summary: Dict[str, Any],
    leadership_summary: Dict[str, Any],
    bookkeeper_action_queue: Sequence[Dict[str, Any]],
    as_of_date: date,
) -> str:
    matched_count = sum(1 for transaction in transactions if transaction.disposition == "MATCHED")
    missing_count = sum(1 for transaction in transactions if transaction.disposition == "MISSING_CREATE_NEW")
    updated_count = sum(1 for transaction in transactions if transaction.disposition == "MATCHED_NEEDS_UPDATE")
    grouped_count = sum(1 for transaction in transactions if transaction.disposition == "POSSIBLE_GROUPED_ITEM")
    unclear_count = sum(1 for transaction in transactions if transaction.disposition == "UNCLEAR_REQUIRES_REVIEW")
    total_upcoming = round(sum(item["remaining_balance"] for item in warnings if parse_date(item["due_date"]) and parse_date(item["due_date"]) >= as_of_date), 2)
    total_overdue = round(sum(item["remaining_balance"] for item in overdue if item["remaining_balance"] > 0), 2)
    total_partial_remaining = weekly_summary["total_partial_balances"]

    lines = [
        "SECTION 1: EXECUTIVE SUMMARY",
        f"- total transactions reviewed: {len(transactions)}",
        f"- matched count: {matched_count}",
        f"- missing count: {missing_count}",
        f"- updated count: {updated_count}",
        f"- grouped count: {grouped_count}",
        f"- overdue review count: {len(overdue)}",
        f"- upcoming warning count: {len(warnings)}",
        f"- unclear review count: {unclear_count}",
        f"- new charge alerts: {weekly_summary['new_charge_alerts']}",
        f"- material new charges: {weekly_summary['material_new_charge_alerts']}",
        f"- unknown new charges requiring review: {weekly_summary['unknown_new_charges']}",
        f"- total upcoming obligations: ${total_upcoming:.2f}",
        f"- total overdue obligations: ${total_overdue:.2f}",
        f"- total partially paid remaining balances: ${total_partial_remaining:.2f}",
        "",
        "SECTION 2: NEW ITEMS TO CREATE",
    ]
    if creates:
        for item in creates:
            lines.extend(
                [
                    f"- Vendor: {item['vendor_name']}",
                    f"  Suggested Task Name: {item['task_name']}",
                    f"  Amount: ${item['amount_due']:.2f}",
                    f"  Due Date or Expected Charge Date: {item['due_date'] or item['expected_charge_date']}",
                    f"  Category: {item['category']}",
                    f"  Frequency: {item['frequency']}",
                    f"  Standalone or Grouped: {'Grouped' if item['grouped_flag'] else 'Standalone'}",
                    f"  Reason for Creation: {item['reason_for_creation']}",
                    f"  Confidence Level: {item['confidence']:.2f}",
                    f"  Suggested Notes: {item['notes']}",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "SECTION 3: EXISTING CLICKUP ITEMS TO UPDATE"])
    if updates:
        for item in updates:
            lines.extend(
                [
                    f"- Current Task Name: {item['current_task_name']}",
                    f"  Vendor: {item['vendor']}",
                    f"  Current Status: {item['current_status']}",
                    f"  Recommended Change: {item['recommended_change']}",
                    f"  Why it needs updating: {item['why']}",
                    f"  Amount changes: {json.dumps(item['amount_changes'], sort_keys=True)}",
                    f"  Due date changes: {json.dumps(item['due_date_changes'], sort_keys=True)}",
                    f"  Partial payment update if applicable: {item['partial_payment_update'] or 'None'}",
                    f"  Notes to append: {' | '.join(item['notes_to_append']) if item['notes_to_append'] else 'None'}",
                    f"  Confidence Level: {item['confidence']:.2f}",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "SECTION 4: GROUPED ITEMS TO ROLL INTO EXISTING TASKS"])
    if grouped:
        for item in grouped:
            lines.extend(
                [
                    f"- Vendor: {item['vendor_name']}",
                    f"  Group Name: {item['group_task_name']}",
                    f"  Amount: ${item['amount']:.2f}",
                    f"  Transaction Date: {item['transaction_date']}",
                    f"  Why it belongs in the group: {item['why_grouped']}",
                    f"  Update needed to the grouped ClickUp task: {item['update_needed']}",
                    f"  Sub-detail note to append: {item['sub_detail_note']}",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "SECTION 5: OVERDUE TASK REVIEW QUEUE"])
    if overdue:
        for item in overdue:
            lines.extend(
                [
                    f"- Task Name: {item['task_name']}",
                    f"  Vendor: {item['vendor']}",
                    f"  Original Due Date: {item['original_due_date']}",
                    f"  Amount Due: ${item['amount_due']:.2f}",
                    f"  Amount Paid if known: ${item['amount_paid']:.2f}",
                    f"  Remaining Balance: ${item['remaining_balance']:.2f}",
                    f"  Matching transaction found?: {item['matching_transaction_found']}",
                    f"  Most likely explanation: {item['most_likely_explanation']}",
                    f"  Recommended next action: {item['recommended_next_action']}",
                    f"  Confidence Level: {item['confidence']:.2f}",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "SECTION 6: SLACK WARNINGS"])
    if warnings:
        lines.extend(f"- {item['message']}" for item in warnings)
    else:
        lines.append("- None.")

    lines.extend(["", "SECTION 7: REVIEW EXCEPTIONS"])
    if exceptions:
        for item in exceptions:
            lines.extend(
                [
                    f"- Vendor: {item['vendor']}",
                    f"  Amount: ${item['amount']:.2f}",
                    f"  Date: {item['date']}",
                    f"  Possible matches: {', '.join(item['possible_matches'])}",
                    f"  Why unclear: {item['why_unclear']}",
                    f"  Recommended human review step: {item['recommended_human_review_step']}",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "SECTION 8: NORMALIZATION / SYSTEM IMPROVEMENT NOTES"])
    lines.extend(f"- {note}" for note in improvements)

    if mode == "weekly":
        lines.extend(
            [
                "",
                "SECTION 9: LEADERSHIP SUMMARY",
                f"- as_of_date: {leadership_summary['as_of_date']}",
                f"- total_due_this_week: ${leadership_summary['total_due_this_week']:.2f}",
                f"- total_overdue: ${leadership_summary['total_overdue']:.2f}",
                f"- total_partial_balances: ${leadership_summary['total_partial_balances']:.2f}",
            ]
        )
        for item in leadership_summary["critical_items"][:10]:
            lines.append(
                f"- {item['vendor']} | due {item['due_date']} | remaining ${item['remaining_balance']:.2f} | action {item['action']} | level {item['level']}"
            )
        for item in leadership_summary["material_new_charges"][:10]:
            lines.append(
                f"- material new charge | {item['vendor']} | ${item['amount']:.2f} | {item['date']} | {item['alert_type']}"
            )
        lines.extend(["", "SECTION 10: BOOKKEEPER ACTION QUEUE"])
        for item in bookkeeper_action_queue:
            lines.append(
                f"- {item['queue_type']} | {item.get('task_name', '')} | {item.get('vendor', '')} | {item['action']} | {item.get('source_transaction_reference', '')}"
            )

    return "\n".join(lines)


def build_payload(
    creates: Sequence[Dict[str, Any]],
    updates: Sequence[Dict[str, Any]],
    grouped: Sequence[Dict[str, Any]],
    overdue: Sequence[Dict[str, Any]],
    warnings: Sequence[Dict[str, Any]],
    exceptions: Sequence[Dict[str, Any]],
    new_charge_alerts: Sequence[Dict[str, Any]],
    weekly_summary: Dict[str, Any],
    leadership_summary: Dict[str, Any],
    bookkeeper_action_queue: Sequence[Dict[str, Any]],
    schema_summary: Dict[str, Any],
    clickup_actions: Dict[str, Any],
    slack_payload: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "create_tasks": list(creates),
        "update_tasks": list(updates),
        "grouped_rollups": list(grouped),
        "overdue_reviews": list(overdue),
        "slack_warnings": list(warnings),
        "exceptions": list(exceptions),
        "new_charge_alerts": list(new_charge_alerts),
        "weekly_summary": weekly_summary,
        "leadership_summary": leadership_summary,
        "bookkeeper_action_queue": list(bookkeeper_action_queue),
        "schema_summary": schema_summary,
        "clickup_update_actions": clickup_actions,
        "slack_payload": slack_payload,
    }


def main() -> None:
    args = parse_args()
    transactions_path, clickup_path, rules_path = resolve_input_paths(args)
    rules = load_rules(rules_path)
    automation_config = load_automation_config()
    schema_manifest = load_schema_manifest()
    material_amount = rules.get(
        "material_warning_amount",
        automation_config.get("material_warning_amount", MATERIAL_WARNING_AMOUNT),
    )
    as_of_date = parse_date(args.as_of_date)
    if not as_of_date:
        raise SystemExit("Invalid --as-of-date. Use YYYY-MM-DD.")

    transactions = normalize_transactions(load_rows(transactions_path), rules)
    window_start = as_of_date.fromordinal(as_of_date.toordinal() - max(args.lookback_days - 1, 0))
    transactions = [
        transaction
        for transaction in transactions
        if not transaction.date or window_start <= transaction.date <= as_of_date
    ]
    clickup_token = args.clickup_token or os.getenv("CLICKUP_API_TOKEN")
    clickup_list_id = args.clickup_list_id or os.getenv("CLICKUP_LIST_ID")
    clickup_view_id = args.clickup_view_id or os.getenv("CLICKUP_VIEW_ID")
    schema_fields: List[Dict[str, Any]] = []
    if clickup_path:
        task_rows = load_rows(clickup_path)
    elif clickup_token and (clickup_list_id or clickup_view_id):
        task_rows = fetch_clickup_tasks(clickup_token, clickup_list_id, clickup_view_id)
        if clickup_list_id:
            schema_fields = fetch_clickup_custom_fields(clickup_token, clickup_list_id)
    else:
        raise SystemExit(
            "No ClickUp source found. Provide --clickup, or set CLICKUP_API_TOKEN plus CLICKUP_LIST_ID or CLICKUP_VIEW_ID."
        )
    tasks = normalize_tasks(task_rows, rules)
    match_result = find_matches(transactions, tasks, rules, as_of_date)
    overdue = overdue_reviews(tasks, transactions, match_result["matched_transactions"], as_of_date)
    warnings = build_slack_warnings(
        tasks,
        match_result["update_tasks"],
        match_result["create_tasks"],
        as_of_date,
        args.mode,
        material_amount,
    )
    new_charge_alerts = build_new_charge_alerts(
        transactions=transactions,
        tasks=tasks,
        creates=match_result["create_tasks"],
        exceptions=match_result["exceptions"],
        material_amount=material_amount,
    )
    schema_summary = inspect_clickup_schema(schema_fields, schema_manifest) if schema_fields and schema_manifest else {}
    clickup_actions = build_clickup_update_actions(tasks, match_result["update_tasks"], match_result["grouped_rollups"], schema_fields, as_of_date) if schema_fields else {"actions": [], "skipped": []}
    weekly_summary = build_weekly_summary(
        transactions=transactions,
        tasks=tasks,
        creates=match_result["create_tasks"],
        updates=match_result["update_tasks"],
        grouped=match_result["grouped_rollups"],
        overdue=overdue,
        warnings=warnings,
        exceptions=match_result["exceptions"],
        new_charge_alerts=new_charge_alerts,
        as_of_date=as_of_date,
    )
    leadership_summary = build_leadership_summary(
        weekly_summary=weekly_summary,
        warnings=warnings,
        overdue=overdue,
        new_charge_alerts=new_charge_alerts,
    )
    bookkeeper_action_queue = build_bookkeeper_action_queue(
        match_result["create_tasks"],
        match_result["update_tasks"],
        match_result["grouped_rollups"],
        overdue,
        match_result["exceptions"],
    )
    slack_payload = build_slack_payload(
        mode=args.mode,
        weekly_summary=weekly_summary,
        leadership_summary=leadership_summary,
        warnings=warnings,
    )
    improvements = improvement_notes(transactions, tasks, match_result["exceptions"], rules)
    if schema_summary.get("missing_field_count"):
        improvements.append(
            "Add the missing ClickUp AP custom fields from config/clickup_ap_schema.json before enabling low-risk auto-updates in production."
        )
    report = render_report(
        mode=args.mode,
        transactions=transactions,
        tasks=tasks,
        creates=match_result["create_tasks"],
        updates=match_result["update_tasks"],
        grouped=match_result["grouped_rollups"],
        overdue=overdue,
        warnings=warnings,
        exceptions=match_result["exceptions"],
        new_charge_alerts=new_charge_alerts,
        improvements=improvements,
        weekly_summary=weekly_summary,
        leadership_summary=leadership_summary,
        bookkeeper_action_queue=bookkeeper_action_queue,
        as_of_date=as_of_date,
    )
    payload = build_payload(
        match_result["create_tasks"],
        match_result["update_tasks"],
        match_result["grouped_rollups"],
        overdue,
        warnings,
        match_result["exceptions"],
        new_charge_alerts,
        weekly_summary,
        leadership_summary,
        bookkeeper_action_queue,
        schema_summary,
        clickup_actions,
        slack_payload,
    )
    if args.apply_clickup_updates:
        if not clickup_token or not clickup_list_id:
            raise SystemExit("--apply-clickup-updates requires live ClickUp auth with CLICKUP_API_TOKEN and CLICKUP_LIST_ID.")
        field_index = index_clickup_fields(schema_fields)
        payload["clickup_applied_actions"] = apply_clickup_actions(clickup_token, field_index, clickup_actions["actions"])
    if args.post_slack:
        webhook_url = os.getenv(SLACK_WEBHOOK_ENV)
        if not webhook_url:
            raise SystemExit(f"--post-slack requires {SLACK_WEBHOOK_ENV}.")
        payload["slack_post_result"] = post_slack_payload(webhook_url, slack_payload)
    print(report)
    print("\nMACHINE_ACTION_PAYLOAD")
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.payload_out:
        Path(args.payload_out).write_text(json.dumps(payload, indent=2, sort_keys=True))
    if args.report_out:
        Path(args.report_out).write_text(report)
    if args.schema_report_out:
        Path(args.schema_report_out).write_text(json.dumps(schema_summary, indent=2, sort_keys=True))
    if args.slack_payload_out:
        Path(args.slack_payload_out).write_text(json.dumps(slack_payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
