"""Map engine results to clearing_backlog__service_work row dicts.

Schema aligned with company_exploration.tickets_agent__agent_work.
DB create/insert lives in the DAG / notebook.
"""

from __future__ import annotations

import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json
import re
from datetime import datetime
from typing import Any

from config import MTO_ROUTE_CASE1, MTO_ROUTE_CASE2
from services.classifier import classify_product

ENGINE_MODEL = "tickets-engine"

RMT_ROUTE_BY_RULE = {
    1: "remittance/REMIT_DELIVERED",
    2: "remittance/REMIT_REFUND",
    3: "remittance/RESOLVE",
    4: "remittance/RESOLVE",
    5: "remittance/REMIT_REFUND",
    6: "remittance/REMIT_REFUND",
    # MTO email — n8n polls mto/email/wait/* ; then UPDATE route to mto/email/sent/*.
    7: MTO_ROUTE_CASE1,
    8: MTO_ROUTE_CASE1,
    9: MTO_ROUTE_CASE2,
}

# Cards routes are keyed by CRM flow (CategoryLevel2_c), not a single cash-in prefix.
CARDS_FLOW_ROUTE_PREFIX = {
    "Transaction Dispute": "card_dispute",
    "Incorrect Balance": "card_balance",
    "Card Not Working": "card_not_working",
    "Card Delivery": "card_delivery",
}

WAREHOUSE_TABLE = {
    "rmt": "remittance_baraq__transaction",
    "cards": "cards_transaction",
}

WRITE_BACK_CANDIDATES = {
    "candidate_targets": [
        "serviceRequests/{SrNumber} attribute: ResolutionComments_c",
        "serviceRequests/{SrNumber} attributes: ResolutionCd, ResolveOutcomeCd",
        "serviceRequests/{SrNumber}/child/messages",
    ],
    "implemented": False,
}

_FACTS_SKIP = {
    "SrId", "SrNumber", "CreationDate", "ProblemDescription",
    "cleaned_description", "sanitized_description", "StatusTypeCdMeaning",
    "StatusTypeCd", "BaraqInternalID_c", "QueueName", "CategoryLevel1_c",
    "CategoryLevel2_c", "CategoryLevel3_c", "TransactionID_c",
    "decision", "rule_id", "response_en", "response_ar", "ticket_action",
}


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text.replace("Z", "+0000"), fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _resolution_text(result: dict[str, Any]) -> str:
    en = (result.get("response_en") or "").strip()
    ar = (result.get("response_ar") or "").strip()
    en, ar = _fix_hold_refund_en_ar_mix(en, ar, rule_id=result.get("rule_id"))
    if en and ar:
        return f"{en} {ar}"
    return en or ar or ""


def _fix_hold_refund_en_ar_mix(
    en: str,
    ar: str,
    *,
    rule_id: Any = None,
) -> tuple[str, str]:
    """Repair historic bug: refund EN paired with hold AR (or the reverse).

    Old runs wrote resolutions like:
      EN: "...refunded to your wallet"
      AR: "...معلقة من قبل التاجر...32 يوم"
    When that conflict appears, keep the hold meaning and rebuild matching EN.
    """
    if not en or not ar:
        return en, ar

    en_l = en.lower()
    refund_en = "refunded to your wallet" in en_l
    hold_en = "within 32 days" in en_l or "pending from the merchant" in en_l
    hold_ar = "معلقة من قبل التاجر" in ar
    refund_ar = "لمحفظتك" in ar or "إلى محفظتك" in ar

    if refund_en and hold_ar:
        m = re.match(r"Welcome\s+([^,]+),", en)
        name = (m.group(1).strip() if m else "") or ""
        greet = f"Welcome {name}," if name else "Welcome,"
        en = (
            f"{greet} the transaction is pending from the merchant's side "
            "and is awaiting resolution or release if not settled by the merchant "
            "within 32 days."
        )
        ar = (
            ar.replace("برق⚡️ ,", "برق،")
            .replace("برق⚡️،", "برق،")
            .replace("برق⚡️,", "برق،")
        )
        print(
            "WARNING: fixed refund-EN + hold-AR resolution mix; rule_id=",
            rule_id,
        )
        return en, ar

    if hold_en and refund_ar:
        m = re.match(r"Welcome\s+([^,]+),", en)
        name = (m.group(1).strip() if m else "") or ""
        if name:
            ar = f"حياك الله {name} في برق، تم استرجاع مبلغ العملية لمحفظتك."
        else:
            ar = "حياك الله في برق، تم استرجاع مبلغ العملية لمحفظتك."
        print(
            "WARNING: fixed hold-EN + refund-AR resolution mix; rule_id=",
            rule_id,
        )
        return en, ar

    return en, ar


def _cards_flow_prefix(ticket: dict[str, Any] | None) -> str:
    cat2 = ((ticket or {}).get("CategoryLevel2_c") or "").strip()
    if cat2 in CARDS_FLOW_ROUTE_PREFIX:
        return CARDS_FLOW_ROUTE_PREFIX[cat2]
    if cat2:
        # Stable slug for unexpected L2 values (e.g. future flows).
        slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in cat2).strip("_")
        return f"card_{slug}" if slug else "card_unknown"
    return "card_unknown"


def _route_for(
    product: str | None,
    rule_id: Any,
    *,
    ticket: dict[str, Any] | None = None,
    ticket_action: str | None = None,
) -> str | None:
    if product is None or rule_id is None:
        return None
    if product == "cards":
        prefix = _cards_flow_prefix(ticket)
        action = (ticket_action or "").strip().upper()
        suffix = "HUMAN_REVIEW" if action == "ESCALATE" else "RESOLVE"
        return f"{prefix}/{suffix}"
    try:
        rid = int(rule_id)
    except (TypeError, ValueError):
        return f"{product}/{rule_id}"
    if product == "rmt":
        return RMT_ROUTE_BY_RULE.get(rid, f"remittance/RULE_{rid}")
    return f"{product}/{rid}"


def _as_int_flag(value: Any) -> int:
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    try:
        return 1 if int(value) else 0
    except (TypeError, ValueError):
        text = str(value).strip().lower()
        if text in {"1", "true", "yes"}:
            return 1
        return 0


def _guess_flow(ticket: dict[str, Any], product: str | None) -> str:
    cat2 = (ticket.get("CategoryLevel2_c") or "").strip()
    if cat2:
        return cat2
    if product == "cards":
        return "cards_unknown_flow"
    if product == "rmt":
        return "rmt_unknown_flow"
    return "unknown_flow"


def _no_match_reason(ticket: dict[str, Any], txn: dict[str, Any], product: str | None) -> str:
    """Compact reason code for filtering no-match rows in SQL."""
    cat2 = (ticket.get("CategoryLevel2_c") or "").strip()
    cat3 = (ticket.get("CategoryLevel3_c") or "").strip()
    if product == "cards" and cat2 == "Transaction Dispute":
        hold = _as_int_flag(txn.get("is_hold_account"))
        settle = _as_int_flag(txn.get("is_settle_account"))
        txn_type = str(txn.get("txn_type") or "").strip().upper()
        wallet_refund = _as_int_flag(txn.get("wallet_refund_posted"))
        dispute_account = _as_int_flag(txn.get("is_dispute_provider_account"))
        account = str(txn.get("provider_account_name") or "").strip()
        dispute_cat3 = {
            "Did not Receive Refund or Credit",
            "A transaction you did not approve or make",
            "ATM did not Dispense Cash",
            "Billed Twice for the Same Thing",
            "Did not Receive Service or Item",
            "Payment Made Another Way",
            "Transaction Was Cancelled",
        }
        if cat3 and cat3 not in dispute_cat3:
            return "cat3_not_in_dispute_list"
        if account and dispute_account != 1 and hold == 0 and settle == 0:
            return "provider_account_not_in_dispute_allowlist"
        if (
            not account
            and hold == 0
            and settle == 0
            and txn_type != "REFUND"
            and wallet_refund != 1
        ):
            return "no_hold_settle_or_refund_signal"
        if hold == 0 and settle == 0 and txn_type != "REFUND" and wallet_refund != 1:
            return "no_hold_settle_or_refund_signal"
        return "dispute_conditions_unmatched"
    if product == "cards" and cat2 == "Incorrect Balance":
        if _as_int_flag(txn.get("is_balance_matching")) != 1:
            return "balance_not_matching_needs_review"
        return "incorrect_balance_unmatched"
    if product == "cards" and cat2 == "Card Not Working":
        return "card_not_working_unhandled_status"
    if product == "cards" and cat2 == "Card Delivery":
        return "card_delivery_unhandled_or_missing_fields"
    if product == "rmt":
        return "rmt_no_static_rule_match"
    return "no_rule_matched"


def _no_match_resolution(ticket: dict[str, Any], result: dict[str, Any], product: str | None) -> str:
    txn = result.get("transaction") or {}
    flow = _guess_flow(ticket, product)
    reason = _no_match_reason(ticket, txn, product)
    cat3 = (ticket.get("CategoryLevel3_c") or "").strip() or "-"
    hold = _as_int_flag(txn.get("is_hold_account"))
    settle = _as_int_flag(txn.get("is_settle_account"))
    txn_type = str(txn.get("txn_type") or "").strip() or "-"
    account = str(txn.get("provider_account_name") or "").strip() or "-"
    provision = str(txn.get("provision_status") or "").strip() or "-"
    return (
        f"NO_MATCH | flow={flow} | cat3={cat3} | "
        f"hold={hold} settle={settle} txn_type={txn_type} | "
        f"account={account} | provision={provision} | reason={reason}"
    )


def _format_facts(result: dict[str, Any], product: str | None) -> str:
    txn = result.get("transaction") or {}
    if not txn:
        return (
            "No transaction record was looked up "
            "(no transaction id, or this specialist has no warehouse table wired)."
        )
    table = (
        result.get("warehouse_table")
        or WAREHOUSE_TABLE.get(product or "", "transaction")
    )
    parts = []
    for key, value in txn.items():
        if key in _FACTS_SKIP or value is None or value == "":
            continue
        parts.append(f"{key}={value}")
    return f"Transaction found in `{table}`: {'; '.join(parts)}"


def result_to_service_work_row(result: dict[str, Any], *, mode: str) -> dict[str, Any]:
    ticket = result.get("ticket") or {}
    product = result.get("product") or (
        classify_product(ticket) if ticket.get("CategoryLevel1_c") else None
    )
    rule_id = result.get("rule_id")
    action = result.get("ticket_action")
    processed_at = _parse_dt(result.get("processed_at"))
    status_cd = ticket.get("StatusTypeCd")
    resolution = _resolution_text(result)
    is_no_match = rule_id is None

    if is_no_match:
        # Flag for review: do not auto-close; encode flow + miss reason.
        resolution = _no_match_resolution(ticket, result, product)
        if product == "cards":
            route = f"{_cards_flow_prefix(ticket)}/NO_MATCH"
        else:
            route = f"{product or 'unknown'}/NO_MATCH"
        ticket_status = "Human review needed"
        resolved_at = None
    else:
        if not resolution:
            print(
                "WARNING: empty resolution for matched rule_id=",
                rule_id,
                "SrNumber=",
                ticket.get("SrNumber"),
                "response_en=",
                repr(result.get("response_en")),
                "response_ar=",
                repr(result.get("response_ar")),
            )
        route = _route_for(
            product,
            rule_id,
            ticket=ticket,
            ticket_action=action,
        )
        if action == "CLOSE_RESOLVED":
            ticket_status = "Closed"
            resolved_at = processed_at
        else:
            # Dynamic / escalate matches (e.g. settle) — keep open for humans
            ticket_status = "Human review needed"
            resolved_at = None

    # CRM write-back not implemented in this job yet.
    write_back = json.dumps(WRITE_BACK_CANDIDATES, ensure_ascii=False)

    return {
        "sr_id": ticket.get("SrId"),
        "sr_number": ticket.get("SrNumber"),
        "queue": ticket.get("QueueName"),
        "status_at_fetch": ticket.get("StatusTypeCdMeaning") or ticket.get("StatusTypeCd"),
        "creation_date": _parse_dt(ticket.get("CreationDate")),
        "transaction_id": ticket.get("TransactionID_c"),
        "resolution": resolution,
        "ticket_status": ticket_status,
        "route": route,
        "facts": _format_facts(result, product),
        "model": ENGINE_MODEL,
        "resolved_at": resolved_at,
        "write_back": write_back,
        "crm_field": "ResolveDescription" if resolution else None,
        "crm_text": resolution or None,
        "crm_text_before": None,
        "crm_status_before": status_cd,
        "crm_status_after": status_cd,
        "crm_ok": None,
        "crm_problems": "[]",
        "crm_outcome": "not_written",
        "crm_http_status": None,
        "crm_mode": "legacy",
        "crm_attempts": None,
        "crm_written_at": None,
        "updated_at": datetime.utcnow(),
        "status_type_cd": status_cd,
    }


SERVICE_WORK_ROW_KEYS = (
    "sr_number",
    "sr_id",
    "queue",
    "status_at_fetch",
    "creation_date",
    "transaction_id",
    "resolution",
    "ticket_status",
    "route",
    "facts",
    "model",
    "resolved_at",
    "write_back",
    "crm_field",
    "crm_text",
    "crm_text_before",
    "crm_status_before",
    "crm_status_after",
    "crm_ok",
    "crm_problems",
    "crm_outcome",
    "crm_http_status",
    "crm_mode",
    "crm_attempts",
    "crm_written_at",
    "updated_at",
    "status_type_cd",
)


def service_work_row_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(k) for k in SERVICE_WORK_ROW_KEYS)


def _sql_literal(value: Any) -> str:
    """StarRocks-safe SQL literal (avoid %s — FE rejects unquoted `/` in routes)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    text = str(value).replace("\\", "\\\\").replace("'", "''")
    return "'" + text + "'"


def insert_service_work_rows(
    execute_sql,
    table: str,
    row_dicts: list[dict[str, Any]],
    *,
    batch_size: int = 25,
) -> int:
    """
    INSERT into agent_work / service_work using literal VALUES (StarRocks-compatible).
    `execute_sql` is notebook execute_sql(query) or DAG equivalent.
    """
    if not row_dicts:
        return 0
    cols = ", ".join(SERVICE_WORK_ROW_KEYS)
    inserted = 0
    for start in range(0, len(row_dicts), batch_size):
        chunk = row_dicts[start : start + batch_size]
        value_groups = []
        for row in chunk:
            vals = service_work_row_values(row)
            value_groups.append(
                "(" + ", ".join(_sql_literal(v) for v in vals) + ")"
            )
        sql = f"INSERT INTO {table} ({cols}) VALUES " + ", ".join(value_groups)
        execute_sql(sql)
        inserted += len(chunk)
    return inserted
