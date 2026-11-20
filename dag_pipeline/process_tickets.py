# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import traceback
from datetime import datetime, timezone
from typing import Any

from services.orchestrator import resolve_ticket as fetch_ticket_response
from utils.cleansing_utils import (
    clean_problem_description,
    sanitize_problem_description_for_llm,
)


def _serialize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    return {k: _serialize_value(v) for k, v in ticket.items()}


def _parse_and_clean(ticket: dict) -> dict:
    """Clean ProblemDescription for rules + LLM (no FastAPI)."""
    if "ProblemDescription" in ticket:
        raw_text = ticket.get("ProblemDescription")
        ticket["cleaned_description"] = clean_problem_description(raw_text)
        ticket["sanitized_description"] = sanitize_problem_description_for_llm(raw_text)
    return ticket


def _ticket_snapshot(payload: dict) -> dict:
    return {
        "SrNumber": payload.get("SrNumber"),
        "SrId": payload.get("SrId"),
        "QueueName": payload.get("QueueName"),
        "TransactionID_c": payload.get("TransactionID_c"),
        "StatusTypeCd": payload.get("StatusTypeCd"),
        "StatusTypeCdMeaning": payload.get("StatusTypeCdMeaning"),
        "CategoryLevel1_c": payload.get("CategoryLevel1_c"),
        "CategoryLevel2_c": payload.get("CategoryLevel2_c"),
        "CategoryLevel3_c": payload.get("CategoryLevel3_c"),
        "CreationDate": payload.get("CreationDate"),
    }


def process_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    """
    Run one CRM ticket through the orchestrator (classify -> product engine
    -> rules -> response). Always returns a result dict (never raises) so
    batch runs can continue.
    """
    payload = _serialize_ticket(ticket)
    started = datetime.now(timezone.utc).isoformat()

    try:
        cleaned = _parse_and_clean(dict(payload))
        # Keep product tagged at extract time.
        if ticket.get("product") and not cleaned.get("product"):
            cleaned["product"] = ticket.get("product")
        decision = fetch_ticket_response(cleaned)
        return {
            "ok": True,
            "error": None,
            "processed_at": started,
            "ticket": _ticket_snapshot(payload),
            "decision": decision.get("decision"),
            "rule_id": decision.get("rule_id"),
            "ticket_action": decision.get("ticket_action"),
            "response_en": decision.get("response_en"),
            "response_ar": decision.get("response_ar"),
            "product": decision.get("product") or ticket.get("product"),
            "warehouse_table": decision.get("warehouse_table"),
            "transaction": decision.get("transaction") or {},
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
            "traceback": traceback.format_exc(),
            "processed_at": started,
            "ticket": _ticket_snapshot(payload),
            "decision": None,
            "rule_id": None,
            "ticket_action": None,
            "response_en": None,
            "response_ar": None,
            "product": ticket.get("product"),
            "warehouse_table": None,
            "transaction": {},
        }


def process_tickets(tickets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [process_ticket(ticket) for ticket in tickets]
