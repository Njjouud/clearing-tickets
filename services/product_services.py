# Allow imports when Airflow accidentally parses this file as a DAG
from typing import Optional
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import os
import copy
from functools import lru_cache

# Optional override for notebook / non-Airflow runs:
#   from services.product_services import set_query_runner
#   set_query_runner(my_run_query)
_QUERY_RUNNER = None


def set_query_runner(fn) -> None:
    """Inject a custom run_query(query, params) -> list[dict] (used by notebook)."""
    global _QUERY_RUNNER
    _QUERY_RUNNER = fn


def run_query(query: str, params: dict = None):
    """
    Run SELECT after @placeholder replace.
    - Notebook: uses set_query_runner(...)
    - Airflow: MySqlHook(mysql_conn_id='gcp_mysql')
    """
    params = params or {}
    for key, value in params.items():
        query = query.replace("@" + str(key), str(value))

    if _QUERY_RUNNER is not None:
        return _QUERY_RUNNER(query, None)

    from airflow.providers.mysql.hooks.mysql import MySqlHook

    hook = MySqlHook(mysql_conn_id="gcp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall() or []
        columns = [d[0] for d in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()


def load_sql(product: str, sql_file: str = None) -> str:
    """
    Load a SQL file from services/sql/<product>.
    Cached: the file is read from disk once per process, not once per ticket.
    """
    return _load_sql_cached(product, sql_file)


@lru_cache(maxsize=None)
def _load_sql_cached(product: str, sql_file: str = None) -> str:

    if sql_file is None:
        sql_file = f"fetch_{product}_transaction.sql"

    sql_path = os.path.join(
        os.path.dirname(__file__),
        "sql",
        product,
        sql_file
    )

    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, "r") as f:
        return f.read()

def as_flag(value, default: int = 0) -> int:
    """Coerce warehouse 0/1 / bool / '0'/'1' to int for rule equality checks.
    Shared across product engines -- used by cards.py directly; remittance.py
    keeps its own local copy (as_flag_safe) to avoid a circular import, see
    that file's docstring."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return 1 if value else 0
    try:
        return 1 if int(value) else 0
    except (TypeError, ValueError):
        text = str(value).strip().lower()
        if text in {"1", "true", "yes"}:
            return 1
        if text in {"0", "false", "no"}:
            return 0
        return default


def row_get(row: dict, *keys):
    """Case-insensitive dict get for StarRocks/mysql column names."""
    if not row:
        return None
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for key in keys:
        value = lower_map.get(key.lower())
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _fetch_user_names(baraq_internal_id: Optional[str]) -> dict:
    """Lookup names where users.user_id = CRM BaraqInternalID_c."""
    if not baraq_internal_id:
        return {}
    raw = str(baraq_internal_id).strip()
    if not raw:
        return {}
    safe = raw.replace("'", "''")
    query = f"""
    SELECT u.first_name, u.first_name_en
    FROM polaris.`base.customer_identity_db`.users u
    WHERE TRIM(CAST(u.user_id AS VARCHAR)) = TRIM('{safe}')
    LIMIT 1
    """
    try:
        rows = run_query(query, None)
    except Exception as exc:
        print("USER LOOKUP ERROR:", type(exc).__name__, str(exc))
        return {}
    if not rows:
        return {}
    return {
        "first_name": row_get(rows[0], "first_name"),
        "first_name_en": row_get(rows[0], "first_name_en"),
    }


def _clean_name(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "customer"}:
        return ""
    return text


def build_decision_response(base: dict, decision: dict) -> dict:
    """
    Product-agnostic response composition: fills first_name / transaction_id
    placeholders into whatever response_en/response_ar the matched engine's
    decision carries. Called by orchestrator.resolve_ticket() for every
    product -- do not put product-specific logic in here.
    """
    response = copy.deepcopy(base)
    first_name_ar = _clean_name(base.get("first_name"))
    first_name_en = _clean_name(base.get("first_name_en"))
    if not first_name_ar or not first_name_en:
        profile = _fetch_user_names(base.get("BaraqInternalID_c"))
        first_name_ar = first_name_ar or _clean_name(profile.get("first_name"))
        first_name_en = first_name_en or _clean_name(profile.get("first_name_en"))
    # Prefer the other language name if one side is missing; never invent "Customer"
    first_name_en = first_name_en or first_name_ar
    first_name_ar = first_name_ar or first_name_en

    if not first_name_en and not first_name_ar:
        print(
            "NAME MISSING for BaraqInternalID_c=",
            base.get("BaraqInternalID_c"),
            "SrNumber=",
            base.get("SrNumber"),
            "TransactionID_c=",
            base.get("TransactionID_c"),
        )

    txn_id = (
        base.get("TransactionID_c")
        or base.get("transaction_id")
        or base.get("reference_id")
        or ""
    )
    mto_ref = base.get("mto_txn_reference_number") or ""

    response["decision"] = decision.get("decision", "")
    response["rule_id"] = decision.get("rule_id")
    response["response_en"] = _fill_response_placeholders(
        decision.get("response_en", ""),
        first_name=first_name_en,
        lang="en",
        transaction_id=txn_id,
        mto_txn_reference_number=mto_ref,
    )
    response["response_ar"] = _fill_response_placeholders(
        decision.get("response_ar", ""),
        first_name=first_name_ar,
        lang="ar",
        transaction_id=txn_id,
        mto_txn_reference_number=mto_ref,
    )
    response["ticket_action"] = decision.get("ticket_action", "")
    response["first_name"] = first_name_ar or None
    response["first_name_en"] = first_name_en or None
    return response


def _fill_response_placeholders(
    text: str,
    *,
    first_name: str,
    lang: str = "en",
    transaction_id: str = "",
    mto_txn_reference_number: str = "",
) -> str:
    if not text:
        return text
    text = _personalize_response(text, first_name, lang=lang)
    txn = str(transaction_id or "").strip()
    if "{TransactionID_c}" in text:
        text = text.replace("{TransactionID_c}", txn)
    if "{transaction_id}" in text:
        text = text.replace("{transaction_id}", txn)
    if "{mto_txn_reference_number}" in text:
        text = text.replace(
            "{mto_txn_reference_number}",
            str(mto_txn_reference_number or "").strip(),
        )
    return text


def _personalize_response(text: str, first_name: str, *, lang: str = "en") -> str:
    if not text:
        return text
    name = _clean_name(first_name)
    if "{first_name}" in text:
        if name:
            text = text.replace("{first_name}", name)
        elif lang == "ar":
            text = (
                text.replace("حياك الله {first_name} في برق،", "حياك الله في برق،")
                .replace("حياك الله {first_name}،", "حياك الله،")
                .replace(" {first_name} ", " ")
                .replace("{first_name}", "")
            )
        else:
            text = (
                text.replace("Welcome {first_name}, ", "Welcome to jake, ")
                .replace("Welcome {first_name}! ", "Welcome to jake! ")
                .replace("Welcome {first_name},", "Welcome to jake,")
                .replace("Dear {first_name},", "Dear jake Customer,")
                .replace("{first_name}", "")
            )
        return text
    if not name:
        return text
    # Back-compat for older RMT/cards templates
    replacements = [
        ("Welcome to jake⚡️, ", f"Welcome {name}, "),
        ("Welcome to jake⚡️,", f"Welcome {name},"),
        ("Welcome to jake! ", f"Welcome {name}! "),
        ("Welcome to jake, ", f"Welcome {name}, "),
        ("Welcome to jake. ", f"Welcome {name}, "),
        ("Dear jake Customer,", f"Dear {name},"),
        ("حياك الله في برق⚡️،", f"حياك الله {name} في برق،"),
        ("حياك الله في برق⚡️ ,", f"حياك الله {name} في برق،"),
        ("حياك الله في برق،", f"حياك الله {name} في برق،"),
        ("حياك الله في برق ,", f"حياك الله {name} في برق،"),
        ("حياك الله في برق,", f"حياك الله {name} في برق،"),
    ]
    for old, new in replacements:
        if old in text:
            return text.replace(old, new, 1)
    return text


def fetch_ticket_response(crm_request: dict) -> dict:
    """
    Backward-compatible wrapper. New code should call
    services.orchestrator.resolve_ticket() directly -- this just keeps any
    existing import of fetch_ticket_response() working unchanged.

    Import is inside the function (not at module top) to avoid a circular
    import: orchestrator.py imports build_decision_response from this file,
    so this file cannot import orchestrator.py at module load time.
    """
    from services.orchestrator import resolve_ticket
    return resolve_ticket(crm_request)
