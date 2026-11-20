"""Load CRM-shaped ticket dicts from an operations Excel export."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Callable

import pandas as pd

RunQuery = Callable[[str], list[dict[str, Any]]]

# Column maps for operations Excel exports (IR vs Cards).
EXCEL_COLUMN_PRESETS: dict[str, dict[str, str]] = {
    "ir": {
        "transaction_id": "Transaction ID",
        "sr_number": "Reference Number",
        "problem_description": "Problem Description",
        "queue_name": "Queue Name",
        "category_l1": "Category L1 Name",
        "category_l2": "Category L2 Name",
        "baraq_internal_id": "Baraq Internal ID",
        "date_reported": "Date Reported",
        "sr_status": "SR Status",
    },
    "cards": {
        "transaction_id": "Transaction ID",
        "sr_number": "Reference Number",
        "problem_description": "Problem Description",
        "queue_name": "Queue Name",
        "category_l1": "Category L1 Name",
        "category_l2": "Category L2 Name",
        "category_l3": "Category L3 Name",
        "baraq_internal_id": "Baraq Internal ID",
        "date_reported": "Date Reported",
        "sr_status": "SR Status",
    },
}

# Backward-compatible alias
DEFAULT_EXCEL_COLUMNS = EXCEL_COLUMN_PRESETS["ir"]


def resolve_excel_columns(
    df: pd.DataFrame,
    excel_format: str = "auto",
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Pick IR vs Cards column preset; optional per-field overrides."""
    fmt = (excel_format or "auto").strip().lower()
    if fmt == "auto":
        if "Category L3 Name" in df.columns:
            fmt = "cards"
        else:
            fmt = "ir"
    if fmt not in EXCEL_COLUMN_PRESETS:
        raise ValueError(
            f"Unknown SAMPLE_EXCEL_FORMAT={excel_format!r}; use ir, cards, or auto"
        )
    cols = dict(EXCEL_COLUMN_PRESETS[fmt])
    if overrides:
        cols.update(overrides)
    return cols

_CRM_SELECT = """
SELECT
      t.SrId
    , t.SrNumber
    , t.CreationDate
    , t.ProblemDescription
    , t.StatusTypeCd
    , t.StatusTypeCdMeaning
    , t.BaraqInternalID_c
    , t.QueueName
    , t.CategoryLevel1_c
    , t.CategoryLevel2_c
    , t.CategoryLevel3_c
    , t.TransactionID_c
FROM {tickets_table} t
WHERE UPPER(TRIM(CAST(t.TransactionID_c AS VARCHAR))) IN ({in_list})
"""


def _norm_txn(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if re.fullmatch(r"0+", text):
        return None
    return text


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _fetch_crm_by_transaction_ids(
    run_query: RunQuery,
    tickets_table: str,
    transaction_ids: list[str],
    *,
    chunk_size: int = 400,
) -> dict[str, dict[str, Any]]:
    by_txn: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(transaction_ids, chunk_size):
        in_list = ", ".join(
            "'" + tid.replace("'", "''").upper() + "'" for tid in chunk
        )
        sql = _CRM_SELECT.format(tickets_table=tickets_table, in_list=in_list)
        for row in run_query(sql):
            key = _norm_txn(row.get("TransactionID_c"))
            if key:
                by_txn[key.upper()] = row
    return by_txn


def _row_from_excel_only(
    row: pd.Series,
    cols: dict[str, str],
) -> dict[str, Any]:
    ref = row.get(cols["sr_number"])
    sr_number = None if pd.isna(ref) else str(ref).strip()
    if sr_number and sr_number.endswith(".0"):
        sr_number = sr_number[:-2]

    txn = _norm_txn(row.get(cols["transaction_id"]))
    reported = row.get(cols["date_reported"])
    creation = reported
    if isinstance(reported, pd.Timestamp):
        creation = reported.to_pydatetime()

    status_meaning = row.get(cols["sr_status"])
    if pd.isna(status_meaning):
        status_meaning = None

    l3_col = cols.get("category_l3")
    category_l3 = row.get(l3_col) if l3_col else None
    if l3_col and pd.isna(category_l3):
        category_l3 = None

    queue = row.get(cols["queue_name"])
    if pd.isna(queue) and "Queue Name.1" in row.index:
        queue = row.get("Queue Name.1")

    return {
        "SrId": None,
        "SrNumber": sr_number,
        "CreationDate": creation,
        "ProblemDescription": row.get(cols["problem_description"]),
        "StatusTypeCd": None,
        "StatusTypeCdMeaning": status_meaning,
        "BaraqInternalID_c": row.get(cols["baraq_internal_id"]),
        "QueueName": queue,
        "CategoryLevel1_c": row.get(cols["category_l1"]),
        "CategoryLevel2_c": row.get(cols["category_l2"]),
        "CategoryLevel3_c": category_l3,
        "TransactionID_c": txn,
    }


def load_tickets_from_excel(
    path: str,
    *,
    run_query: RunQuery,
    tickets_table: str,
    product_map: dict[str, str],
    columns: dict[str, str] | None = None,
    excel_format: str = "auto",
    on_date: date | str | None = None,
    limit: int | None = None,
    enrich_from_crm: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Read Excel export → CRM-shaped tickets for process_ticket().

    Returns (tickets, stats).
    """
    df = pd.read_excel(path)
    cols = resolve_excel_columns(df, excel_format, columns)
    fmt_in = (excel_format or "auto").strip().lower()
    fmt_resolved = (
        "cards"
        if fmt_in == "auto" and "Category L3 Name" in df.columns
        else ("cards" if fmt_in == "cards" else "ir")
    )
    stats: dict[str, Any] = {
        "rows_in_file": len(df),
        "excel_format_requested": fmt_in,
        "excel_format_resolved": fmt_resolved,
    }

    if on_date is not None:
        if isinstance(on_date, str):
            on_date = datetime.strptime(on_date[:10], "%Y-%m-%d").date()
        reported = pd.to_datetime(df[cols["date_reported"]], errors="coerce")
        df = df[reported.dt.date == on_date]
        stats["rows_after_date_filter"] = len(df)

    df = df.copy()
    df["_txn_norm"] = df[cols["transaction_id"]].map(_norm_txn)
    df = df[df["_txn_norm"].notna()]
    stats["rows_with_valid_transaction_id"] = len(df)

    if limit is not None and limit > 0:
        df = df.head(int(limit))
        stats["rows_after_limit"] = len(df)

    crm_by_txn: dict[str, dict[str, Any]] = {}
    if enrich_from_crm and len(df) > 0:
        txn_ids = df["_txn_norm"].drop_duplicates().tolist()
        crm_by_txn = _fetch_crm_by_transaction_ids(
            run_query, tickets_table, txn_ids
        )
    stats["crm_matches"] = 0
    stats["excel_only_fallback"] = 0

    tickets: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        txn_key = str(row["_txn_norm"]).upper()
        crm = crm_by_txn.get(txn_key)
        if crm:
            ticket = dict(crm)
            stats["crm_matches"] += 1
            excel_desc = row.get(cols["problem_description"])
            if excel_desc is not None and not pd.isna(excel_desc):
                ticket["ProblemDescription"] = excel_desc
        else:
            ticket = _row_from_excel_only(row, cols)
            stats["excel_only_fallback"] += 1

        cat = (str(ticket.get("CategoryLevel1_c") or "")).strip().lower()
        ticket["product"] = product_map.get(cat)
        if not ticket["product"]:
            ticket["product"] = "cards" if cat == "cards" else "rmt"
        tickets.append(ticket)

    stats["tickets_ready"] = len(tickets)
    return tickets, stats
