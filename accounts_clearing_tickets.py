"""
Airflow DAG: clearing_backlog_accounts

Account & Registration tickets, separate from the cards / rmt DAG
(bulk_clearing_tickets.py). Tickets are routed to Account Operation,
Compliance or Technology using the booklet's CIF status / Application Check
assignment matrix (services/rules.py -> _load_accounts_rules). No ticket is
auto-closed.

Flow: extract tickets (OCI, and Sprinklr once configured)
      -> accounts engine (customer profile by User ID -> routing rules)
      -> write
         routed     -> ACCOUNTS_TARGET_TABLE
         not routed -> ACCOUNTS_NO_MATCH_TABLE

Sources:
  OCI       CategoryLevel1_c = 'Account and Registration'
  Sprinklr  universal_case_custom_property_32 = 'Account Operations'
            (disabled until config.SPRINKLR_ACCOUNTS["table"] and its column
            mapping are filled in)

Account tickets have no TransactionID_c, so they are selected by User ID
(BaraqInternalID_c) instead.

Trigger params (or dag_run.conf):
  limit            -> max tickets per source (newest first), default 50
  lookback_days    -> 0 = no date filter, default 30
  source           -> "all" (default) / "oci" / "sprinklr"
  include_resolved -> default False (resolved tickets are skipped)
  sr_number        -> reprocess one ticket by number (bypasses filters,
                      overwrites its previous result in both tables)
  max_workers, write_retry_attempts, write_retry_delay_seconds
                   -> same as the cards / rmt DAG
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def _project_root() -> Path:
    here = Path(__file__).resolve().parent
    for path in (here, here / "clearing-tickets", here / "clearing_tickets"):
        if (path / "config.py").exists() and (path / "dag_pipeline").exists():
            return path
    return here


_ROOT = _project_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dag_pipeline.dag_common import (  # noqa: E402
    TICKET_COLUMNS,
    _jsonable,
    _pick,
    _safe_run_id,
    _sql_str,
    process_tickets_task,
    run_query,
    write_results_task,
)

PRODUCT = "accounts"
OCI_CATEGORY_LABEL = "Account and Registration"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _as_bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def _oci_sql(*, limit, lookback_days, include_resolved, sr_number) -> str:
    from config import ACCOUNTS_OCI_CATEGORY, TICKETS_TABLE

    if sr_number:
        return f"""
        SELECT {TICKET_COLUMNS}
        FROM {TICKETS_TABLE} t
        WHERE t.SrNumber = '{_sql_str(sr_number)}'
          AND LOWER(TRIM(t.CategoryLevel1_c)) = '{_sql_str(ACCOUNTS_OCI_CATEGORY)}'
        LIMIT 1
        """
    return f"""
        SELECT {TICKET_COLUMNS}
        FROM {TICKETS_TABLE} t
        WHERE LOWER(TRIM(t.CategoryLevel1_c)) = '{_sql_str(ACCOUNTS_OCI_CATEGORY)}'
          AND t.BaraqInternalID_c IS NOT NULL
          AND TRIM(t.BaraqInternalID_c) != ''
          AND (
                {1 if include_resolved else 0} = 1
             OR t.StatusTypeCd != 'ORA_SVC_RESOLVED'
             OR t.StatusTypeCd IS NULL
          )
          AND (
                {int(lookback_days)} = 0
             OR t.CreationDate >= DATE_SUB(CURRENT_DATE(), INTERVAL {int(lookback_days)} DAY)
          )
        ORDER BY t.CreationDate DESC
        LIMIT {int(limit)}
        """


def _sprinklr_config() -> dict | None:
    """The Sprinklr mapping from config, or None while it is not set up."""
    from config import SPRINKLR_ACCOUNTS

    cfg = SPRINKLR_ACCOUNTS or {}
    if not cfg.get("table"):
        return None
    cols = cfg.get("columns") or {}
    missing = [k for k in ("SrNumber", "BaraqInternalID_c", "CreationDate") if not cols.get(k)]
    if missing:
        raise ValueError(
            f"config.SPRINKLR_ACCOUNTS is missing required column mapping(s): {missing}"
        )
    return cfg


def _sprinklr_sql(cfg: dict, *, limit, lookback_days, include_resolved, sr_number) -> str:
    cols = cfg["columns"]

    def col(name: str) -> str:
        return f"s.`{cols[name]}`" if cols.get(name) else "NULL"

    # Same column names as the OCI extract, so the engine can't tell them apart.
    select = ",\n          ".join(
        f"{col(name)} AS {name}"
        for name in (
            "SrId", "SrNumber", "CreationDate", "ProblemDescription",
            "StatusTypeCd", "StatusTypeCdMeaning", "BaraqInternalID_c",
            "QueueName", "CategoryLevel2_c", "CategoryLevel3_c", "TransactionID_c",
        )
    )
    select += f",\n          '{_sql_str(OCI_CATEGORY_LABEL)}' AS CategoryLevel1_c"
    category_filter = (
        f"TRIM(s.`{cfg['category_column']}`) = '{_sql_str(cfg['category_value'])}'"
    )

    if sr_number:
        return f"""
        SELECT {select}
        FROM {cfg['table']} s
        WHERE {col('SrNumber')} = '{_sql_str(sr_number)}'
          AND {category_filter}
        LIMIT 1
        """

    resolved = [v for v in (cfg.get("resolved_values") or []) if v]
    if include_resolved or not resolved or not cols.get("StatusTypeCd"):
        status_filter = "1 = 1"
    else:
        values = ", ".join(f"'{_sql_str(v)}'" for v in resolved)
        status_filter = f"({col('StatusTypeCd')} IS NULL OR {col('StatusTypeCd')} NOT IN ({values}))"

    return f"""
        SELECT {select}
        FROM {cfg['table']} s
        WHERE {category_filter}
          AND {col('BaraqInternalID_c')} IS NOT NULL
          AND TRIM({col('BaraqInternalID_c')}) != ''
          AND {status_filter}
          AND (
                {int(lookback_days)} = 0
             OR {col('CreationDate')} >= DATE_SUB(CURRENT_DATE(), INTERVAL {int(lookback_days)} DAY)
          )
        ORDER BY {col('CreationDate')} DESC
        LIMIT {int(limit)}
        """


def extract_tickets_task(**context):
    limit = int(_pick(context, "limit", 50))
    lookback_days = int(_pick(context, "lookback_days", 30))
    include_resolved = _as_bool(_pick(context, "include_resolved", False))
    source = str(_pick(context, "source", "all") or "all").strip().lower()
    if source not in {"all", "oci", "sprinklr"}:
        raise ValueError(f"source must be all / oci / sprinklr, got {source!r}")
    sr_number = _pick(context, "sr_number", "") or None
    if isinstance(sr_number, str):
        sr_number = sr_number.strip() or None

    opts = dict(limit=limit, lookback_days=lookback_days,
                include_resolved=include_resolved, sr_number=sr_number)
    tickets: list[dict] = []
    counts: dict[str, int] = {}

    if source in {"all", "oci"}:
        rows = run_query(_oci_sql(**opts))
        for t in rows:
            t["source"] = "oci"
        counts["oci"] = len(rows)
        tickets.extend(rows)

    sprinklr = _sprinklr_config()
    if source in {"all", "sprinklr"}:
        if sprinklr is None:
            if source == "sprinklr":
                raise ValueError(
                    "Sprinklr is not configured yet: fill config.SPRINKLR_ACCOUNTS"
                )
            print("Sprinklr source not configured yet (config.SPRINKLR_ACCOUNTS) -- skipped")
        elif not (sr_number and tickets):  # single-ticket mode: first hit wins
            rows = run_query(_sprinklr_sql(sprinklr, **opts))
            for t in rows:
                t["source"] = "sprinklr"
            counts["sprinklr"] = len(rows)
            tickets.extend(rows)

    for t in tickets:
        t["product"] = PRODUCT

    meta = {
        "limit": limit,
        "lookback_days": lookback_days,
        "source": source,
        "include_resolved": include_resolved,
        "sr_number": sr_number,
        "run_id": _safe_run_id(context),
        "ticket_count": len(tickets),
        "source_counts": counts,
    }
    ti = context["ti"]
    ti.xcom_push(key="extract_meta", value=meta)
    ti.xcom_push(key="tickets", value=_jsonable(tickets))
    print(f"Extracted {len(tickets)} account tickets {counts}"
          + (f" | single SrNumber={sr_number}" if sr_number else ""))
    return meta


def write_accounts_results_task(**context):
    from config import ACCOUNTS_TARGET_TABLE, ACCOUNTS_NO_MATCH_TABLE

    return write_results_task(
        target_table=ACCOUNTS_TARGET_TABLE,
        no_match_table=ACCOUNTS_NO_MATCH_TABLE,
        **context,
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="clearing_backlog_accounts",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 11, 21),
    catchup=False,
    max_active_runs=1,
    tags=["crm", "rules-engine", "backlog", "accounts"],
    params={
        "limit": 50,
        "lookback_days": 30,
        "source": "all",
        "include_resolved": False,
        "sr_number": "",
        "max_workers": 4,
        "write_retry_attempts": 3,
        "write_retry_delay_seconds": 30,
    },
) as dag:

    t_extract = PythonOperator(
        task_id="extract_tickets",
        python_callable=extract_tickets_task,
    )
    t_process = PythonOperator(
        task_id="process_tickets",
        python_callable=process_tickets_task,
        retries=0,
    )
    t_write = PythonOperator(
        task_id="write_results",
        python_callable=write_accounts_results_task,
        retries=0,
    )

    t_extract >> t_process >> t_write
