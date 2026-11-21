"""
Airflow DAG: clearing_backlog_ticket

Flow: extract tickets -> run rules engine -> write
      matches    -> TARGET_TABLE   (tickets_agent__agent_work)
      no-matches -> NO_MATCH_TABLE (clearing_backlog__service_work)

Same logic as bulk_clearing_tickets.py, split into three Airflow tasks.
Data passes between tasks via XCom (not /tmp — tasks can run on different workers).

DB connection: MySqlHook(mysql_conn_id="gcp_mysql")
LLM settings (Airflow Variables, fall back to config.py if missing):
  litellm_clearing_tickets_token  -> API key (required)
  litellm_base_url                -> LiteLLM proxy base URL
  litellm_clearing_tickets_model  -> model name, e.g. groq/openai/gpt-oss-20b

Trigger params (or dag_run.conf):
  limit, lookback_days, product ("cards" / "rmt" / "" for both;
                               Account & Registration tickets run in the
                               separate accounts_clearing_tickets.py DAG),
  include_resolved -> default False: tickets with StatusTypeCd = ORA_SVC_RESOLVED
                      are skipped. True = process resolved tickets too.
  sr_number  -> reprocess one ticket by SrNumber (bypasses filters, overwrites
                its previous result in both tables)
  max_workers -> tickets processed concurrently in process_tickets (default 4;
                 1 = the old one-at-a-time behaviour). Results keep extract order.
  write_retry_attempts       -> in write_results, re-run tickets that failed with a
                                transient error (lost connection, StarRocks memory
                                limit, LLM busy) up to N times, one at a time (default 3)
  write_retry_delay_seconds  -> pause before retry round k = k * this (default 30)
"""

from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# DAG configuration
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def _project_root() -> Path:
    here = Path(__file__).resolve().parent
    for path in (here, here / "clearing-tickets", here / "clearing_tickets"):
        if (path / "config.py").exists() and (path / "dag_pipeline").exists():
            return path
    return here


def _ensure_imports() -> Path:
    root = _project_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


# Shared task code (process / write / DB / LLM helpers)
_ensure_imports()
from dag_pipeline.dag_common import (  # noqa: E402
    _pick,
    _safe_run_id,
    _jsonable,
    _sql_str,
    TICKET_COLUMNS,
    run_query,
    execute_sql,
    process_tickets_task,
    write_results_task,
)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_tickets_task(**context):
    _ensure_imports()
    from config import PRODUCT_MAP, TICKETS_TABLE, NO_TRANSACTION_PRODUCTS

    limit = int(_pick(context, "limit", 50))
    lookback_days = int(_pick(context, "lookback_days", 30))
    product = _pick(context, "product", "") or None
    if isinstance(product, str):
        product = product.strip() or None
    # Resolved tickets (StatusTypeCd = ORA_SVC_RESOLVED) are excluded unless
    # include_resolved is explicitly true. Parse strings too: bool("false")
    # is True in Python, so a conf value of "false" used to count as true.
    raw_include = _pick(context, "include_resolved", False)
    if isinstance(raw_include, str):
        include_resolved = raw_include.strip().lower() in {"1", "true", "yes", "y"}
    else:
        include_resolved = bool(raw_include)
    include_resolved_flag = 1 if include_resolved else 0
    sr_number = _pick(context, "sr_number", "") or None
    if isinstance(sr_number, str):
        sr_number = sr_number.strip() or None

    run_id = _safe_run_id(context)

    if sr_number:
        # Single-ticket mode: bypass status / lookback / category filters
        sql = f"""
        SELECT {TICKET_COLUMNS}
        FROM {TICKETS_TABLE} t
        WHERE t.SrNumber = '{_sql_str(sr_number)}'
        LIMIT 1
        """
    else:
        # Filter product in SQL so LIMIT applies to mapped categories only
        if product:
            allowed_cats = sorted(c for c, key in PRODUCT_MAP.items() if key == product)
        else:
            # "" = all products that are keyed by a transaction id (cards +
            # rmt, as before). User-ID products (accounts) only run when
            # asked for explicitly, so they can't crowd out the others.
            allowed_cats = sorted(
                c for c, key in PRODUCT_MAP.items()
                if key not in NO_TRANSACTION_PRODUCTS
            )
        if not allowed_cats:
            raise ValueError(f"No CategoryLevel1_c mapping for product={product!r}")
        cats_sql = ", ".join("'" + _sql_str(c) + "'" for c in allowed_cats)

        if product in NO_TRANSACTION_PRODUCTS:
            raise ValueError(
                f"product={product!r} runs in its own DAG "
                "(accounts_clearing_tickets.py / clearing_backlog_accounts)"
            )

        sql = f"""
        SELECT {TICKET_COLUMNS}
        FROM {TICKETS_TABLE} t
        WHERE t.TransactionID_c IS NOT NULL
          AND TRIM(t.TransactionID_c) != ''
          AND LOWER(TRIM(t.CategoryLevel1_c)) IN ({cats_sql})
          AND (
                {include_resolved_flag} = 1
             OR t.StatusTypeCd != 'ORA_SVC_RESOLVED'
             OR t.StatusTypeCd IS NULL
          )
          AND (
                {lookback_days} = 0
             OR t.CreationDate >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
          )
        ORDER BY t.CreationDate DESC
        LIMIT {limit}
        """

    tickets = run_query(sql)

    # Tag product (rmt / cards) so process skips re-classify
    for t in tickets:
        cat = (t.get("CategoryLevel1_c") or "").strip().lower()
        t["product"] = PRODUCT_MAP.get(cat)

    meta = {
        "limit": limit,
        "lookback_days": lookback_days,
        "product": product,
        "include_resolved": include_resolved,
        "sr_number": sr_number,
        "run_id": run_id,
        "ticket_count": len(tickets),
    }
    ti = context["ti"]
    ti.xcom_push(key="extract_meta", value=meta)
    ti.xcom_push(key="tickets", value=_jsonable(tickets))

    print(f"Extracted {len(tickets)} tickets | product={product or 'all mapped'}"
          + (f" | single SrNumber={sr_number}" if sr_number else ""))
    return meta


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="clearing_backlog_ticket_oci",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/30 * * * *",
    start_date =  datetime(2026, 11, 18),
    catchup=False,
    max_active_runs=1,
    tags=["crm", "rules-engine", "backlog"],
    params={
        "limit": 50,
        "lookback_days": 30,
        "product": "",
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
        retries=0,  # don't re-call the LLM for the whole batch on a retry
    )
    t_write = PythonOperator(
        task_id="write_results",
        python_callable=write_results_task,
        retries=0,  # the error check at the end would only fail again
    )

    t_extract >> t_process >> t_write
