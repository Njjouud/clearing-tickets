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
  limit, lookback_days, product ("cards" / "rmt" / "" for all),
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

MYSQL_CONN_ID = "gcp_mysql"
LLM_KEY_VARIABLE = "litellm_clearing_tickets_token"
LLM_BASE_URL_VARIABLE = "litellm_base_url"
LLM_MODEL_VARIABLE = "litellm_clearing_tickets_model"

# Same DDL as bulk_clearing_tickets.py step 3
CREATE_NO_MATCH_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    sr_number          VARCHAR(128) NOT NULL
  , sr_id              VARCHAR(128)
  , queue              VARCHAR(256)
  , status_at_fetch    VARCHAR(128)
  , creation_date      DATETIME
  , transaction_id     VARCHAR(256)
  , resolution         VARCHAR(65533)
  , ticket_status      VARCHAR(64)
  , route              VARCHAR(128)
  , facts              VARCHAR(65533)
  , model              VARCHAR(128)
  , resolved_at        DATETIME
  , write_back         VARCHAR(65533)
  , crm_field          VARCHAR(256)
  , crm_text           VARCHAR(65533)
  , crm_text_before    VARCHAR(65533)
  , crm_status_before  VARCHAR(128)
  , crm_status_after   VARCHAR(128)
  , crm_ok             TINYINT
  , crm_problems       VARCHAR(65533)
  , crm_outcome        VARCHAR(64)
  , crm_http_status    INT
  , crm_mode           VARCHAR(64)
  , crm_attempts       INT
  , crm_written_at     DATETIME
  , updated_at         DATETIME
  , status_type_cd     VARCHAR(128)
)
ENGINE=OLAP
PRIMARY KEY(sr_number)
DISTRIBUTED BY HASH(sr_number) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
)
"""

TICKET_COLUMNS = """
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
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _pick(context: dict, key: str, default=None):
    conf = {}
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None):
        conf = dag_run.conf or {}
    params = context.get("params") or {}
    if key in conf and conf[key] is not None:
        return conf[key]
    if key in params and params[key] is not None:
        return params[key]
    return default


def _safe_run_id(context: dict) -> str:
    run_id = context.get("run_id") or "manual"
    return str(run_id).replace(":", "").replace("+", "_").replace(" ", "_")


def _jsonable(payload):
    """Make rows XCom-safe (datetimes, Decimals -> str)."""
    return json.loads(json.dumps(payload, ensure_ascii=False, default=str))


def _sql_str(value) -> str:
    return str(value).replace("'", "''")


# --- DB helpers: same signatures as bulk_clearing_tickets.py, via MySqlHook ---

def _get_connection():
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    return MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_conn()


# One warehouse connection per worker thread, reused across that thread's
# queries. Before, every query built a MySqlHook (Airflow metadata-DB lookup)
# and opened + authenticated a fresh connection -- 1-2x per ticket.
_THREAD_CONN = threading.local()
_OPEN_CONNS: list = []
_OPEN_CONNS_LOCK = threading.Lock()


def _thread_connection(fresh: bool = False):
    conn = getattr(_THREAD_CONN, "conn", None)
    if conn is not None and not fresh:
        return conn
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    conn = _get_connection()
    _THREAD_CONN.conn = conn
    with _OPEN_CONNS_LOCK:
        _OPEN_CONNS.append(conn)
    return conn


def close_thread_connections() -> None:
    with _OPEN_CONNS_LOCK:
        conns = list(_OPEN_CONNS)
        _OPEN_CONNS.clear()
    for conn in conns:
        try:
            conn.close()
        except Exception:
            pass


def _select(conn, query: str) -> list[dict]:
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall() or []
        columns = [d[0] for d in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        try:
            # End any implicit transaction so the next SELECT on this reused
            # connection sees fresh data (same as a new connection would).
            conn.commit()
        except Exception:
            pass


def run_query(query: str, params: dict | None = None) -> list[dict]:
    params = params or {}
    for key, value in params.items():
        query = query.replace("@" + str(key), str(value))
    try:
        return _select(_thread_connection(), query)
    except Exception:
        # Reused connection may have gone stale (server timeout etc.):
        # retry the read once on a brand-new connection, as before.
        return _select(_thread_connection(fresh=True), query)


def execute_sql(query: str, many=None) -> int:
    conn = _get_connection()
    cursor = conn.cursor()
    try:
        if many is None:
            cursor.execute(query)
        else:
            cursor.executemany(query, many)
        conn.commit()
        return cursor.rowcount
    finally:
        cursor.close()
        conn.close()


def _wire_llm_env() -> None:
    """Expose LiteLLM settings to utils.llm_utils via env vars."""
    from airflow.models import Variable
    import config

    base_url = Variable.get(LLM_BASE_URL_VARIABLE, default_var=None) or getattr(
        config, "LITELLM_BASE_URL", None
    )
    model = Variable.get(LLM_MODEL_VARIABLE, default_var=None) or getattr(
        config, "LITELLM_MODEL", None
    )
    api_key = Variable.get(LLM_KEY_VARIABLE)

    if base_url:
        os.environ["LITELLM_BASE_URL"] = str(base_url)
    if model:
        os.environ["LITELLM_MODEL"] = str(model)
    os.environ["LITELLM_API_KEY"] = api_key

    print(f"LLM: base_url={base_url} model={model}")


def _has_resolution(r: dict) -> bool:
    return bool(
        (r.get("response_en") or "").strip()
        or (r.get("response_ar") or "").strip()
    )


def _is_matched(r: dict) -> bool:
    return r.get("rule_id") is not None and _has_resolution(r)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_tickets_task(**context):
    _ensure_imports()
    from config import PRODUCT_MAP, TICKETS_TABLE

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
            allowed_cats = sorted(PRODUCT_MAP.keys())
        if not allowed_cats:
            raise ValueError(f"No CategoryLevel1_c mapping for product={product!r}")
        cats_sql = ", ".join("'" + _sql_str(c) + "'" for c in allowed_cats)

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
# Process (rules engine)
# ---------------------------------------------------------------------------

def process_tickets_task(**context):
    _ensure_imports()
    _wire_llm_env()
    os.environ["MATCH_DEBUG"] = "0"

    from services.product_services import set_query_runner
    from dag_pipeline.process_tickets import process_ticket

    set_query_runner(run_query)

    ti = context["ti"]
    extract_meta = ti.xcom_pull(task_ids="extract_tickets", key="extract_meta")
    tickets = ti.xcom_pull(task_ids="extract_tickets", key="tickets") or []

    max_workers = max(1, int(_pick(context, "max_workers", 4)))

    def _run_one(ticket):
        r = process_ticket(ticket)  # never raises; errors come back as ok=False
        r["_problem_description"] = ticket.get("ProblemDescription")
        return r

    # Each ticket is independent and almost all its time is spent waiting on
    # the warehouse and the LLM proxy, so run them concurrently.
    # executor.map keeps results in the same order as `tickets`.
    try:
        if max_workers == 1 or len(tickets) <= 1:
            results = [_run_one(t) for t in tickets]
        else:
            with ThreadPoolExecutor(
                max_workers=min(max_workers, len(tickets)),
                thread_name_prefix="ticket",
            ) as pool:
                results = list(pool.map(_run_one, tickets))
    finally:
        close_thread_connections()

    meta = {
        **extract_meta,
        "ok_count": sum(1 for r in results if r.get("ok")),
        "error_count": sum(1 for r in results if not r.get("ok")),
        "matched_count": sum(1 for r in results if _is_matched(r)),
    }
    ti.xcom_push(key="process_meta", value=meta)
    ti.xcom_push(key="results", value=_jsonable(results))

    print(
        f"Processed {len(results)} tickets "
        f"(ok={meta['ok_count']}, error={meta['error_count']}, "
        f"matched={meta['matched_count']})"
    )
    return meta


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

# Errors worth retrying: the warehouse was overloaded or dropped the
# connection, or the LLM proxy was busy. Anything else (e.g. "No transaction
# found") is a real result and retrying would give the same answer.
TRANSIENT_ERROR_MARKERS = (
    "lost connection",            # MySQL 2013
    "server has gone away",       # MySQL 2006
    "can't connect",              # MySQL 2003
    "memory of process exceed",   # StarRocks memory limit
    "mem usage",
    "exceed limit",
    "timed out",
    "timeout",
    "too many requests",          # LLM 429
    "429",
    "502", "503", "504",
    "connection reset",
    "connection aborted",
    "connectionerror",
    "readtimeout",
)


def _is_transient_failure(r: dict) -> bool:
    if r.get("ok"):
        return False
    text = f"{r.get('error_type') or ''} {r.get('error') or ''}".lower()
    return any(marker in text for marker in TRANSIENT_ERROR_MARKERS)


def _retry_failed_tickets(results: list[dict], tickets: list[dict], context) -> list[dict]:
    """Re-run tickets that failed with a transient error, one at a time
    (no concurrency, so the warehouse isn't hit by parallel heavy queries),
    with a growing pause between rounds. Same process_ticket() as the
    process step, so a retried ticket gets exactly the same decision logic.
    Returns `results` with successful retries swapped in, same order."""
    import time

    attempts = max(0, int(_pick(context, "write_retry_attempts", 3)))
    base_delay = max(0, int(_pick(context, "write_retry_delay_seconds", 30)))
    if attempts == 0:
        return results

    failed_idx = [i for i, r in enumerate(results) if _is_transient_failure(r)]
    if not failed_idx:
        return results

    _wire_llm_env()
    os.environ["MATCH_DEBUG"] = "0"
    from services.product_services import set_query_runner
    from dag_pipeline.process_tickets import process_ticket

    set_query_runner(run_query)

    # Full extracted ticket (incl. BaraqInternalID_c, ProblemDescription,
    # product) -- the result only carries a snapshot.
    by_sr = {str(t.get("SrNumber")): t for t in tickets if t.get("SrNumber")}

    results = list(results)
    try:
        for attempt in range(1, attempts + 1):
            pending = [i for i in failed_idx if _is_transient_failure(results[i])]
            if not pending:
                break
            delay = base_delay * attempt
            print(f"Retry {attempt}/{attempts}: {len(pending)} ticket(s) "
                  f"failed with transient errors; waiting {delay}s")
            if delay:
                time.sleep(delay)
            # Drop any connection that may have been killed by the server.
            close_thread_connections()
            _THREAD_CONN.conn = None
            for i in pending:
                sr = str((results[i].get("ticket") or {}).get("SrNumber"))
                ticket = by_sr.get(sr)
                if ticket is None:
                    print(f"  {sr}: original ticket not in extract XCom, can't retry")
                    continue
                r = process_ticket(ticket)
                r["_problem_description"] = ticket.get("ProblemDescription")
                r = _jsonable(r)
                results[i] = r
                status = "ok" if r.get("ok") else f"failed again: {str(r.get('error'))[:120]}"
                print(f"  {sr}: {status}")
    finally:
        close_thread_connections()

    still = sum(1 for i in failed_idx if not results[i].get("ok"))
    print(f"Retries done: {len(failed_idx) - still} recovered, {still} still failing")
    return results


def write_results_task(**context):
    _ensure_imports()
    from config import TARGET_TABLE, NO_MATCH_TABLE
    from dag_pipeline.write_service_work import (
        result_to_service_work_row,
        insert_service_work_rows,
    )

    ti = context["ti"]
    process_meta = ti.xcom_pull(task_ids="process_tickets", key="process_meta")
    results = ti.xcom_pull(task_ids="process_tickets", key="results") or []
    results = [r for r in results if (r.get("ticket") or {}).get("SrNumber")]

    # Retry tickets that failed for transient reasons (warehouse memory /
    # lost connection / LLM busy) before anything is written.
    tickets = ti.xcom_pull(task_ids="extract_tickets", key="tickets") or []
    results = _retry_failed_tickets(results, tickets, context)
    ok_count = sum(1 for r in results if r.get("ok"))
    error_count = sum(1 for r in results if not r.get("ok"))

    execute_sql(CREATE_NO_MATCH_SQL.format(table=NO_MATCH_TABLE))

    # Single-ticket rerun: clear its previous result from both tables first,
    # since a rerun can move a ticket from no-match to match or back.
    sr_number = process_meta.get("sr_number")
    if sr_number:
        safe = _sql_str(sr_number)
        del_target = execute_sql(f"DELETE FROM {TARGET_TABLE} WHERE sr_number = '{safe}'")
        del_nomatch = execute_sql(f"DELETE FROM {NO_MATCH_TABLE} WHERE sr_number = '{safe}'")
        print(f"Cleared prior result for {sr_number} "
              f"(target rows={del_target}, no-match rows={del_nomatch})")

    matched = [r for r in results if _is_matched(r)]
    unmatched = [r for r in results if not _is_matched(r)]

    inserted_match = inserted_nomatch = 0
    if matched:
        rows = [result_to_service_work_row(r, mode="production") for r in matched]
        insert_service_work_rows(execute_sql, TARGET_TABLE, rows)
        inserted_match = len(rows)
        print(f"Inserted {inserted_match} matches into {TARGET_TABLE}")
    if unmatched:
        rows = [result_to_service_work_row(r, mode="production") for r in unmatched]
        insert_service_work_rows(execute_sql, NO_MATCH_TABLE, rows)
        inserted_nomatch = len(rows)
        print(f"Inserted {inserted_nomatch} no-matches into {NO_MATCH_TABLE}")

    summary = {
        "ticket_count": process_meta.get("ticket_count", 0),
        "ok_count": ok_count,
        "error_count": error_count,
        "inserted_matches": inserted_match,
        "inserted_no_matches": inserted_nomatch,
        "target_table": TARGET_TABLE,
        "no_match_table": NO_MATCH_TABLE,
        "run_id": process_meta.get("run_id"),
    }
    ti.xcom_push(key="summary", value=summary)
    print(f"Wrote results: {summary}")

    # Rows are saved either way; turn the run red so errors get noticed.
    if summary["error_count"]:
        errors = [
            f"{(r.get('ticket') or {}).get('SrNumber')}: {str(r.get('error'))[:200]}"
            for r in results if not r.get("ok")
        ][:5]
        raise RuntimeError(
            f"{summary['error_count']} ticket(s) failed. Samples: {errors}"
        )
    return summary


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
