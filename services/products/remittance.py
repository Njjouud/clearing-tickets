# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from typing import Optional

from services.products.base import ProductEngine
from services.rules import load_static_rules, match_rule
from services.product_services import load_sql, run_query, row_get


class RemittanceEngine(ProductEngine):
    product_key = "rmt"
    warehouse_table = "remittance_baraq__transaction"

    def requires_transaction(self) -> bool:
        # RMT rules are meaningless without a real transaction row -- a
        # missing/unowned transaction should hard-fail so the ticket is
        # distinguishable in the backlog as "no transaction data" rather
        # than silently falling into NO_MATCH.
        return True

    def fetch_transaction(self, crm_request: dict) -> Optional[dict]:
        # fetch_rmt_transaction.sql enforces ownership itself (forced join
        # through b_account_id -> accounts -> user_accounts_maps -> user_id,
        # WHERE'd against the ticket's own BaraqInternalID_c) -- no extra
        # check needed here.
        query = load_sql(self.product_key)
        transaction_id = crm_request.get("TransactionID_c")
        rows = run_query(query, {"TransactionID_c": transaction_id})
        return rows[0] if rows else None

    def build_row(self, crm_request: dict, db_row: dict) -> dict:
        db_row = db_row or {}
        base_row = {
            "SrId": crm_request.get("SrId"),
            "SrNumber": crm_request.get("SrNumber"),
            "CreationDate": crm_request.get("CreationDate"),
            "ProblemDescription": crm_request.get("ProblemDescription"),
            "cleaned_description": crm_request.get("cleaned_description"),
            "sanitized_description": crm_request.get("sanitized_description"),
            "StatusTypeCdMeaning": crm_request.get("StatusTypeCdMeaning"),
            "StatusTypeCd": crm_request.get("StatusTypeCd"),
            "BaraqInternalID_c": crm_request.get("BaraqInternalID_c"),
            "QueueName": crm_request.get("QueueName"),
            "CategoryLevel1_c": crm_request.get("CategoryLevel1_c"),
            "CategoryLevel2_c": crm_request.get("CategoryLevel2_c"),
            "CategoryLevel3_c": crm_request.get("CategoryLevel3_c"),
            "TransactionID_c": crm_request.get("TransactionID_c"),
        }
        base_row.update({
            "jake_status": db_row.get("baraq_status"),
            "baraq_sub_status": db_row.get("baraq_sub_status"),
            "mto_transaction_status": db_row.get("mto_transaction_status"),
            "mto_transaction_sub_status": db_row.get("mto_transaction_sub_status"),
            "transaction_date_time": db_row.get("transaction_date_time"),
            "expected_date_of_delivery": db_row.get("expected_date_of_delivery"),
            "compliance_status": db_row.get("compliance_status"),
            "amend_required": as_flag_safe(db_row.get("amend_required")),
            "provider": db_row.get("provider"),
            "delivery_option": db_row.get("delivery_option"),
            "country": db_row.get("country"),
            "frontend_status": db_row.get("frontend_status"),
            "finscan_status": db_row.get("finscan_status"),
            "sas_status": db_row.get("sas_status"),
            "core_banking_status": db_row.get("core_banking_status"),
            "otp_status": db_row.get("otp_status"),
            "error_code": db_row.get("error_code"),
            "error_message": db_row.get("error_message"),
            "mto_txn_reference_number": db_row.get("mto_txn_reference_number"),
            "total_amount": db_row.get("total_amount_to_collect_from_sender"),
            "transaction_datetime_till_now": db_row.get("transaction_datetime_till_now"),
            "ticket_date_datetime_till_now": db_row.get("ticket_date_datetime_till_now"),
            "cif": db_row.get("cif") or row_get(db_row, "cif"),
            "first_name": row_get(db_row, "first_name"),
            "first_name_en": row_get(db_row, "first_name_en"),
        })
        return base_row

    def get_rules(self):
        return load_static_rules(self.product_key)

    def match(self, row: dict, rules_df) -> dict:
        return match_rule(row, rules_df)


def as_flag_safe(value, default: int = 0) -> int:
    """
    Same coercion as product_services.as_flag(), duplicated here (not
    imported) to keep this file's only cross-product dependency on
    product_services limited to load_sql/run_query/row_get.

    FIX vs. the old build_product_row(): amend_required now goes through
    this coercion. Previously it was passed through raw from db_row, so a
    warehouse type mismatch (str "1" or Decimal(1) instead of int) would
    silently fail rule 3/4's `amend_required == 1` check every time.
    """
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
