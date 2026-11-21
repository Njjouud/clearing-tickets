# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from decimal import Decimal, InvalidOperation
from typing import Optional

from services.products.base import ProductEngine
from services.rules import load_static_rules, match_rule
from services.product_services import load_sql, run_query, as_flag, row_get

# user_application_checks.status values seen in the warehouse:
# PASSED / PENDING / FAILED / INCOMPLETE. Only PASSED means "passed";
# anything else unexpected matches no routing rule (escalated for review).
APP_CHECK_PASSED_VALUES = {"PASSED"}

# Balance-transfer approval bands (SAR), from the Account & Registration
# booklet section 11: <= 2,000 / > 2,000 and <= 10,000 / > 10,000.
def approval_band(balance) -> Optional[str]:
    if balance is None or balance == "":
        return None
    try:
        amount = Decimal(str(balance))
    except (InvalidOperation, ValueError):
        return None
    if amount <= Decimal("2000"):
        return "MAKER_CHECKER_PAYMENTS_OFFICER"
    if amount <= Decimal("10000"):
        return "HEAD_OF_PAYMENTS"
    return "REGULATORY_COMPLIANCE"


def normalize_cif_status(db_row: dict) -> Optional[str]:
    """Upper-cased cif_status. A user row with a blank/NULL cif_status (or no
    CIF) is 'NO_CIF'. No user row at all -> None (matches no rule)."""
    if not db_row or not row_get(db_row, "user_id"):
        return None
    status = (row_get(db_row, "cif_status") or "").upper()
    if not status or not row_get(db_row, "cif"):
        return status or "NO_CIF"
    return status


def normalize_app_check(db_row: dict) -> Optional[str]:
    raw = (row_get(db_row, "app_check_status") or "").upper()
    if not raw:
        return None
    if raw in APP_CHECK_PASSED_VALUES:
        return "PASSED"
    return raw


class AccountsEngine(ProductEngine):
    product_key = "accounts"
    warehouse_table = "customer_identity_db.users"

    def requires_transaction(self) -> bool:
        # Account tickets have no transaction id; a missing profile just
        # means no routing rule can match (-> escalated as NO_MATCH).
        return False

    def fetch_transaction(self, crm_request: dict) -> Optional[dict]:
        user_id = str(crm_request.get("BaraqInternalID_c") or "").strip()
        if not user_id:
            return None
        safe_user_id = user_id.replace("\\", "\\\\").replace("'", "''")
        query = load_sql(self.product_key, "fetch_accounts_profile.sql")
        rows = run_query(query, {"BaraqInternalID_c": safe_user_id})
        if not rows:
            return None
        row = dict(rows[0])
        row["approval_band"] = approval_band(row.get("wallet_balance"))
        return row

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
            "cif": row_get(db_row, "cif"),
            "cif_status": normalize_cif_status(db_row),
            "cif_super_status": row_get(db_row, "cif_super_status"),
            "app_check_status": normalize_app_check(db_row),
            "app_check_type": row_get(db_row, "app_check_type"),
            "account_status": (row_get(db_row, "account_status") or "").upper() or None,
            "wallet_balance": db_row.get("wallet_balance"),
            "approval_band": db_row.get("approval_band"),
            "is_blocked_account": as_flag(db_row.get("is_blocked_account"), 0),
            "is_minor": as_flag(db_row.get("is_minor"), 0),
            "first_name": row_get(db_row, "first_name"),
            "first_name_en": row_get(db_row, "first_name_en"),
        })
        return base_row

    def get_rules(self):
        return load_static_rules(self.product_key)

    def match(self, row: dict, rules_df) -> dict:
        return match_rule(row, rules_df)
