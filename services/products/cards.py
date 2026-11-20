# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from typing import Optional

from services.products.base import ProductEngine
from services.rules import load_static_rules, match_rule
from services.product_services import load_sql, run_query, as_flag, row_get
from services.llm_fallback_issues import classify_and_resolve
from services.issue_catalogs.cards_llm_fallback import CARDS_LLM_FALLBACK_CATALOG


class CardsEngine(ProductEngine):
    product_key = "cards"
    warehouse_table = "cards_transaction"

    def get_sql_file(self, crm_request: dict) -> str:
        category = str(
            crm_request.get("CategoryLevel2_c") or ""
        ).strip()

        mapping = {
            "Transaction Dispute": "fetch_cards_dispute.sql",
            "Incorrect Balance": "fetch_cards_balance.sql",
            "Card Not Working": "fetch_cards_not_working.sql",
            "Card Delivery": "fetch_cards_delivery.sql",
        }

        return mapping.get(
            category,
            "fetch_cards_transaction.sql"
        )

    def requires_transaction(self) -> bool:
        # Cards can still match CRM-only rules (e.g. "no card issues" on
        # CategoryLevel2_c alone) even without a resolved warehouse
        # transaction -- so a missing/unowned transaction should NOT hard
        # fail here, unlike RMT.
        return False

    def fetch_transaction(self, crm_request: dict) -> Optional[dict]:
        # The selected cards SQL file enforces ownership validation itself. (INNER JOIN
        # on authorization_transaction.cif = the ticket's own cif) -- no
        # extra check needed here.
        sql_file = self.get_sql_file(crm_request)

        print(
        "CATEGORY =",
        crm_request.get("CategoryLevel2_c")
        )

        print(
            "SQL_FILE =",
            sql_file
        )

        query = load_sql(
            self.product_key,
            sql_file
        )
        transaction_id = crm_request.get("TransactionID_c")
        rows = run_query(query, {"TransactionID_c": transaction_id})
        print("ROWS RETURNED =", len(rows))
        if rows:
            print("DEBUG ROW =", rows[0])
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
            "transaction_status": db_row.get("transaction_status"),
            "payment_transaction_status": db_row.get("payment_transaction_status"),
            "provider_account_number": db_row.get("provider_account_number"),
            "provider_account_name": db_row.get("provider_account_name"),
            # Hold/settle only when SQL allow-lists the provider account and
            # see dispute SQL subscription_stop logic.
            "is_hold_account": as_flag(db_row.get("is_hold_account"), 0),
            "is_settle_account": as_flag(db_row.get("is_settle_account"), 0),
            "is_dispute_provider_account": as_flag(
                db_row.get("is_dispute_provider_account"), 0
            ),
            "txn_type": db_row.get("txn_type"),
            "is_balance_matching": as_flag(db_row.get("is_balance_matching"), 0),
            "provision_status": db_row.get("provision_status"),
            "provision_status_within_two_weeks": as_flag(
                db_row.get("provision_status_within_two_weeks"), 0
            ),
            "rejection_description": db_row.get("rejection_description"),
            # MADA/VISA returned by the selected cards SQL query.
            # None only if both joins leave it unresolved.
            # Cards settle rules key off this being exactly "MADA" or "VISA".
            "card_network": db_row.get("card_network"),
            # 0/1 -- returned by the dispute SQL subscription_stop logic.
            # Defaults to 0 (not the None-preserving pattern above) because
            # the SQL itself already returns a definite 0 when no matching
            # stop instruction exists, rather than NULL/unresolved.
            "charged_after_stop": as_flag(db_row.get("charged_after_stop"), 0),
            # 1 if ANY stop/cancel record exists for this merchant+cif at
            # all (regardless of charge timing) -- used by the
            # LLM-fallback "subscription still active" issue as a data
            # guard, independent of charged_after_stop.
            "has_stop_record": as_flag(db_row.get("has_stop_record"), 0),
            # 1 when SQL confirms a near-duplicate SUCCESS auth (same cif,
            # merchant, amount within 5 minutes). Used by cards rule 3.
            "is_billed_twice": as_flag(db_row.get("is_billed_twice"), 0),
            # 1 only when the wallet ledger has a non-zero REFUND for this
            # transaction. Cards rule 8 closes on this, not on txn_type alone.
            "wallet_refund_posted": as_flag(db_row.get("wallet_refund_posted"), 0),
            # Delivery SQL fields (if/when that query is used)
            "has_physical_card": db_row.get("has_physical_card"),
            "digital_only": db_row.get("digital_only"),
            "order_status": db_row.get("order_status"),
            "order_physical_status": db_row.get("order_physical_status"),
            "subscription_status": db_row.get("subscription_status"),
            "first_name": row_get(db_row, "first_name"),
            "first_name_en": row_get(db_row, "first_name_en"),
        })
        return base_row

    def get_rules(self):
        return load_static_rules(self.product_key)

    def match(self, row: dict, rules_df) -> dict:
        # Pass 1: deterministic rules EXCEPT hold. Charged-after-stop,
        # unauthorized, billed-twice, posted wallet refund, and settle
        # win before hold. Subscription LLM runs before hold.
        hold_mask = rules_df["conditions"].apply(
            lambda c: isinstance(c, dict) and c.get("is_hold_account") == 1
        )
        pre_hold_rules = rules_df.loc[~hold_mask]
        decision = match_rule(row, pre_hold_rules)
        if decision.get("decision") != "NO_MATCH":
            return decision

        # Pass 2: subscription LLM (catalog) BEFORE hold so hold no longer
        # blocks subscription_still_active_no_stop.
        fallback = classify_and_resolve(row, CARDS_LLM_FALLBACK_CATALOG)
        if fallback is not None:
            return fallback

        # Pass 3: hold rule (escalate for proof) if it applies.
        hold_rules = rules_df.loc[hold_mask]
        if not hold_rules.empty:
            hold_decision = match_rule(row, hold_rules)
            if hold_decision.get("decision") != "NO_MATCH":
                return hold_decision

        return decision
