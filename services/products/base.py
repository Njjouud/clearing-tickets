# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from abc import ABC, abstractmethod
from typing import Any, Optional


class ProductEngine(ABC):
    """
    Contract every product (cards, remittance, and anything registered
    later) must implement so the orchestrator can dispatch to it uniformly
    without knowing anything product-specific.

    To add a new product later:
      1. Subclass this, implement the 4 abstract methods.
      2. Register it: orchestrator.register_engine(YourEngine()).
      3. Nothing else in orchestrator.py changes.
    """

    #: internal product key -- MUST match the values in config.PRODUCT_MAP
    #: (what services.classifier.classify_product() returns).
    product_key: str = ""

    #: warehouse table name, carried into the response for logging/audit.
    warehouse_table: str = ""

    @abstractmethod
    def fetch_transaction(self, crm_request: dict) -> Optional[dict]:
        """
        Look up this product's transaction row for the ticket.

        MUST enforce ownership internally (the transaction returned must
        belong to the ticket's own user/CIF) -- the orchestrator does not
        and cannot check this generically, since the ownership column/join
        differs per product. Return None if no transaction is found or
        found-but-not-owned-by-this-user (those should be indistinguishable
        to the caller -- fail closed).
        """
        raise NotImplementedError

    @abstractmethod
    def build_row(self, crm_request: dict, db_row: dict) -> dict:
        """Merge CRM ticket fields + transaction fields into one flat dict
        for rule matching. db_row may be {} if requires_transaction() is
        False and no transaction was found."""
        raise NotImplementedError

    @abstractmethod
    def get_rules(self) -> Any:
        """Return this product's rules (a pandas DataFrame with rule_id,
        conditions, priority, response columns -- see services.rules)."""
        raise NotImplementedError

    @abstractmethod
    def match(self, row: dict, rules_df: Any) -> dict:
        """Return a decision dict with keys: decision, rule_id, response_en,
        response_ar, ticket_action."""
        raise NotImplementedError

    def requires_transaction(self) -> bool:
        """
        Whether a missing/unowned transaction should hard-fail this product
        (raise, so the ticket is distinguishable as "no transaction data")
        or be allowed to proceed with an empty row so CRM-only rules can
        still match (e.g. cards' "no card issues" category-based rule).

        Default True -- override to False in engines that need the
        permissive behavior.
        """
        return True
