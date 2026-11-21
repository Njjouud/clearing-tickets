# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from typing import Dict

from services.classifier import classify_product
from services.products.base import ProductEngine
from services.products.cards import CardsEngine
from services.products.remittance import RemittanceEngine
from services.products.accounts import AccountsEngine
from services.product_services import build_decision_response

_REGISTRY: Dict[str, ProductEngine] = {}


def register_engine(engine: ProductEngine) -> None:
    """
    Add a product engine to the registry. To add a new product later:

        from services.products.base import ProductEngine

        class RewardsEngine(ProductEngine):
            product_key = "rewards"
            warehouse_table = "..."
            def fetch_transaction(self, crm_request): ...
            def build_row(self, crm_request, db_row): ...
            def get_rules(self): ...
            def match(self, row, rules_df): ...

        # then, once at import time (e.g. bottom of this file, or wherever
        # you define it, as long as it runs before resolve_ticket() is
        # called):
        register_engine(RewardsEngine())

    Nothing else in this file or config.PRODUCT_MAP needs to change beyond
    adding the new key to config.PRODUCT_MAP so classify_product() can
    route CategoryLevel1_c values to it.
    """
    if not engine.product_key:
        raise ValueError(f"{type(engine).__name__} must set product_key")
    _REGISTRY[engine.product_key] = engine


def get_engine(product_key: str) -> ProductEngine:
    engine = _REGISTRY.get(product_key)
    if engine is None:
        raise ValueError(
            f"No engine registered for product: '{product_key}'. "
            f"Registered: {sorted(_REGISTRY.keys())}"
        )
    return engine


# Built-in registrations. Adding a product = write one engine class +
# one register_engine() call -- this is the only place that needs the
# extra line for a new product to be reachable.
register_engine(CardsEngine())
register_engine(RemittanceEngine())
register_engine(AccountsEngine())


def resolve_ticket(crm_request: dict) -> dict:
    """
    Single entry point for turning a CRM ticket into a resolution.
    Replaces the old product_services.fetch_ticket_response().

    Flow: classify -> dispatch to the matched product engine -> fetch
    transaction (ownership enforced inside the engine/its SQL) -> build the
    merged row -> match rules -> compose the final bilingual response.
    """
    # Prefer product set at extract time (avoids re-classify on every ticket).
    product = crm_request.get("product") or classify_product(crm_request)
    if not product:
        raise ValueError(
            f"Unrecognized product: '{crm_request.get('CategoryLevel1_c')}'"
        )

    engine = get_engine(product)

    transaction_id = crm_request.get("TransactionID_c")
    db_row = engine.fetch_transaction(crm_request)

    if not db_row:
        if engine.requires_transaction():
            raise ValueError(
                f"No transaction found for TransactionID_c: {transaction_id} "
                f"(product={product})"
            )
        db_row = {}

    merged_row = engine.build_row(crm_request, db_row)
    rules_df = engine.get_rules()
    decision = engine.match(merged_row, rules_df)

    # Use merged_row so first_name / first_name_en from the transaction/users
    # join are applied -- crm_request alone has no name fields.
    response = build_decision_response(merged_row, decision)
    response["product"] = product
    response["warehouse_table"] = engine.warehouse_table
    response["transaction"] = db_row
    return response
