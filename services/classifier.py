# Allow imports when Airflow accidentally parses this file as a DAG
from typing import Optional
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import PRODUCT_MAP


def classify_product(crm_request: dict) -> Optional[str]:
    """
    Reads CategoryLevel1_c from the CRM request and maps it
    to an internal product key (e.g., 'rmt', 'card').

    Returns None if product is unrecognized.
    """
    raw_category = crm_request.get("CategoryLevel1_c", "")

    if not raw_category:
        return None

    normalized = raw_category.strip().lower()
    return PRODUCT_MAP.get(normalized, None)
