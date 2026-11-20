# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# datetime_utils.py
from datetime import datetime, timedelta
import pandas as pd
import pytz

def check_last_24h(txn_datetime):

    if not txn_datetime:
        return False

    ksa = pytz.timezone("Asia/Riyadh")
    now = datetime.now(ksa)

    txn_time = pd.to_datetime(txn_datetime)

    if txn_time.tzinfo is None:
        txn_time = txn_time.tz_localize("UTC")

    txn_time = txn_time.tz_convert("Asia/Riyadh")

    return txn_time >= now - timedelta(hours=24)