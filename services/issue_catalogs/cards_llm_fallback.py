# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

"""
Cards' catalog of LLM-fallback issues -- only consulted when
services.rules.match_rule() (the deterministic pass) returns NO_MATCH.
See services/llm_fallback_issues.py for the mechanics and safety
properties.

Each entry:
  issue_id         -- unique string id (shows up as `rule_id` in output)
  description      -- shown to the LLM as the definition of this issue;
                       keep it specific enough that it's not a plausible
                       match for unrelated complaints
  category_filter  -- optional; if set, this issue is only ever
                       considered when row['CategoryLevel2_c'] equals
                       this value (cheap pre-filter before the LLM call)
  data_guard       -- dict of {field: expected_value} that MUST already
                       be true on the row (independent of the LLM) for
                       this issue's action to apply
  response         -- frozen en/ar templates + ticket_action, same shape
                       as services.rules' rule "response" blocks
"""

CARDS_LLM_FALLBACK_CATALOG = [
    {
        "issue_id": "subscription_still_active_no_stop",
        "description": (
            "Customer says they previously stopped/cancelled a recurring "
            "subscription (e.g. a Netflix-style service) but were charged "
            "again, and is disputing that charge as unauthorized because "
            "they believe the subscription should no longer be active."
        ),
        "category_filter": "Transaction Dispute",
        # Requires that we found NO stop/cancel record on file at all for
        # this merchant+cif (see fetch_cards_transaction.sql's
        # subscription_stop CTE / has_stop_record column). If a stop
        # record DOES exist, this issue does not apply -- that's the
        # charged_after_stop deterministic rule (cards rule 1) instead.
        # CardsEngine.match() runs this catalog BEFORE the hold rule so
        # hold no longer blocks subscription detection.
        "data_guard": {"has_stop_record": 0},
        "response": {
            "en": (
                "Welcome {first_name}, we checked our records and found no "
                "stop or cancellation request on file for this "
                "subscription -- it remains active, which is why the "
                "charge went through. If you'd still like to stop it, you "
                "can do so from the app, or let us know and we'll assist."
            ),
            "ar": (
                "حياك الله {first_name} في برق، تحققنا من سجلاتنا ولم نجد "
                "أي طلب إيقاف أو إلغاء لهذا الاشتراك، وهو لا يزال نشطًا، "
                "ولهذا تم تنفيذ عملية الخصم. إذا كنت ترغب بإيقافه، يمكنك "
                "ذلك من خلال التطبيق أو إخبارنا للمساعدة."
            ),
            "status": "CLOSE_RESOLVED",
        },
    },
]
