# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

"""
LLM-fallback issue classification.

Runs ONLY when the deterministic rules engine (services.rules.match_rule)
returns NO_MATCH -- i.e. no structured rule condition matched. Handles
cases where the customer's own wording doesn't map cleanly onto
CategoryLevel2_c/3_c (e.g. a subscription complaint filed under a generic
"Transaction Dispute" category), by asking the LLM to pick from a small,
CLOSED set of known issue types for that product -- never open-ended
reasoning about the resolution itself.

Client setup mirrors utils/llm_utils.py exactly (OpenAI SDK pointed at
the LiteLLM proxy, same API-key resolution order) so this file behaves
identically to the existing classify_problem_description() under the
same LiteLLM setup, auth, and failure modes.

Safety properties preserved (same spirit as the rest of this engine):
  - The LLM only ever picks from a fixed list of issue_ids handed to it
    for this specific call; any other response is treated as no match.
  - Each issue's "data_guard" (deterministic facts already on the row)
    must ALSO independently hold true before its action is applied. The
    LLM can suggest a category; it can never make that category true on
    its own, and a guard failure overrides an LLM pick.
  - Response text is still a frozen template per issue, same pattern as
    services.rules -- the LLM composes nothing here. Swapping in
    LLM-composed response text is a separate, later change.
  - candidates are pre-filtered by category_filter + data_guard BEFORE
    the LLM is even called -- if nothing is eligible, we never spend an
    LLM call on a ticket that couldn't use any of these issues anyway.
"""

import json
import os
import re
from typing import Optional
import requests

from airflow.models import Variable


from config import (
    LITELLM_API_KEY,
    LITELLM_BASE_URL,
    LITELLM_MODEL,
)


def _guard_passes(data_guard: Optional[dict], row: dict) -> bool:
    for field, expected in (data_guard or {}).items():
        if row.get(field) != expected:
            return False
    return True


def _category_matches(issue: dict, row: dict) -> bool:
    cat_filter = issue.get("category_filter")
    if cat_filter is None:
        return True
    return row.get("CategoryLevel2_c") == cat_filter


def _eligible_candidates(catalog: list, row: dict) -> list:
    return [
        issue for issue in catalog
        if _category_matches(issue, row) and _guard_passes(issue.get("data_guard"), row)
    ]

def _litellm_settings():
    """Resolve URL / key / model. In config.py, LITELLM_BASE_URL is an Airflow
    Connection id and LITELLM_API_KEY may be an Airflow Variable name."""
    base = LITELLM_BASE_URL
    key = LITELLM_API_KEY
    model = os.getenv("LITELLM_MODEL") or LITELLM_MODEL
    if not base.startswith("http"):
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(base)
        host = (conn.host or "").rstrip("/")
        if not host.startswith("http"):
            host = f"{conn.schema or 'https'}://{host}"
        if conn.port and f":{conn.port}" not in host:
            host = f"{host}:{conn.port}"
        base = host
        if conn.password:
            key = conn.password
    if key == LITELLM_API_KEY:  # still a name, not a real key: try it as a Variable
        try:
            from airflow.models import Variable
            key = Variable.get(LITELLM_API_KEY, default_var=key)
        except Exception:
            pass
    return base.rstrip("/"), key, model

def _chat(messages, temperature=0, max_tokens=256, timeout=60) -> str:
    """Call the LiteLLM proxy (OpenAI-compatible) over plain HTTP. No extra libraries."""
    base, api_key, model = _litellm_settings()
    url = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return (r.json()["choices"][0]["message"].get("content") or "").strip()


def _parse_issue_id(content: str, valid_ids: set) -> Optional[str]:
    """Extract an issue_id from model text (JSON or bare id), same
    robustness pattern as llm_utils._parse_label(): handle markdown
    fences, try strict JSON, fall back to regex, fall back to a raw
    substring search against the known valid ids. Returns None (not "NONE")
    if nothing valid is found -- caller treats that as no match."""
    if not content:
        return None
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()

    try:
        data = json.loads(text)
        issue_id = data.get("issue_id")
        if issue_id in valid_ids:
            return issue_id
        return None
    except json.JSONDecodeError:
        pass

    match = re.search(r'"issue_id"\s*:\s*"([^"]+)"', text)
    if match and match.group(1) in valid_ids:
        return match.group(1)

    for issue_id in valid_ids:
        if re.search(rf"\b{re.escape(issue_id)}\b", text):
            return issue_id

    return None


def _call_llm(description: str, candidates: list) -> Optional[str]:
    """Ask the LLM to pick ONE issue_id from `candidates`, or None if it
    doesn't clearly match any of them."""
    if not candidates:
        return None

    valid_ids = {c["issue_id"] for c in candidates}
    options_text = "\n".join(
        f'- "{c["issue_id"]}": {c["description"]}' for c in candidates
    )

    prompt = f"""
A customer support ticket did not match any structured rule. Based on the
customer's own words below, decide if it clearly matches ONE of these
known issue types, or none.

Known issue types:
{options_text}

Customer's description:
{description}

Return JSON only in this format:
{{"issue_id": "<one_of_the_ids_above_or_NONE>"}}
""".strip()

    try:
        content = _chat([
            {
                "role": "system",
                "content": (
                    "You are a strict support-ticket issue classifier. "
                    'Reply with only one JSON object: {"issue_id":"<id_or_NONE>"}'
                ),
            },
            {"role": "user", "content": prompt},
        ])
        return _parse_issue_id(content, valid_ids)
    except Exception as exc:
        print("LLM FALLBACK CLASSIFY ERROR:", type(exc).__name__, str(exc))
        return None


def classify_and_resolve(row: dict, catalog: list) -> Optional[dict]:
    """
    Entry point. Returns a decision dict (same shape as
    services.rules.match_rule's return value: decision, rule_id,
    response_en, response_ar, ticket_action) if an LLM-fallback issue
    matched AND its data_guard holds, else None -- caller should fall
    through to the generic NO_MATCH/ESCALATE in that case.
    """
    candidates = _eligible_candidates(catalog, row)
    if not candidates:
        return None

    description = row.get("sanitized_description") or row.get("cleaned_description") or ""
    if not description.strip():
        return None

    matched_id = _call_llm(description, candidates)
    if not matched_id:
        return None

    issue = next(c for c in candidates if c["issue_id"] == matched_id)
    # Re-check the guard -- belt and suspenders in case anything about
    # the row changed between building candidates and here.
    if not _guard_passes(issue.get("data_guard"), row):
        return None

    resp = issue["response"]
    return {
        "decision": "LLM_FALLBACK_ISSUE",
        "rule_id": issue["issue_id"],
        "response_en": resp.get("en", ""),
        "response_ar": resp.get("ar", ""),
        "ticket_action": resp.get("status") or "ESCALATE",
    }
