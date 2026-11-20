# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json
import os
import re
import threading

import requests

from config import (
    LITELLM_API_KEY,
    LITELLM_BASE_URL,
    LITELLM_MODEL,
)


ALLOWED_LABELS = {
    "delay",
    "refund_reason",
    "refund",
    "amend_required",
    "reject_reason",
    "financial",
    "unavailable_transaction",
    "other",
}


def _parse_label(content: str) -> str:
    """Extract label from model text (JSON or bare label)."""
    if not content:
        return "other"
    text = content.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    try:
        data = json.loads(text)
        label = data.get("label", "other")
        return label if label in ALLOWED_LABELS else "other"
    except json.JSONDecodeError:
        pass
    match = re.search(
        r'"label"\s*:\s*"(delay|refund_reason|refund|amend_required|reject_reason|financial|unavailable_transaction|other)"',
        text,
    )
    if match:
        return match.group(1)
    for label in ALLOWED_LABELS:
        if re.search(rf"\b{re.escape(label)}\b", text):
            return label
    return "other"

_SETTINGS_LOCK = threading.Lock()
_SETTINGS_CACHE = None  # (base, key) once resolved


def _resolve_base_and_key():
    """Resolve URL / key. In config.py, LITELLM_BASE_URL is an Airflow
    Connection id and LITELLM_API_KEY may be an Airflow Variable name."""
    base = LITELLM_BASE_URL
    key = LITELLM_API_KEY
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
    return base.rstrip("/"), key


def _litellm_settings():
    """Same result as before, but the Airflow Connection / Variable lookups
    (metadata-DB round trips) run once per process instead of once per LLM
    call. A failed lookup is not cached, so it is retried next call."""
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is None:
        with _SETTINGS_LOCK:
            if _SETTINGS_CACHE is None:
                _SETTINGS_CACHE = _resolve_base_and_key()
    base, key = _SETTINGS_CACHE
    model = os.getenv("LITELLM_MODEL") or LITELLM_MODEL
    return base, key, model


_HTTP = threading.local()


def _http_session() -> requests.Session:
    """One keep-alive session per thread: reuses the TCP/TLS connection to the
    LiteLLM proxy instead of a new handshake on every call."""
    session = getattr(_HTTP, "session", None)
    if session is None:
        session = requests.Session()
        _HTTP.session = session
    return session


def _chat(messages, temperature=0, max_tokens=256, timeout=60) -> str:
    """Call the LiteLLM proxy (OpenAI-compatible) over plain HTTP. No extra libraries."""
    base, api_key, model = _litellm_settings()
    url = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")

    r = _http_session().post(
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


def classify_problem_description(text: str) -> str:
    if not text:
        return "other"

    prompt = f"""
Classify this support complaint into exactly one label.

Allowed labels:
- delay
- refund_reason
- refund
- amend_required
- reject_reason
- financial
- unavailable_transaction
- other

Guidance:
- delay: customer says the transfer did not arrive yet, is late, or delayed. Also applies
  when the customer is following up on a transfer, asking about its status, asking where
  their money is, or their message implies they are waiting for it to arrive — even if
  they do not use the word "delay" (e.g. "send my money to pakistan", "where is my
  transfer", "any update on my transaction", "when will it arrive").
- unavailable_transaction: customer says the transaction is unavailable / not available /
  not found / not showing / cannot be viewed or selected / غير متاحة / غير موجودة /
  لا تظهر في التطبيق — distinct from a general delay follow-up when the core issue is
  that the txn itself is unavailable.
- refund_reason: customer asks WHY the transfer was refunded, reversed, or returned —
  they want an explanation for the refund.
- refund: customer is asking to GET their money back, requesting a refund, or asking
  when the amount will be returned — without asking why.
- amend_required: customer needs to edit, correct, complete, or update beneficiary or
  transaction information.
- reject_reason: customer is asking why the transaction was rejected or declined, or
  wants an explanation for the rejection.
- financial: the complaint is fundamentally about a financial matter related to a
  transaction or amount — meaning the customer is disputing a charge, asking about a
  specific transaction amount, reporting a missing or incorrect amount, asking about
  a refund or reversal of a payment, or referencing money movement on their account.
  This applies when the core concern is monetary even if the customer does not use
  technical terms.
  Key Arabic indicators: المبلغ، ارجاع، استرداد، عملية، العمليه، استرجاع، فلوسي،
  حسابي، عمليه، مبلغ، إرجاع، بمبلغ.
  Key English indicators: Amount, Refund, Transaction, Money, Return, Valid, Reverse.
  Important: only use this label when the complaint is primarily about a card-related
  financial issue. Do NOT use it for transfer delay or refund requests that clearly
  belong to the delay, refund, or refund_reason labels.
- other: anything else that does not fit the above, including greetings or unrelated
  messages.

Return JSON only in this format:
{{"label": "<one_label>"}}

Complaint:
{text}
""".strip()

    # No try/except on purpose: if the LLM call fails, the ticket is marked
    # ok=False and the DAG turns red instead of silently returning "other".
    # Avoid response_format=json_object: Groq often returns
    # json_validate_failed for gpt-oss models.
    content = _chat([
        {
            "role": "system",
            "content": (
                "You are a strict support complaint classifier. "
                'Reply with only one JSON object: {"label":"<one_label>"}'
            ),
        },
        {"role": "user", "content": prompt},
    ])
    return _parse_label(content)

