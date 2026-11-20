"""Helpers for MTO partner email (n8n or tests). State lives on agent_work.route."""

from __future__ import annotations

import re
from typing import Any


def parse_provider_from_facts(facts: str | None) -> str:
    if not facts:
        return ""
    match = re.search(r"(?:^|;\s*)provider=([^;]+)", str(facts), flags=re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip().upper()


def mto_to_address(provider: str, by_provider: dict[str, str], ops_fallback: str) -> str:
    key = (provider or "").strip().upper()
    if key and by_provider.get(key):
        return by_provider[key]
    return ops_fallback


def build_mto_email_body(intro: str, transaction_ids: list[str]) -> str:
    lines = [t for t in transaction_ids if (t or "").strip()]
    return (intro or "").rstrip() + "\n" + "\n".join(lines) + ("\n" if lines else "")


def build_mto_email_for_row(
    row: dict[str, Any],
    *,
    by_provider: dict[str, str],
    ops_email: str,
    subject: str,
    body_intro: str,
) -> dict[str, str]:
    provider = parse_provider_from_facts(row.get("facts"))
    txn = (row.get("transaction_id") or "").strip()
    to_email = mto_to_address(provider, by_provider, ops_email)
    return {
        "provider": provider,
        "to_email": to_email,
        "cc_email": ops_email,
        "subject": subject,
        "body": build_mto_email_body(body_intro, [txn]),
    }


def fetch_pending_mto_sql(
    target_table: str,
    pending_routes: tuple[str, ...],
    *,
    limit: int = 50,
) -> str:
    route_list = ", ".join("'" + r.replace("'", "''") + "'" for r in pending_routes)
    return f"""
SELECT
      sr_number
    , transaction_id
    , route
    , facts
    , ticket_status
    , updated_at
FROM {target_table}
WHERE model = 'tickets-engine'
  AND route IN ({route_list})
  AND ticket_status = 'Human review needed'
ORDER BY updated_at ASC
LIMIT {int(limit)}
""".strip()


def update_route_after_send_sql(
    target_table: str,
    sr_number: str,
    sent_route: str,
) -> str:
    safe_sr = str(sr_number).replace("'", "''")
    safe_route = str(sent_route).replace("'", "''")
    return f"""
UPDATE {target_table}
SET route = '{safe_route}', updated_at = NOW()
WHERE sr_number = '{safe_sr}'
""".strip()
