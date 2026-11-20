# Bulk clearing tickets — full folder
# Bulk clearing tickets — full folder

Drop this into your `clearing-tickets-test-copy` project root, overwriting
matching paths. Files NOT included below are untouched by anything in this
conversation — keep your existing copies as-is:

  - config.py
  - dag_pipeline/write_service_work.py
  - utils/  (cleansing_utils.py, llm_utils.py, datetime_utils.py, excel_tickets.py)
  - requirements.txt / your conda env

## What's in this folder

services/
├── sql/
│   ├── cards/
│   │   ├── fetch_cards_base.sql
│   │   ├── fetch_cards_dispute.sql
│   │   ├── fetch_cards_balance.sql
│   │   ├── fetch_cards_not_working.sql
│   │   └── fetch_cards_delivery.sql
│   └── rmt/
│       └── fetch_rmt_transaction.sql
├── products/
│   ├── base.py
│   ├── cards.py
│   └── remittance.py
├── orchestrator.py
├── product_services.py
├── classifier.py
└── rules.py


## What changed in product_services.py

- `_run_query` / `_as_flag` / `_row_get` renamed to `run_query` / `as_flag` /
  `row_get` (public — now imported by services/products/*.py too).
- `fetch_product_transaction()` and `build_product_row()` REMOVED — their
  logic now lives per-product in `services/products/cards.py` and
  `services/products/remittance.py`.
- `fetch_ticket_response()` KEPT as a one-line backward-compatible wrapper
  around `orchestrator.resolve_ticket()`, so nothing importing it elsewhere
  breaks.
- `build_decision_response()` and the placeholder/name-filling helpers are
  UNCHANGED — still the shared, product-agnostic response composition layer.

## Fixes riding along in this refactor

1. **RMT `amend_required`** now coerced through the same int-flag logic as
   the other boolean fields (previously raw from the warehouse — a type
   mismatch was likely silently killing rules 3/4).
2. **Cards `provision_status_within_two_weeks`** — same fix (was likely
   silently killing rules 5/6/7).
3. **Both SQL files** — forced ownership check so a ticket referencing a
   `TransactionID_c` that doesn't belong to the ticket's own user can no
   longer pull back someone else's transaction data. Fails closed (returns
   nothing) rather than fails open.

## Verify before running

- Test each SQL file against a ticket+transaction pair you know is
  legitimately the same user (should still return data), and against a
  manufactured mismatch (should return zero rows).
- RMT: confirm `accounts.account_no` is really what's stored in
  `b_account_id` (I inferred this — if it's actually `accounts.id`, swap
  the join in fetch_rmt_transaction.sql).

## Adding a new product later

1. `services/products/<name>.py` — subclass `ProductEngine`, implement the
   4 abstract methods (`fetch_transaction`, `build_row`, `get_rules`, `match`).
2. One line in `services/orchestrator.py`: `register_engine(<Name>Engine())`.
3. Add the category key to `config.PRODUCT_MAP` so `classify_product()`
   routes to it.

Nothing else changes.

## Rule-based → LLM-fallback issue cascade (NEW)

Added a second classification pass that runs **only when the
deterministic rules find nothing** (`services.rules.match_rule` returns
`NO_MATCH`). This handles cases where the customer's own wording doesn't
map cleanly onto `CategoryLevel2_c`/`CategoryLevel3_c` — e.g. a
subscription complaint filed as a generic "Transaction Dispute."

```
services/rules.py                 -- unchanged: deterministic pass, tried first
services/llm_fallback_issues.py    -- NEW: generic fallback mechanics
services/issue_catalogs/
  cards_llm_fallback.py            -- NEW: cards' closed list of LLM-eligible issues
services/products/cards.py         -- match() now: rules first, LLM-fallback second
```

**Safety properties, same spirit as the rest of this engine:**
- The LLM only ever picks from a fixed, per-call list of `issue_id`s — anything else is treated as no match.
- Each issue has a `data_guard` (deterministic facts already on the row) that must independently hold true — the LLM can suggest a category, it can never make that category true on its own.
- Response text is still a frozen template per issue (same pattern as `rules.py`) — the LLM composes nothing here.
- Candidates are filtered by `category_filter` + `data_guard` **before** the LLM is even called — a ticket with no eligible candidate never spends an LLM call at all.

**First issue in the catalog**: `subscription_still_active_no_stop` —
fires only when `CategoryLevel2_c = 'Transaction Dispute'` AND
`has_stop_record = 0` (no CANCELED/STOPPED record found for that
merchant+cif at all), AND the LLM confirms the customer's wording
actually describes a stopped-subscription complaint. Tells the customer
the subscription is still active with no stop on file.

**Client confirmed against your actual `utils/llm_utils.py`**: uses the
same OpenAI SDK client pointed at the LiteLLM proxy (`OpenAI(api_key=...,
base_url=...)`), the same API-key resolution order (Airflow Variable →
`GROQ_API_KEY_VAR` env → `LITELLM_API_KEY` env → config default), and the
same `chat.completions.create(...)` call shape as
`classify_problem_description()`. No new dependency (`openai` was already
in use) — the earlier `requests`-based version has been replaced.

**New `has_stop_record` field**: distinct from `charged_after_stop` — 1 if
ANY stop/cancel record exists for the merchant+cif at all, regardless of
whether the disputed charge came before or after it. Added alongside
`charged_after_stop` in `fetch_cards_transaction.sql`'s `subscription_stop`
CTE output.

## Rule 15 (cards) — charged after subscription stop

New: `fetch_cards_transaction.sql` now also resolves `charged_after_stop`
via `card_hub_app_db__subscription_payment_instruction` — joined on
`card_acceptor_id` + `cif` (ownership-safe). Detects the specific pattern
"customer stopped a subscription (Netflix-style) but got charged after."

**ASSUMPTION to verify**: uses `modified_date` on that table as the
stop-effective timestamp (when `status` last changed to CANCELED/STOPPED)
compared against `authorization_transaction.created_date` as the charge
timestamp. Neither was explicitly confirmed as the "real" date column —
verify against a known case before trusting this in production.

Rule 15 fires at priority 0 (before everything else) when
`CategoryLevel2_c = 'Transaction Dispute'` and `charged_after_stop = 1`.
Routes to **ESCALATE** (not auto-closed) since this is a billing anomaly
that may need a real refund — a human should confirm the reversal, not
the rules engine.

## Rule 13 split (cards) — card-network-aware routing

`fetch_cards_transaction.sql` now resolves `card_network` (`MADA` /
`VISA` / `NULL` if unresolved) via
`card_subscription_order.alias -> card_product_id -> card_product.type`
(`CLASSIC` = Mada, anything else = Visa — GCC is NOT distinguishable with
this data model yet).

The old single rule 13 ("needs review, generic") is now two rules:
- **Rule 13** — Mada → routes to Cortex clearing
- **Rule 14** — Visa or unresolved network → asks for supporting documents
  (NOT "escalate to CMS with proof attached" — document-attachment status
  isn't tracked anywhere queryable yet, so this defaults to the safer
  "ask for docs" branch rather than assuming proof already exists)

A row where `card_network` is `NULL` (no subscription-order match found)
matches neither rule and falls through to the generic NO_MATCH escalate —
intentional fail-closed behavior rather than guessing a network.

## Known gaps still open (from the earlier review — not yet fixed in code)

- **GCC card network** — not distinguishable from Visa with current data;
  needs its own signal if the business wants a third route.
- **Document-attached status** — not tracked anywhere queryable; rule 14
  can't yet split into "docs provided → CMS" vs "no docs → ask for docs."
- Cards rules 10–12 (Card Delivery, shipped/not-shipped/elapsed-days) are
  still stub/empty — the underlying data (days elapsed, shipped flag,
  carrier tracking) isn't in `build_row` yet.
- Cards rule 9 ("no card issues") still has no financial-keyword screen —
  playbook requires excluding tickets mentioning refund/amount/etc. before
  auto-closing.
- Cards rule 13 (settle/dispute review) still doesn't branch by card type
  (Visa/Mada/GCC) or document-attached status.
- RMT rule 1 still doesn't check complaint type (delay vs. wrong-details
  vs. non-credit-amount) before firing the generic "completed" message.
- Several playbook branches (Terrapay hold, amend=0, the full "In Progress"
  mto-status tree) aren't implemented as rules yet.

## What's in this folder

services/
├── sql/
│   ├── cards/
│   │   ├── fetch_cards_base.sql
│   │   ├── fetch_cards_dispute.sql
│   │   ├── fetch_cards_balance.sql
│   │   ├── fetch_cards_not_working.sql
│   │   └── fetch_cards_delivery.sql
│   └── rmt/
│       └── fetch_rmt_transaction.sql
├── products/
│   ├── base.py
│   ├── cards.py
│   └── remittance.py
├── orchestrator.py
├── product_services.py
├── classifier.py
└── rules.py


## What changed in product_services.py

- `_run_query` / `_as_flag` / `_row_get` renamed to `run_query` / `as_flag` /
  `row_get` (public — now imported by services/products/*.py too).
- `fetch_product_transaction()` and `build_product_row()` REMOVED — their
  logic now lives per-product in `services/products/cards.py` and
  `services/products/remittance.py`.
- `fetch_ticket_response()` KEPT as a one-line backward-compatible wrapper
  around `orchestrator.resolve_ticket()`, so nothing importing it elsewhere
  breaks.
- `build_decision_response()` and the placeholder/name-filling helpers are
  UNCHANGED — still the shared, product-agnostic response composition layer.

## Fixes riding along in this refactor

1. **RMT `amend_required`** now coerced through the same int-flag logic as
   the other boolean fields (previously raw from the warehouse — a type
   mismatch was likely silently killing rules 3/4).
2. **Cards `provision_status_within_two_weeks`** — same fix (was likely
   silently killing rules 5/6/7).
3. **Both SQL files** — forced ownership check so a ticket referencing a
   `TransactionID_c` that doesn't belong to the ticket's own user can no
   longer pull back someone else's transaction data. Fails closed (returns
   nothing) rather than fails open.

## Verify before running

- Test each SQL file against a ticket+transaction pair you know is
  legitimately the same user (should still return data), and against a
  manufactured mismatch (should return zero rows).
- RMT: confirm `accounts.account_no` is really what's stored in
  `b_account_id` (I inferred this — if it's actually `accounts.id`, swap
  the join in fetch_rmt_transaction.sql).

## Adding a new product later

1. `services/products/<name>.py` — subclass `ProductEngine`, implement the
   4 abstract methods (`fetch_transaction`, `build_row`, `get_rules`, `match`).
2. One line in `services/orchestrator.py`: `register_engine(<Name>Engine())`.
3. Add the category key to `config.PRODUCT_MAP` so `classify_product()`
   routes to it.

Nothing else changes.

## Rule-based → LLM-fallback issue cascade (NEW)

Added a second classification pass that runs **only when the
deterministic rules find nothing** (`services.rules.match_rule` returns
`NO_MATCH`). This handles cases where the customer's own wording doesn't
map cleanly onto `CategoryLevel2_c`/`CategoryLevel3_c` — e.g. a
subscription complaint filed as a generic "Transaction Dispute."

```
services/rules.py                 -- unchanged: deterministic pass, tried first
services/llm_fallback_issues.py    -- NEW: generic fallback mechanics
services/issue_catalogs/
  cards_llm_fallback.py            -- NEW: cards' closed list of LLM-eligible issues
services/products/cards.py         -- match() now: rules first, LLM-fallback second
```

**Safety properties, same spirit as the rest of this engine:**
- The LLM only ever picks from a fixed, per-call list of `issue_id`s — anything else is treated as no match.
- Each issue has a `data_guard` (deterministic facts already on the row) that must independently hold true — the LLM can suggest a category, it can never make that category true on its own.
- Response text is still a frozen template per issue (same pattern as `rules.py`) — the LLM composes nothing here.
- Candidates are filtered by `category_filter` + `data_guard` **before** the LLM is even called — a ticket with no eligible candidate never spends an LLM call at all.

**First issue in the catalog**: `subscription_still_active_no_stop` —
fires only when `CategoryLevel2_c = 'Transaction Dispute'` AND
`has_stop_record = 0` (no CANCELED/STOPPED record found for that
merchant+cif at all), AND the LLM confirms the customer's wording
actually describes a stopped-subscription complaint. Tells the customer
the subscription is still active with no stop on file.

**Client confirmed against your actual `utils/llm_utils.py`**: uses the
same OpenAI SDK client pointed at the LiteLLM proxy (`OpenAI(api_key=...,
base_url=...)`), the same API-key resolution order (Airflow Variable →
`GROQ_API_KEY_VAR` env → `LITELLM_API_KEY` env → config default), and the
same `chat.completions.create(...)` call shape as
`classify_problem_description()`. No new dependency (`openai` was already
in use) — the earlier `requests`-based version has been replaced.

**New `has_stop_record` field**: distinct from `charged_after_stop` — 1 if
ANY stop/cancel record exists for the merchant+cif at all, regardless of
whether the disputed charge came before or after it. Added alongside
`charged_after_stop` in `fetch_cards_transaction.sql`'s `subscription_stop`
CTE output.

## Rule 15 (cards) — charged after subscription stop

New: `fetch_cards_transaction.sql` now also resolves `charged_after_stop`
via `card_hub_app_db__subscription_payment_instruction` — joined on
`card_acceptor_id` + `cif` (ownership-safe). Detects the specific pattern
"customer stopped a subscription (Netflix-style) but got charged after."

**ASSUMPTION to verify**: uses `modified_date` on that table as the
stop-effective timestamp (when `status` last changed to CANCELED/STOPPED)
compared against `authorization_transaction.created_date` as the charge
timestamp. Neither was explicitly confirmed as the "real" date column —
verify against a known case before trusting this in production.

Rule 15 fires at priority 0 (before everything else) when
`CategoryLevel2_c = 'Transaction Dispute'` and `charged_after_stop = 1`.
Routes to **ESCALATE** (not auto-closed) since this is a billing anomaly
that may need a real refund — a human should confirm the reversal, not
the rules engine.

## Rule 13 split (cards) — card-network-aware routing

`fetch_cards_transaction.sql` now resolves `card_network` (`MADA` /
`VISA` / `NULL` if unresolved) via
`card_subscription_order.alias -> card_product_id -> card_product.type`
(`CLASSIC` = Mada, anything else = Visa — GCC is NOT distinguishable with
this data model yet).

The old single rule 13 ("needs review, generic") is now two rules:
- **Rule 13** — Mada → routes to Cortex clearing
- **Rule 14** — Visa or unresolved network → asks for supporting documents
  (NOT "escalate to CMS with proof attached" — document-attachment status
  isn't tracked anywhere queryable yet, so this defaults to the safer
  "ask for docs" branch rather than assuming proof already exists)

A row where `card_network` is `NULL` (no subscription-order match found)
matches neither rule and falls through to the generic NO_MATCH escalate —
intentional fail-closed behavior rather than guessing a network.

## Known gaps still open (from the earlier review — not yet fixed in code)

- **GCC card network** — not distinguishable from Visa with current data;
  needs its own signal if the business wants a third route.
- **Document-attached status** — not tracked anywhere queryable; rule 14
  can't yet split into "docs provided → CMS" vs "no docs → ask for docs."
- Cards rules 10–12 (Card Delivery, shipped/not-shipped/elapsed-days) are
  still stub/empty — the underlying data (days elapsed, shipped flag,
  carrier tracking) isn't in `build_row` yet.
- Cards rule 9 ("no card issues") still has no financial-keyword screen —
  playbook requires excluding tickets mentioning refund/amount/etc. before
  auto-closing.
- Cards rule 13 (settle/dispute review) still doesn't branch by card type
  (Visa/Mada/GCC) or document-attached status.
- RMT rule 1 still doesn't check complaint type (delay vs. wrong-details
  vs. non-credit-amount) before firing the generic "completed" message.
- Several playbook branches (Terrapay hold, amend=0, the full "In Progress"
  mto-status tree) aren't implemented as rules yet.
