import os

# ─────────────────────────────────────────────
# PRODUCT MAP
# Maps CRM CategoryLevel1_c values to internal product keys
# ─────────────────────────────────────────────
PRODUCT_MAP = {
    "international transfer": "rmt",
    "cards": "cards",
    # later more products to be added
}

# ─────────────────────────────────────────────
# LiteLLM OpenAI-compatible proxy 
# client = openai.OpenAI(api_key=..., base_url=...)
# ─────────────────────────────────────────────
LITELLM_BASE_URL = "litellm_clearing_tickets"
LITELLM_API_KEY = "litellm_clearing_tickets_token"
LITELLM_MODEL = "gpt-oss-20b"

# Airflow Variable name (optional). Notebook / local uses LITELLM_API_KEY above.


# ─────────────────────────────────────────────
# Warehouse DB (notebook bulk job)
# Same endpoint as Airflow connection gcp_mysql
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
# Bulk notebook run params
# ─────────────────────────────────────────────
LIMIT = 100
BATCH_SIZE = 20
PRODUCT = "cards"
PRODUCT_IR = "rmt" # ignore - the eother notebook
LOOKBACK_DAYS = 3
INCLUDE_RESOLVED = True

# True = extract only RMT tickets that can hit rule 7 (MTO email flow).
# Filters warehouse: SUCCESS + RECEIVED + WILL_CALL/TRANSFERPAID + MG/WU.
TEST_EMAIL_FLOW = False

# Run from an Excel export instead of SQL extract (notebook).
USE_SAMPLE_EXCEL = False
SAMPLE_EXCEL_PATH = (
     "/Users/saadalrukhaimi/My/Clearing Backlog of Tickets-rely/"
    "clearing-tickets-test-pr-test2/cards_sample_tickets.xlsx"
)

SAMPLE_EXCEL_PATH_IR = (
      "/Users/saadalrukhaimi/My/Clearing Backlog of Tickets-rely/"
      "clearing-tickets-test-pr-test2/ir_sample_data_test.xlsx"
 )
SAMPLE_EXCEL_DATE = ""  # "2026-08-31" = one day; "" = all rows
SAMPLE_EXCEL_LIMIT = 2000  # 0 = no cap after date filter
SAMPLE_EXCEL_ENRICH_CRM = True
SAMPLE_EXCEL_FORMAT = "auto"  # required for notebook import

# ── MTO partner email (n8n sends; state on agent_work.route) ──
MTO_ROUTE_CASE1 = "mto/email/wait/case1"   # IR rules 7–8
MTO_ROUTE_CASE2 = "mto/email/wait/case2"   # IR rule 9
MTO_ROUTE_CASE1_SENT = "mto/email/sent/case1"
MTO_ROUTE_CASE2_SENT = "mto/email/sent/case2"
MTO_ROUTES_PENDING = (MTO_ROUTE_CASE1, MTO_ROUTE_CASE2)
MTO_ROUTE_AFTER_SEND = {
    MTO_ROUTE_CASE1: MTO_ROUTE_CASE1_SENT,
    MTO_ROUTE_CASE2: MTO_ROUTE_CASE2_SENT,
}

MTO_EMAIL_BY_PROVIDER = {
    "MG": "hfaloqaily@jake.com",
    "WU": "hfaloqaily@jake.com",
}
MTO_OPS_EMAIL = "hfaloqaily@jake.com"
MTO_EMAIL_SUBJECT = "jake Remittance Transaction Status Confirmation"
MTO_EMAIL_BODY_INTRO = (
    "Dears,\n"
    "Kindly confirm the status of the transactions below:\n\n"
)

TICKETS_TABLE = "polaris.`base.oci_crm_db`.all_service_requests"
# Matched rules → agent work (existing table, INSERT only)
TARGET_TABLE = "jake_exploration.tickets_agent__agent_work"
ENSURE_TABLE = False  # never create/alter agent_work
# Non-matched tickets → backlog table
NO_MATCH_TABLE = "jake_exploration.clearing_backlog__service_work"
ENSURE_NO_MATCH_TABLE = True  # create backlog table if missing

 