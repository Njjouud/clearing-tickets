import os

# ─────────────────────────────────────────────
# PRODUCT MAP
# Maps CRM CategoryLevel1_c values to internal product keys
# ─────────────────────────────────────────────
PRODUCT_MAP = {
    "international transfer": "rmt",
    "cards": "cards",
    "account and registration": "accounts",
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

# Products whose tickets have no TransactionID_c: extracted by User ID
# (BaraqInternalID_c) instead, and only when the DAG is triggered with
# that product explicitly (product="" keeps the old cards + rmt run).
NO_TRANSACTION_PRODUCTS = ("accounts",)

TICKETS_TABLE = "polaris.`base.oci_crm_db`.all_service_requests"
# Matched rules → agent work (existing table, INSERT only)
TARGET_TABLE = "jake_exploration.tickets_agent__agent_work"
ENSURE_TABLE = False  # never create/alter agent_work
# Non-matched tickets → backlog table
NO_MATCH_TABLE = "jake_exploration.clearing_backlog__service_work"
ENSURE_NO_MATCH_TABLE = True  # create backlog table if missing

 
# ─────────────────────────────────────────────
# Accounts DAG (accounts_clearing_tickets.py / clearing_backlog_accounts)
# ─────────────────────────────────────────────
ACCOUNTS_OCI_CATEGORY = "account and registration"  # LOWER(TRIM(CategoryLevel1_c))
ACCOUNTS_TARGET_TABLE = TARGET_TABLE                # matched (routed) tickets
ACCOUNTS_NO_MATCH_TABLE = NO_MATCH_TABLE            # not in the routing matrix

# Sprinklr cases for Account Operations. Disabled while "table" is None.
# Fill "table" and every column marked REQUIRED to switch it on; the DAG
# renames these columns to the OCI names so the same engine/rules apply.
SPRINKLR_ACCOUNTS = {
    "table": None,  # e.g. "polaris.`base.sprinklr_db`.cases"
    "category_column": "universal_case_custom_property_32",
    "category_value": "Account Operations",
    "columns": {             # OCI name          -> Sprinklr column
        "SrNumber": None,            # REQUIRED  case number
        "BaraqInternalID_c": None,   # REQUIRED  customer User ID
        "CreationDate": None,        # REQUIRED  case created date
        "CategoryLevel2_c": None,    # subcategory, e.g. "Account closure request"
        "CategoryLevel3_c": None,
        "ProblemDescription": None,
        "StatusTypeCd": None,        # case status (used to skip resolved)
        "StatusTypeCdMeaning": None,
        "QueueName": None,
        "SrId": None,
        "TransactionID_c": None,
    },
    "resolved_values": [],   # StatusTypeCd values that mean resolved/closed
}
