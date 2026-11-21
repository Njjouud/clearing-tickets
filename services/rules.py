# Allow imports when Airflow accidentally parses this file as a DAG
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import os
import re
from functools import lru_cache

import pandas as pd
from utils.datetime_utils import check_last_24h
from utils.llm_utils import classify_problem_description


def _debug_enabled() -> bool:
    return os.getenv("MATCH_DEBUG", "").strip().lower() in {"1", "true", "yes"}


def _dbg(*args, **kwargs):
    if _debug_enabled():
        print(*args, **kwargs)


def load_static_rules(product: str):
    """Rules are static, so the DataFrame is built once per process and reused.
    Callers only read it (match_rule sorts into a copy, cards uses .loc)."""
    return _load_static_rules_cached(product)


@lru_cache(maxsize=None)
def _load_static_rules_cached(product: str):
    if product == "rmt":
        return _load_rmt_rules()
    
    if product == "cards":
        return _load_cards_rules()

    if product == "accounts":
        return _load_accounts_rules()

    raise ValueError(f"No rules defined for product: '{product}'")


def _load_rmt_rules():
    rules = [
        {
            "rule_id": 1,
            "conditions": {
                "jake_status": ["SUCCES", "SUCCESS"],
                "mto_transaction_status": [
                    "3000",
                    "70000",
                    "DELIVERED",
                    "transferDeliveredToBank"
                ],
                "transaction_date_time": "LAST_24H",
                "delivery_option": [
                    "BANK_DEPOSIT",
                    "DIRECT_TO_ACCT"
                ]
            },
            "problem_label": "delay",
            "response": {
                "en": "Welcome {first_name}, We want to inform you that the transaction was completed. Get ready for a unique experience like no other.",
                "ar": "حياك الله {first_name} في برق، نود إبلاغك بأن العملية تم تنفيذها بنجاح، استعد لتجربة مميزة مالها آخر",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 1
        },

        {
            "rule_id": 2,
            "conditions": {
                "jake_status": "REFUNDED",
                "compliance_status": ["ACCEPTED", "HOLD_ACCEPTED"],
                "core_banking_status": "REVERSAL",
                "delivery_option": [
                    "BANK_DEPOSIT",
                    "DIRECT_TO_ACCT",
                    "WILL_CALL"
                ]
            },
            "problem_exclude_labels": ["refund_reason"],
            "response": {
                "en": "Welcome {first_name}, The transaction amount has been successfully returned to your wallet.",
                "ar": "حياك الله {first_name} في برق، تم استرجاع مبلغ العملية بنجاح إلى محفظتك.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 2
        },

        {
            "rule_id": 3,
            "conditions": {
                "jake_status": "ON_HOLD",
                "mto_transaction_status": "PROCESSING",
                "amend_required": 1,
                "delivery_option": [
                    "BANK_DEPOSIT",
                    "DIRECT_TO_ACCT"
                ],
                "provider": "MG"
            },
            "response": {
                "en": "Dear {first_name},\nYou have a pending transaction that requires your attention. To complete the process, please follow the steps below:\n\n1- Ensure you have the latest version of the jake app installed\n2- Open the jake app\n3- You can complete the pending transaction from the Home Page or by following the steps below:\n4- Navigate to International Transfer\n5- Select the pending transaction and complete all the required details\n\nImportant:\nPlease make sure to enter the Receiver's date of birth exactly as it appears on their national ID, and all information should be written in English.",
                "ar": "حياك الله {first_name} في برق،\nلديك عملية تحويل معلّقة، لإكمال العملية يُرجى اتباع الخطوات التالية:\n\n1. تأكد من أن لديك أحدث إصدار من تطبيق برق.\n2. افتح تطبيق برق.\n3. يمكنك إتمام العملية المعلّقة من الصفحة الرئيسية أو باتباع الخطوات التالية:\n4. الانتقال إلى التحويل الدولي.\n5. اختيار العملية المعلقة ثم إكمال جميع البيانات المطلوبة.\n\nمهم:\nيرجى التأكد من إدخال تاريخ ميلاد المستفيد تمامًا كما هو مذكور في هويته الوطنية، وأن تكون جميع البيانات مكتوبة باللغة الإنجليزية.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 3
        },

        {
            "rule_id": 4,
            "conditions": {
                "jake_status": "ON_HOLD",
                "mto_transaction_status": ["20110", "20000", "50000"],
                "amend_required": 1,
                "delivery_option": ["BANK_DEPOSIT", "DIRECT_TO_ACCT"],
                "provider": "Thunes"
            },
            "response": {
                "en": "Dear {first_name},\nYou have a pending transaction that requires your attention. To complete the process, please follow the steps below:\n\n1- Ensure you have the latest version of the jake app installed\n2- Open the jake app\n3- You can complete the pending transaction from the Home Page or by following the steps below:\n4- Navigate to International Transfer\n5- Select the pending transaction and complete all the required details\n\nImportant:\nPlease make sure to enter the Receiver's date of birth exactly as it appears on their national ID, and all information should be written in English.",
                "ar": "حياك الله {first_name} في برق،\nلديك عملية تحويل معلّقة، لإكمال العملية يُرجى اتباع الخطوات التالية:\n\n1. تأكد من أن لديك أحدث إصدار من تطبيق برق.\n2. افتح تطبيق برق.\n3. يمكنك إتمام العملية المعلّقة من الصفحة الرئيسية أو باتباع الخطوات التالية:\n4. الانتقال إلى التحويل الدولي.\n5. اختيار العملية المعلقة ثم إكمال جميع البيانات المطلوبة.\n\nمهم:\nيرجى التأكد من إدخال تاريخ ميلاد المستفيد تمامًا كما هو مذكور في هويته الوطنية، وأن تكون جميع البيانات مكتوبة باللغة الإنجليزية.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 4
        },

        {
            "rule_id": 5,
            "conditions": {
                "jake_status": "FAILED",
                "core_banking_status": "REVERSAL"
            },
            "problem_label": ["refund", "delay"],
            "response": {
                "en": "Welcome {first_name}, The transaction amount has been successfully returned to your wallet.",
                "ar": "حياك الله {first_name} في برق، تم استرجاع مبلغ العملية بنجاح إلى محفظتك.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 5
        },

        {
            "rule_id": 6,
            "conditions": {
                "jake_status": "IN_PROGRESS",
                "mto_transaction_status": "REJECTED",
                "core_banking_status": "REVERSAL"
            },
            "problem_exclude_labels": ["reject_reason"],
            "response": {
                "en": "Welcome {first_name}, The transaction amount has been successfully returned to your wallet.",
                "ar": "حياك الله {first_name} في برق، تم استرجاع مبلغ العملية بنجاح إلى محفظتك.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 6
        },

        {
            "rule_id": 7,
            "conditions": {
                "jake_status": ["SUCCES", "SUCCESS"],
                "mto_transaction_status": ["RECEIVED", "Received", "received"],
                "delivery_option": ["WILL_CALL"],
                "provider": "MG",
            },
            "problem_label": "unavailable_transaction",
            "response": {
                "en": (
                    "Welcome {first_name}, we are verifying your transfer status with our "
                    "partner for transaction {TransactionID_c}. We will update you as soon "
                    "as we have confirmation."
                ),
                "ar": (
                    "حياك الله {first_name} في برق، جاري التحقق من حالة عملية التحويل "
                    "{TransactionID_c} مع شريكنا، وسنوافيك بالتحديث فور استلام التأكيد."
                ),
                "status": "ESCALATE",
            },
            "priority": 7,
        },
        {
            "rule_id": 8,
            "conditions": {
                "jake_status": ["SUCCES", "SUCCESS"],
                "mto_transaction_status": [
                    "TRANSFERPAID",
                    "TRANSFER_PAID",
                    "transferPaid",
                    "TransferPaid",
                ],
                "delivery_option": ["WILL_CALL"],
                "provider": "WU",
            },
            "problem_label": "unavailable_transaction",
            "response": {
                "en": (
                    "Welcome {first_name}, we are verifying your transfer status with our "
                    "partner for transaction {TransactionID_c}. We will update you as soon "
                    "as we have confirmation."
                ),
                "ar": (
                    "حياك الله {first_name} في برق، جاري التحقق من حالة عملية التحويل "
                    "{TransactionID_c} مع شريكنا، وسنوافيك بالتحديث فور استلام التأكيد."
                ),
                "status": "ESCALATE",
            },
            "priority": 8,
        },
        {
            "rule_id": 9,
            "conditions": {
                "jake_status": ["IN_PROGRESS", "IN PROGRESS", "In Progress"],
                "mto_transaction_status": ["AVAILABLE", "Available", "available"],
                "delivery_option": ["WILL_CALL"],
                "provider": ["MG", "WU"],
            },
            "problem_label": "unable_to_collect_fund",
            "response": {
                "en": (
                    "Welcome {first_name}, we are checking with our partner regarding "
                    "collection of transaction {TransactionID_c}. We will update you shortly."
                ),
                "ar": (
                    "حياك الله {first_name} في برق، جاري التحقق مع شريكنا بخصوص استلام "
                    "مبلغ العملية {TransactionID_c}، وسنوافيك بالتحديث قريبًا."
                ),
                "status": "ESCALATE",
            },
            "priority": 9,
        },
    ]

    return pd.DataFrame(rules)


def _load_cards_rules():
    # rule_id == priority == list order (1..N). Lower number wins first.
    rules = [
        # ── RULE 1 ── Charged after subscription was stopped ───────────
        # High-confidence, data-verified signal: fires ahead of the
        # hold/refund/settle rules since it explains *why* the dispute
        # happened rather than just describing the transaction's current
        # state. ESCALATE (not auto-closed) -- this is a billing anomaly
        # that may need a refund, not something to close without a human
        # confirming the reversal.
        {
            "rule_id": 1,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "charged_after_stop": 1,
            },
            "response": {
                "en": (
                    "Welcome {first_name}, we've confirmed that you previously "
                    "stopped this subscription, but a charge was made afterward. "
                    "This has been escalated to our team to investigate and "
                    "process a refund if applicable."
                ),
                "ar": (
                    "حياك الله {first_name} في برق، تأكدنا من أنك قمت بإيقاف "
                    "هذا الاشتراك مسبقًا، إلا أنه تم تنفيذ عملية سحب بعد ذلك. "
                    "تم تصعيد هذا الأمر لفريقنا للتحقق ومعالجة الاسترداد إن "
                    "كان مستحقًا."
                ),
                "status": "ESCALATE"
            },
            "priority": 1
        },

        # ── RULE 2 ── Billed twice (confirmed in auth table) ───────────
        {
            "rule_id": 2,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": "Billed Twice for the Same Thing",
                "is_billed_twice": 1,
            },
            "response": {
                "en": (
                    "Welcome {first_name}, we found more than one matching "
                    "successful charge for this merchant and amount. This "
                    "duplicate-charge dispute has been escalated for review."
                ),
                "ar": (
                    "حياك الله {first_name} في برق، وجدنا أكثر من عملية ناجحة "
                    "مطابقة لنفس التاجر والمبلغ. تم تصعيد نزاع السحب المكرر "
                    "للمراجعة."
                ),
                "status": "ESCALATE"
            },
            "priority": 2
        },

        # ── RULE 3 ── Refunded (wallet REFUND/REVERSAL posted) ─────────
        # Card Dispute Flow booklet: static close when refunded.
        {
            "rule_id": 3,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": [
                    "Did not Receive Refund or Credit",
                    "A transaction you did not approve or make",
                    "ATM did not Dispense Cash",
                    "Billed Twice for the Same Thing",
                    "Did not Receive Service or Item",
                    "Payment Made Another Way",
                    "Transaction Was Cancelled",
                ],
                "wallet_refund_posted": 1,
            },
            "response": {
                "en": (
                    "Welcome to jake⚡️, the transaction amount has been "
                    "refunded to your wallet."
                ),
                "ar": (
                    "حياك الله في برق⚡️ , تم استرجاع مبلغ العملية لمحفظتك."
                ),
                "status": "CLOSE_RESOLVED"
            },
            "priority": 3
        },

        # ── RULE 4 ── Refunded (ledger txn_type REFUND/REVERSAL) ───────
        {
            "rule_id": 4,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": [
                    "Did not Receive Refund or Credit",
                    "A transaction you did not approve or make",
                    "ATM did not Dispense Cash",
                    "Billed Twice for the Same Thing",
                    "Did not Receive Service or Item",
                    "Payment Made Another Way",
                    "Transaction Was Cancelled",
                ],
                "txn_type": ["REFUND", "REVERSAL"],
            },
            "response": {
                "en": (
                    "Welcome to jake⚡️, the transaction amount has been "
                    "refunded to your wallet."
                ),
                "ar": (
                    "حياك الله في برق⚡️ , تم استرجاع مبلغ العملية لمحفظتك."
                ),
                "status": "CLOSE_RESOLVED"
            },
            "priority": 4
        },

        # ── RULE 5 ── On-hold / unsettled (booklet static 32-day close)
        # Hold accounts only: 120316, 120317, 120318. Same message for all
        # dispute L3s including unauthorized.
        {
            "rule_id": 5,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": [
                    "Did not Receive Refund or Credit",
                    "A transaction you did not approve or make",
                    "ATM did not Dispense Cash",
                    "Billed Twice for the Same Thing",
                    "Did not Receive Service or Item",
                    "Payment Made Another Way",
                    "Transaction Was Cancelled",
                ],
                "is_hold_account": 1,
            },
            "response": {
                "en": (
                    "Welcome to jake⚡️ the transaction is pending from the "
                    "merchant's side and is awaiting resolution or release if "
                    "not settled by the merchant within 32 days."
                ),
                "ar": (
                    "حياك الله في برق⚡️ , العملية معلقة من قبل التاجر في "
                    "إنتظار تسويتها أو فك قيدها في حال لم يتم تسويتها من قبل "
                    "التاجر خلال 32 يوم"
                ),
                "status": "CLOSE_RESOLVED"
            },
            "priority": 5
        },

        # ── RULE 6 ── Settled Mada -> Cortex clearing ─────────────────
        {
            "rule_id": 6,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": [
                    "Did not Receive Refund or Credit",
                    "A transaction you did not approve or make",
                    "ATM did not Dispense Cash",
                    "Billed Twice for the Same Thing",
                    "Did not Receive Service or Item",
                    "Payment Made Another Way",
                    "Transaction Was Cancelled",
                ],
                "is_settle_account": 1,
                "card_network": "MADA",
            },
            "response": {
                "en": (
                    "The transaction was successful. Since this is a Mada card, "
                    "the dispute is being raised to Cortex clearing for review."
                ),
                "ar": (
                    "تم تنفيذ العملية بنجاح. بما أن البطاقة مدى، سيتم رفع "
                    "النزاع إلى Cortex clearing للمراجعة."
                ),
                "status": "ESCALATE"
            },
            "priority": 6
        },

        # ── RULE 7 ── Settled Visa -> Apex (unauthorized / ATM) ───────
        {
            "rule_id": 7,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": [
                    "A transaction you did not approve or make",
                    "ATM did not Dispense Cash",
                ],
                "is_settle_account": 1,
                "card_network": "VISA",
            },
            "response": {
                "en": (
                    "The transaction was successful. This dispute is being "
                    "raised to Apex clearing for manual review."
                ),
                "ar": (
                    "تم تنفيذ العملية بنجاح. سيتم رفع هذا النزاع إلى "
                    "Apex clearing للمراجعة اليدوية."
                ),
                "status": "ESCALATE"
            },
            "priority": 7
        },

        # ── RULE 8 ── Settled Visa -> ask documents (booklet text) ─────
        # Docs-attached -> CMS is not queryable yet; default to ask-docs.
        {
            "rule_id": 8,
            "conditions": {
                "CategoryLevel2_c": "Transaction Dispute",
                "CategoryLevel3_c": [
                    "Did not Receive Refund or Credit",
                    "Billed Twice for the Same Thing",
                    "Did not Receive Service or Item",
                    "Payment Made Another Way",
                    "Transaction Was Cancelled",
                ],
                "is_settle_account": 1,
                "card_network": "VISA",
            },
            "response": {
                "en": (
                    "Welcome to jake⚡️\n"
                    "Thank you for reaching out to us. Please complete the "
                    "necessary requirements to proceed with your request, which "
                    "are (proof of non-utilization of the payment process, "
                    "either by providing the transaction receipt or sharing "
                    "correspondence through the merchant's official platforms "
                    "to prove eligibility for the amount and its refund)."
                ),
                "ar": (
                    "حياك الله في برق⚡️، \n"
                    "شكرًا على تواصلك معنا، يرجى إكمال المتطلبات اللازمة "
                    "لمتابعة طلبك وهي (إثبات عدم الاستفادة من عملية الدفع إما "
                    "بإيصال العملية أو مشاركة المراسلات عبر منصات التاجر "
                    "الرسمية لإثبات إستحقاق المبلغ واسترجاعه). "
                ),
                "status": "ESCALATE"
            },
            "priority": 8
        },

        # ── RULE 9 ── Incorrect balance matching ───────────────────────
        {
            "rule_id": 9,
            "conditions": {
                "CategoryLevel2_c": "Incorrect Balance",
                "is_balance_matching": 1
            },
            "response": {
                "en": "Welcome {first_name}, After reviewing the transactions, it was found that there is no issue with your balance. If there is any disputed transaction, please provide us with the transaction details so we can further investigate.",
                "ar": "حياك الله {first_name} في برق، بعد التحقق من العمليات تبين عدم وجود اشكالية في رصيدكم في حال وجود اعتراض على عملية نامل تزويدنا بالعملية المعترض عليها",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 9
        },

        # ── RULE 10 ── Card not working / pending verification ────────
        {
            "rule_id": 10,
            "conditions": {
                "CategoryLevel2_c": "Card Not Working",
                "provision_status": "PENDING_VERIFICATION",
            },
            "problem_exclude_labels": ["financial"],
            "response": {
                "en": "Welcome {first_name}, Please verify card activation via the jake app using NFC (hold the plastic card near an NFC tag to read its data) or via SMS (you will receive an OTP message to activate the card). You can then use the card via Apple Pay or Mada Pay.",
                "ar": "حياك الله {first_name}، الرجاء التحقق من تفعيل البطاقة من خلال تطبيق برق عبر NFC (يتطلب حمل البطاقة البلاستيكية بالقرب من علامة NFC لقراءة بياناتها) أو عبر الرسائل القصيرة (ستتلقى رسالة OTP لتفعيل البطاقة) وبإمكانك استعمال البطاقة عن طريق ابل باي او مدى باي",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 10
        },

        # ── RULE 11 ── Closed by customer ──────────────────────────────
        {
            "rule_id": 11,
            "conditions": {
                "CategoryLevel2_c": "Card Not Working",
                "provision_status": "SUSPENDED",
                "rejection_description": "%[006][CLOSED BY CUSTOMER]%",
                "provision_status_within_two_weeks": 1,
            },
            "problem_exclude_labels": ["financial"],
            "response": {
                "en": "Welcome {first_name}, After verification, it appears that the card was successfully closed by you. This ticket will be closed, and we welcome your communication with us for any further inquiries.",
                "ar": "حياك الله {first_name} في برق، بعد التحقق تبين انه تم اغلاق البطاقة من قبلكم بنجاح راح يتم إغلاق هذه التذكرة ونرحّب بتواصلك معنا لأي استفسار آخر.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 11
        },

        # ── RULE 12 ── Temporary block ─────────────────────────────────
        {
            "rule_id": 12,
            "conditions": {
                "CategoryLevel2_c": "Card Not Working",
                "provision_status": "SUSPENDED",
                "rejection_description": "%[010][TEMPORARY BLOCK]%",
                "provision_status_within_two_weeks": 1,
            },
            "problem_exclude_labels": ["financial"],
            "response": {
                "en": "Welcome {first_name}, After checking the card status, it was found that the card has been temporarily suspended, and you can reactivate it through the application.",
                "ar": "حياك الله {first_name} في برق، بعد التحقق من حالة البطاقة تبين بأن تم ايقاف البطاقة بشكل مؤقت و بأمكانك تنشيطها من خلال التطبيق .",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 12
        },

        # ── RULE 13 ── Closed by bank ──────────────────────────────────
        {
            "rule_id": 13,
            "conditions": {
                "CategoryLevel2_c": "Card Not Working",
                "provision_status": "SUSPENDED",
                "rejection_description": "%[007][CLOSED BY BANK]%",
                "provision_status_within_two_weeks": 1,
            },
            "problem_exclude_labels": ["financial"],
            "response": {
                "en": "Welcome {first_name}, we would like to inform you that the card has been blocked according to verify the fraudulent activity and protect your account.\nThis ticket will be closed, and we welcome your contact for any further inquiries.\nHave a great day!",
                "ar": "حياك الله {first_name} في برق، حبينا نبلغك بأنه تم إيقاف البطاقة حسب التذكرة المرفوعة للتحقق من عملية الاحتيال ولحماية حسابكم.\nراح يتم إغلاق هذه التذكرة ونرحّب بتواصلك معنا لأي استفسار آخر.\nيومك سعيد .",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 13
        },

        # ── RULE 14 ── PIN retry exceeded ──────────────────────────────
        {
            "rule_id": 14,
            "conditions": {
                "CategoryLevel2_c": "Card Not Working",
                "provision_status": "PIN_RETRY_EXCEEDED",
            },
            "problem_exclude_labels": ["financial"],
            "response": {
                "en": "Welcome {first_name}, You can change your card PIN through the app (Card Selection - Card Information - Enter Password - Change PIN). This ticket is now closed. Please feel free to contact us with any further inquiries.",
                "ar": "حياك الله {first_name} في برق، بإمكانك تغير الرقم السري للبطاقة من خلال التطبيق ( البطاقات اختيار البطاقة - معلومات البطاقة - ادخال كلمة المرور - تغيرالرقم السري ) راح يتم إغلاق هذه التذكرة ونرحّب بتواصلك معنا لأي استفسار آخر.",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 14
        },

        # ── RULE 15 ── Card delivery / not issued ──────────────────────
        # Any other Card Delivery ticket is left unmatched so it stays in
        # human review. Empty catch-all rules used to close those tickets.
        {
            "rule_id": 15,
            "conditions": {
                "CategoryLevel2_c": "Card Delivery",
                "has_physical_card": 0,
                "digital_only": 0,
            },
            "response": {
                "en": "Welcome {first_name}! You can issue a card by going to Cards - Issue Card and completing the issuance procedures. You can request a plastic card or opt for the electronic one only. Please note that the maximum number of cards issued is two primary Mada cards and three primary Visa cards only, including both active and inactive cards.",
                "ar": "حياك الله {first_name} في برق، بإمكانك إصدار بطاقة من خلال التوجه للبطاقات - إصدار بطاقة واكمال اجراءات الاصدار، وتستطيع طلب البطاقة البلاستيكية او الاكتفاء بالالكترونية فقط، مع العلم ان الحد الاقصى لإصدار البطاقات هو 2 بطاقات اساسية لمدى و 3 بطاقات اساسية لفيزا  فقط تشمل البطاقات النشطة والغير نشطة",
                "status": "CLOSE_RESOLVED"
            },
            "priority": 15
        },

    ]
    _assert_cards_en_ar_pairs(rules)
    return pd.DataFrame(rules)


# ─────────────────────────────────────────────────────────────────────────
# ACCOUNTS (Account & Registration) — routing per the booklet's
# "CIF Status & Application Check Assignment Matrix" (sections 3, 4, 6, 7,
# 10). Every rule ESCALATEs to a department (route in
# ACCOUNTS_ROUTE_BY_RULE); none closes the ticket. The customer reply asks
# for what the team needs next (confirmation / documents) or acknowledges.
#
# DRAFT replies -- wording to be approved by Account Operation.
#
# Row fields (services/products/accounts.py):
#   cif_status        users.cif_status, upper-cased; blank -> "NO_CIF"
#   app_check_status  latest record of EACH check type, summarised:
#                     FAILED > PENDING > INCOMPLETE > PASSED (else OTHER)
#   account_status    accounts.account_status of the MAIN account
#   has_balance       1 when the wallet balance is > 0
# Anything outside the matrix matches no rule -> escalated as NO_MATCH.
# ─────────────────────────────────────────────────────────────────────────
ACCOUNTS_VERIFICATION_STATUSES = [
    "NO_CIF", "UNVERIFIED", "NAFATH_REQUIRED", "KYC_UPDATE", "RE_VERIFY",
]
ACCOUNTS_TRANSFER_STATUSES = ["DECEASED", "FINAL_EXIT", "END_RELATIONSHIP"]

# rule_id -> department route (used by dag_pipeline/write_service_work.py)
ACCOUNTS_ROUTE_BY_RULE = {
    1: "accounts/COMPLIANCE",
    2: "accounts/ACCOUNT_OPERATION",
    3: "accounts/ACCOUNT_OPERATION_CLOSURE",
    4: "accounts/ACCOUNT_OPERATION",
    5: "accounts/ACCOUNT_OPERATION_BALANCE_TRANSFER",
    6: "accounts/ACCOUNT_OPERATION_BALANCE_TRANSFER",
    7: "accounts/ACCOUNT_OPERATION_BALANCE_TRANSFER",
    8: "accounts/ACCOUNT_OPERATION",
    9: "accounts/COMPLIANCE",
    10: "accounts/TECHNOLOGY",
    11: "accounts/COMPLIANCE",
    12: "accounts/TECHNOLOGY",
    13: "accounts/TECHNOLOGY",
    14: "accounts/TECHNOLOGY",
}

_ACC_REVIEW = {
    "en": "Welcome {first_name}, we have received your request and it is being reviewed by the specialized team. We will update you as soon as possible.",
    "ar": "حياك الله {first_name} في برق، استلمنا طلبك وجاري مراجعته من قبل الفريق المختص، وراح نوافيك بالتحديث في أقرب وقت.",
    "status": "ESCALATE",
}
_ACC_PERSONAL_DATA = {
    "en": "Welcome {first_name}, we have received your request to correct your personal data and it has been forwarded to the specialized team for review. We will update you as soon as possible.",
    "ar": "حياك الله {first_name} في برق، استلمنا طلب تصحيح بياناتك الشخصية وتم تحويله للفريق المختص للمراجعة، وراح نوافيك بالتحديث في أقرب وقت.",
    "status": "ESCALATE",
}
# Booklet section 7: customer confirmation required before closing.
_ACC_CLOSURE_CONFIRM = {
    "en": "Welcome {first_name}, we have received your request to close your wallet. Please reply to this message to confirm that you would like to proceed with closing your wallet.",
    "ar": "حياك الله {first_name} في برق، استلمنا طلب إغلاق محفظتك. نأمل تأكيد رغبتك في إغلاق المحفظة بالرد على هذه الرسالة.",
    "status": "ESCALATE",
}
# Booklet 10.3 (FINAL_EXIT).
_ACC_DOCS_FINAL_EXIT = {
    "en": "Welcome {first_name}, to transfer your remaining balance, please provide: a copy of your Iqama, a copy of your passport, a bank certificate confirming a local IBAN, and a signed declaration that includes your full name, Iqama number, the beneficiary IBAN and bank, the beneficiary name, your relationship with the beneficiary, the total transfer amount, your approval of the transfer, and your signature and date.",
    "ar": "حياك الله {first_name} في برق، لتحويل رصيدك المتبقي نأمل تزويدنا بالتالي: نسخة من الإقامة، نسخة من جواز السفر، شهادة بنكية توضح رقم الآيبان المحلي، وإقرار موقّع يتضمن: الاسم الكامل، رقم الإقامة، رقم الآيبان واسم البنك للمستفيد، اسم المستفيد، صلة القرابة بالمستفيد، إجمالي مبلغ التحويل، موافقتك على التحويل، والتوقيع والتاريخ.",
    "status": "ESCALATE",
}
# Booklet 10.5 (DECEASED). Written to the representative -- deliberately no
# {first_name} (that would be the deceased customer's name).
_ACC_DOCS_DECEASED = {
    "en": "Hello, please accept our condolences. To transfer the remaining balance, please provide: the representative's ID, a valid Power of Attorney that authorizes one person to act on behalf of the deceased and their heirs and explicitly authorizes contacting jake to withdraw or transfer the balance, and a bank certificate confirming a local IBAN belonging to the representative.",
    "ar": "أهلاً بك، عظّم الله أجركم وأحسن عزاءكم. لتحويل الرصيد المتبقي نأمل تزويدنا بالتالي: هوية الممثل، وكالة شرعية سارية تخوّل شخصاً واحداً بالنيابة عن المتوفى وورثته وتتضمن نصاً صريحاً بالتواصل مع برق وسحب أو تحويل الرصيد، وشهادة بنكية توضح رقم الآيبان المحلي باسم الممثل.",
    "status": "ESCALATE",
}
# Booklet 10.6 (END_RELATIONSHIP).
_ACC_DOCS_END_RELATIONSHIP = {
    "en": "Welcome {first_name}, to transfer your remaining balance, please provide a bank certificate confirming a local IBAN and a copy of your National ID / Iqama.",
    "ar": "حياك الله {first_name} في برق، لتحويل رصيدك المتبقي نأمل تزويدنا بشهادة بنكية توضح رقم الآيبان المحلي ونسخة من الهوية الوطنية / الإقامة.",
    "status": "ESCALATE",
}
# INCOMPLETE checks the customer can complete themselves (see
# services/products/accounts.py CUSTOMER_COMPLETABLE_CHECKS). Replies never
# name the check -- AML / EDD / sanctions etc. must not be disclosed.
# No CIF yet: they never finished onboarding.
_ACC_COMPLETE_REGISTRATION = {
    "en": "Welcome {first_name}, your registration is not complete yet. Please open the jake app and complete the remaining steps so we can finish opening your account.",
    "ar": "حياك الله {first_name} في برق، لم يكتمل تسجيلك بعد. نأمل الدخول إلى تطبيق برق واستكمال الخطوات المتبقية حتى نتمكن من إكمال فتح حسابك.",
    "status": "ESCALATE",
}
# Has a CIF: they completed onboarding before and are being asked to update.
_ACC_UPDATE_INFO = {
    "en": "Welcome {first_name}, we need you to update some of your information. Please open the jake app and update the requested details.",
    "ar": "حياك الله {first_name} في برق، نحتاج منك تحديث بعض بياناتك. نأمل الدخول إلى تطبيق برق وتحديث البيانات المطلوبة.",
    "status": "ESCALATE",
}
# Booklet 10.2 / FAQ Q13 (ID_EXPIRED).
_ACC_ID_EXPIRED = {
    "en": "Welcome {first_name}, your ID appears to be expired. Please update it by providing your renewed Iqama. If you have left the Kingdom on a Final Exit visa, please provide a copy of the Final Exit visa and a local IBAN in your name so we can transfer your balance.",
    "ar": "حياك الله {first_name} في برق، يظهر لدينا أن هويتك منتهية. نأمل تحديثها بتزويدنا بالإقامة المجددة، وفي حال مغادرتك المملكة بتأشيرة خروج نهائي نأمل تزويدنا بنسخة من تأشيرة الخروج النهائي ورقم آيبان محلي باسمك لتحويل رصيدك.",
    "status": "ESCALATE",
}


def _load_accounts_rules():
    rules = [
        {
            # Section 6: personal data correction -> Compliance.
            "rule_id": 1,
            "conditions": {"CategoryLevel2_c": [
                "Request to correct personal data",
                "Request to Correct Personal Data",
            ]},
            "response": _ACC_PERSONAL_DATA,
            "priority": 1,
        },
        {
            # Section 4.1: Active CIF with a Closed account -> Account Operation.
            "rule_id": 2,
            "conditions": {"cif_status": "ACTIVE", "account_status": "CLOSED"},
            "response": _ACC_REVIEW,
            "priority": 2,
        },
        {
            # Section 7: closure request (active, checks passed) -> ask the
            # customer to confirm before Account Operation acts.
            "rule_id": 3,
            "conditions": {
                "cif_status": "ACTIVE",
                "app_check_status": "PASSED",
                "CategoryLevel2_c": ["Account closure request", "Account Closure Request"],
            },
            "response": _ACC_CLOSURE_CONFIRM,
            "priority": 3,
        },
        {
            # Matrix: ACTIVE + all checks passed -> Account Operation.
            "rule_id": 4,
            "conditions": {"cif_status": "ACTIVE", "app_check_status": "PASSED"},
            "response": _ACC_REVIEW,
            "priority": 4,
        },
        {
            # Matrix + 10.3: FINAL_EXIT with a balance -> transfer documents.
            "rule_id": 5,
            "conditions": {"cif_status": "FINAL_EXIT", "app_check_status": "PASSED", "has_balance": 1},
            "response": _ACC_DOCS_FINAL_EXIT,
            "priority": 5,
        },
        {
            # Matrix + 10.5: DECEASED with a balance -> representative documents.
            "rule_id": 6,
            "conditions": {"cif_status": "DECEASED", "app_check_status": "PASSED", "has_balance": 1},
            "response": _ACC_DOCS_DECEASED,
            "priority": 6,
        },
        {
            # Matrix + 10.6: END_RELATIONSHIP with a balance -> documents.
            "rule_id": 7,
            "conditions": {"cif_status": "END_RELATIONSHIP", "app_check_status": "PASSED", "has_balance": 1},
            "response": _ACC_DOCS_END_RELATIONSHIP,
            "priority": 7,
        },
        {
            # Matrix: DECEASED / FINAL_EXIT / END_RELATIONSHIP, no balance
            # left -> Account Operation reviews (nothing to transfer).
            "rule_id": 8,
            "conditions": {"cif_status": ACCOUNTS_TRANSFER_STATUSES, "app_check_status": "PASSED"},
            "response": _ACC_REVIEW,
            "priority": 8,
        },
        {
            # Matrix: BLOCKED + passed -> Compliance.
            "rule_id": 9,
            "conditions": {"cif_status": "BLOCKED", "app_check_status": "PASSED"},
            "response": _ACC_REVIEW,
            "priority": 9,
        },
        {
            # Matrix: ID_EXPIRED + passed -> Technology.
            "rule_id": 10,
            "conditions": {"cif_status": "ID_EXPIRED", "app_check_status": "PASSED"},
            "response": _ACC_ID_EXPIRED,
            "priority": 10,
        },
        {
            # Matrix: verification statuses + PENDING / FAILED -> Compliance.
            "rule_id": 11,
            "conditions": {
                "cif_status": ACCOUNTS_VERIFICATION_STATUSES,
                "app_check_status": ["PENDING", "FAILED"],
            },
            "response": _ACC_REVIEW,
            "priority": 11,
        },
        {
            # INCOMPLETE, no CIF, only customer-completable checks ->
            # ask them to finish registration. Technology per the matrix.
            "rule_id": 12,
            "conditions": {
                "cif_status": "NO_CIF",
                "app_check_status": "INCOMPLETE",
                "incomplete_action": "CUSTOMER",
            },
            "response": _ACC_COMPLETE_REGISTRATION,
            "priority": 12,
        },
        {
            # INCOMPLETE, customer already has a CIF (completed onboarding
            # before; e.g. re-collection requested), only customer-completable
            # checks -> ask them to update their information.
            "rule_id": 13,
            "conditions": {
                "cif_status": ["ACTIVE", "UNVERIFIED", "NAFATH_REQUIRED", "KYC_UPDATE", "RE_VERIFY"],
                "app_check_status": "INCOMPLETE",
                "incomplete_action": "CUSTOMER",
            },
            "response": _ACC_UPDATE_INFO,
            "priority": 13,
        },
        {
            # INCOMPLETE on an internal / sensitive check (AML, EDD,
            # sanctions, ...): Technology per the matrix, neutral reply --
            # nothing about the check is disclosed. (ACTIVE customers here
            # fall outside the matrix -> NO_MATCH review.)
            "rule_id": 14,
            "conditions": {
                "cif_status": ACCOUNTS_VERIFICATION_STATUSES,
                "app_check_status": "INCOMPLETE",
                "incomplete_action": "INTERNAL",
            },
            "response": _ACC_REVIEW,
            "priority": 14,
        },
    ]
    return pd.DataFrame(rules)

def _assert_cards_en_ar_pairs(rules: list) -> None:
    """Fail fast if a cards rule pairs refund EN with hold AR (or reverse)."""
    for rule in rules:
        resp = rule.get("response") or {}
        en = str(resp.get("en") or "")
        ar = str(resp.get("ar") or "")
        if not en and not ar:
            continue
        en_l = en.lower()
        refund_en = "refunded to your wallet" in en_l
        hold_en = "within 32 days" in en_l or "pending from the merchant" in en_l
        hold_ar = "معلقة من قبل التاجر" in ar
        refund_ar = "لمحفظتك" in ar or "إلى محفظتك" in ar
        if (refund_en and hold_ar) or (hold_en and refund_ar):
            raise ValueError(
                f"cards rule_id={rule.get('rule_id')} has mismatched EN/AR "
                "(refund paired with hold). Fix the response templates."
            )


def match_rule(row, rules_df):
    applicable_rules = rules_df.sort_values("priority")

    _dbg("\n===== MATCH DEBUG START =====")
    _dbg("Incoming row:", row)

    problem_label_cache = None

    for _, rule in applicable_rules.iterrows():
        conditions = rule.get("conditions", {})
        rule_failed = False

        _dbg(f"\n--- Checking rule_id={rule['rule_id']} priority={rule['priority']} ---")
        _dbg("Conditions:", conditions)

        for field, expected_value in conditions.items():
            actual_value = row.get(field)

            _dbg(
                f"[FIELD CHECK] field='{field}' | actual='{actual_value}' | expected='{expected_value}'"
            )

            if isinstance(expected_value, list):
                if actual_value not in expected_value:
                    _dbg(
                        f"[FAIL] rule_id={rule['rule_id']} -> field '{field}' value '{actual_value}' not in {expected_value}"
                    )
                    rule_failed = True
                    break

            elif expected_value == "LAST_24H":
                result = check_last_24h(actual_value)
                _dbg(
                    f"[TIME CHECK] field='{field}' | actual='{actual_value}' | last_24h={result}"
                )
                if not result:
                    _dbg(
                        f"[FAIL] rule_id={rule['rule_id']} -> field '{field}' is not within LAST_24H"
                    )
                    rule_failed = True
                    break

            elif isinstance(expected_value, str) and (
                expected_value.startswith("%") or expected_value.endswith("%")
            ):
                # SQL LIKE: % = any text. Escape each literal piece and join
                # with ".*" (re.escape no longer escapes "%" on Python 3.7+,
                # so the old escape-then-replace never produced a wildcard).
                pattern = (
                    "^"
                    + ".*".join(re.escape(part) for part in expected_value.split("%"))
                    + "$"
                )
                text = "" if actual_value is None else str(actual_value)
                if not re.search(pattern, text, flags=re.IGNORECASE):
                    _dbg(
                        f"[FAIL] rule_id={rule['rule_id']} -> field '{field}' "
                        f"value '{actual_value}' does not match LIKE '{expected_value}'"
                    )
                    rule_failed = True
                    break

            else:
                if actual_value != expected_value:
                    _dbg(
                        f"[FAIL] rule_id={rule['rule_id']} -> field '{field}' value '{actual_value}' != '{expected_value}'"
                    )
                    rule_failed = True
                    break

        if rule_failed:
            _dbg(f"[RULE RESULT] rule_id={rule['rule_id']} -> FAILED on conditions")
            continue

        needs_llm = False

        expected_problem_label = rule.get("problem_label")
        if isinstance(expected_problem_label, str) and expected_problem_label.strip():
            needs_llm = True
        elif isinstance(expected_problem_label, list) and expected_problem_label:
            needs_llm = True

        excluded_labels = rule.get("problem_exclude_labels", [])
        if not isinstance(excluded_labels, list):
            excluded_labels = []

        if excluded_labels:
            needs_llm = True

        if needs_llm and problem_label_cache is None:
            description_for_llm = row.get("sanitized_description", "")
            problem_label_cache = classify_problem_description(description_for_llm)

            _dbg(
                f"[LLM RESULT] sanitized_description='{description_for_llm}' | problem_label='{problem_label_cache}'"
            )

        if isinstance(expected_problem_label, str) and expected_problem_label.strip():
            _dbg(
                f"[LLM CHECK] actual_label='{problem_label_cache}' | expected_label='{expected_problem_label}'"
            )

            if problem_label_cache != expected_problem_label:
                _dbg(
                    f"[FAIL] rule_id={rule['rule_id']} -> problem_label '{problem_label_cache}' != '{expected_problem_label}'"
                )
                continue

        if isinstance(expected_problem_label, list) and expected_problem_label:
            _dbg(
                f"[LLM CHECK] actual_label='{problem_label_cache}' | expected_labels='{expected_problem_label}'"
            )
            if problem_label_cache not in expected_problem_label:
                _dbg(
                    f"[FAIL] rule_id={rule['rule_id']} -> problem_label '{problem_label_cache}' not in {expected_problem_label}"
                )
                continue

        if excluded_labels:
            _dbg(
                f"[LLM EXCLUDE CHECK] actual_label='{problem_label_cache}' | excluded_labels='{excluded_labels}'"
            )

            if problem_label_cache in excluded_labels:
                _dbg(
                    f"[FAIL] rule_id={rule['rule_id']} -> problem_label '{problem_label_cache}' is excluded"
                )
                continue

        _dbg(f"[MATCHED] rule_id={rule['rule_id']}")
        _dbg("===== MATCH DEBUG END =====\n")

        resp = rule["response"]
        if not isinstance(resp, dict):
            _dbg(
                f"[SKIP] rule_id={rule['rule_id']} -> response is not a dict: {type(resp)}"
            )
            continue
        response_en = "" if resp.get("en") is None else str(resp.get("en"))
        response_ar = "" if resp.get("ar") is None else str(resp.get("ar"))
        if not response_en.strip() and not response_ar.strip():
            _dbg(
                f"[SKIP] rule_id={rule['rule_id']} -> empty response_en/response_ar"
            )
            continue

        return {
            "decision": "STATIC_RESPONSE",
            "rule_id": int(rule["rule_id"]) if rule["rule_id"] is not None else None,
            "response_en": response_en,
            "response_ar": response_ar,
            "ticket_action": resp.get("status") or "ESCALATE",
        }

    _dbg("[NO MATCH] No rule matched this row")
    _dbg("===== MATCH DEBUG END =====\n")

    return {
        "decision": "NO_MATCH",
        "rule_id": None,
        "response_en": "",
        "response_ar": "",
        "ticket_action": "ESCALATE"
    }
