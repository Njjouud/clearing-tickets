-- to be added: edit the query to have this edit: 
-- "REFUND - CHW20260421-90H7R3JP	- VISA - UNAUTHORIZED CREDIT in account name 
-- --> "if the merchant name is same and the txn id of the other txn id starts with CHW - Reufund and has unautgorized credit
WITH ticket_data AS (
    SELECT
          t.BaraqInternalID_c
        , t.TransactionID_c
        , u.cif
        , u.first_name
        , u.first_name_en
    FROM polaris.`base.oci_crm_db`.all_service_requests t
    LEFT JOIN polaris.`base.customer_identity_db`.users u
        ON TRIM(CAST(u.user_id AS VARCHAR))
         = TRIM(CAST(t.BaraqInternalID_c AS VARCHAR))
    WHERE UPPER(TRIM(CAST(t.TransactionID_c AS VARCHAR)))
          = UPPER(TRIM('@TransactionID_c'))
),

account_ledger_filtered AS (
    SELECT al.*
    FROM polaris.`base.core_accounting_db`.account_ledger al
    WHERE UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
          = UPPER(TRIM('@TransactionID_c'))
),

latest_provider_account AS (
    SELECT *
    FROM (
        SELECT
              al.txn_category_txn_id
            , pa.account_number
            , pa.account_name
            , al.amount
            , ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
                ORDER BY al.updated_at DESC,
                         al.auto_id DESC
              ) rn
        FROM account_ledger_filtered al
        JOIN polaris.`base.core_accounting_db`.provider_accounts pa
          ON pa.account_number = al.account_number
    ) x
    WHERE rn = 1
),

wallet_txn AS (
    SELECT
          txn_amount
        , pre_txn_balance
        , txn_type
    FROM polaris.`base.core_wallet_db`.customer_wallet_ledger
    WHERE UPPER(TRIM(CAST(txn_category_txn_id AS VARCHAR)))
          = UPPER(TRIM('@TransactionID_c'))
)

SELECT
      at.transaction_id
    , td.cif
    , td.first_name
    , td.first_name_en
    , lpa.account_number AS provider_account_number
    , lpa.account_name AS provider_account_name
    -- , wt.txn_type
    -- , wt.txn_amount

    , CASE
        WHEN LOWER(lpa.account_name) LIKE '%hold%'
        THEN 1
        ELSE 0
      END AS is_hold_account

    , CASE
        WHEN LOWER(lpa.account_name) LIKE '%clearing%'
        THEN 1
        ELSE 0
      END AS is_settle_account

    , CASE
        WHEN LOWER(lpa.account_name) LIKE '%hold%'
          OR LOWER(lpa.account_name) LIKE '%clearing%'
        THEN 1
        ELSE 0
      END AS is_dispute_provider_account

    , CASE
        WHEN LOWER(lpa.account_name) LIKE '%mada%'
        THEN 'MADA'
        WHEN LOWER(lpa.account_name) LIKE '%visa%'
        THEN 'VISA'
        ELSE NULL
      END AS card_network
    , COALESCE(wt.wallet_refund_posted, 0) AS wallet_refund_posted
    -- , 0 AS wallet_refund_posted
    , 0 AS is_billed_twice
    , 0 AS charged_after_stop
    , 0 AS has_stop_record

FROM ticket_data td

JOIN polaris.`base.card_hub_payment_engine_db`.authorization_transaction at
    ON UPPER(TRIM(CAST(at.transaction_id AS VARCHAR)))
     = UPPER(TRIM(CAST(td.TransactionID_c AS VARCHAR)))
    AND TRIM(CAST(at.cif AS VARCHAR))
     = TRIM(CAST(td.cif AS VARCHAR))

LEFT JOIN latest_provider_account lpa
    ON UPPER(TRIM(CAST(lpa.txn_category_txn_id AS VARCHAR)))
     = UPPER(TRIM(CAST(at.transaction_id AS VARCHAR)))

LEFT JOIN (
  SELECT MAX(
    CASE
      WHEN UPPER(TRIM(CAST(txn_type AS VARCHAR))) IN ('REFUND', 'REVERSAL')
        AND txn_amount IS NOT NULL
        AND txn_amount <> 0
      THEN 1 ELSE 0
    END
  ) AS wallet_refund_posted
  FROM wallet_txn
  ) wt
  ON 1 = 1;