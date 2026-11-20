-- Used by ALL card tickets.
-- Retrieves:
-- - ticket ownership validation
-- - authorization transaction
-- - wallet transaction
-- - basic customer data


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

latest_provider_account AS (
    SELECT *
    FROM (
        SELECT
              al.txn_category_txn_id
            , pa.account_number
            , pa.account_name
            , ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
                ORDER BY al.updated_at DESC,
                         al.auto_id DESC
              ) rn
        FROM polaris.`base.core_accounting_db`.account_ledger al
        JOIN polaris.`base.core_accounting_db`.provider_accounts pa
          ON pa.account_number = al.account_number
        WHERE UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
              = UPPER(TRIM('@TransactionID_c'))
    ) x
    WHERE rn = 1
),

wallet_txn AS (
    SELECT
          UPPER(TRIM(CAST(txn_category_txn_id AS VARCHAR))) txn_id
        , txn_amount
        , pre_txn_balance
        , txn_type
    FROM polaris.`base.core_wallet_db`.customer_wallet_ledger
    WHERE UPPER(TRIM(CAST(txn_category_txn_id AS VARCHAR)))
          = UPPER(TRIM('@TransactionID_c'))
)

SELECT
      at.transaction_id
    , at.transaction_status
    , at.payment_transaction_status
    , td.first_name
    , td.first_name_en
    , td.cif
    , wt.txn_amount
    , wt.pre_txn_balance
    , wt.txn_type
    , lpa.account_number AS provider_account_number
    , lpa.account_name AS provider_account_name
FROM ticket_data td
JOIN polaris.`base.card_hub_payment_engine_db`.authorization_transaction at
  ON UPPER(TRIM(CAST(at.transaction_id AS VARCHAR)))
   = UPPER(TRIM(CAST(td.TransactionID_c AS VARCHAR)))
LEFT JOIN wallet_txn wt
  ON wt.txn_id =
     UPPER(TRIM(CAST(at.transaction_id AS VARCHAR)))
LEFT JOIN latest_provider_account lpa
  ON UPPER(TRIM(CAST(lpa.txn_category_txn_id AS VARCHAR)))
   = UPPER(TRIM(CAST(at.transaction_id AS VARCHAR)));