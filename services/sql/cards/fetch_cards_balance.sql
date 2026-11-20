WITH ticket_data AS (
    SELECT
          u.first_name
        , u.first_name_en
    FROM polaris.`base.oci_crm_db`.all_service_requests t
    LEFT JOIN (
        -- Pre-filter users to the ticket owner(s) for this transaction id. Same
        -- join result as joining the whole table, without hashing all users.
        SELECT uu.*
        FROM polaris.`base.customer_identity_db`.users uu
        WHERE TRIM(CAST(uu.user_id AS VARCHAR)) IN (
            SELECT TRIM(CAST(tk.BaraqInternalID_c AS VARCHAR))
            FROM polaris.`base.oci_crm_db`.all_service_requests tk
            WHERE UPPER(TRIM(CAST(tk.TransactionID_c AS VARCHAR))) = UPPER(TRIM('@TransactionID_c'))
        )
    ) u
        ON TRIM(CAST(u.user_id AS VARCHAR))
         = TRIM(CAST(t.BaraqInternalID_c AS VARCHAR))
    WHERE UPPER(TRIM(CAST(t.TransactionID_c AS VARCHAR)))
          = UPPER(TRIM('@TransactionID_c'))
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
      td.first_name
    , td.first_name_en
    , wt.txn_amount
    , wt.pre_txn_balance
    , wt.txn_type
    , CASE
        WHEN wt.txn_amount = wt.pre_txn_balance
        THEN 1
        ELSE 0
      END AS is_balance_matching
FROM wallet_txn wt
CROSS JOIN ticket_data td;