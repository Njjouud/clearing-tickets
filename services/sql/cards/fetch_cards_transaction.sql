-- this query is old and only for reference 
-- Same logic as Postman cards query, StarRocks-safe for mysql-connector:
-- - no QUALIFY (use ROW_NUMBER + WHERE rn = 1)
-- - CIF from users join (crm has no CIF_c)
-- - UPPER(TRIM(...)) on txn ids so CRM/auth/ledger casing/spaces do not drop the row
--
-- SECURITY: forced ownership check (unchanged from previous version).
-- authorization_transaction carries its own `cif` column; we require it to
-- equal the ticket's own CIF. INNER JOIN so a mismatched/unresolved owner
-- drops the row entirely (fail closed).
--
-- NEW: card_network resolution for settle/dispute routing (cards rules
-- MADA vs Visa). Primary: card_subscription_order.alias -> card_product.type
--   ('CLASSIC' = Mada, anything else = Visa).
-- Fallback: provider account_name contains 'mada' / 'visa' when the product
-- join is missing (common for settle disputes). NULL only if both fail.
-- NOTE: GCC is NOT distinguishable with this data model yet.
WITH ticket_data AS (
    SELECT
          t.BaraqInternalID_c
        , t.SrNumber
        , t.QueueName
        , t.TransactionID_c
        , t.CreationDate
        , t.ProblemDescription
        , t.StatusTypeCd
        , t.CategoryLevel2_c
        , u.cif AS CIF_c
        , u.first_name
        , u.first_name_en
    FROM polaris.`base.oci_crm_db`.all_service_requests t
    LEFT JOIN polaris.`base.customer_identity_db`.users u
        ON TRIM(CAST(u.user_id AS VARCHAR)) = TRIM(CAST(t.BaraqInternalID_c AS VARCHAR))
    WHERE UPPER(TRIM(CAST(t.TransactionID_c AS VARCHAR)))
        = UPPER(TRIM('@TransactionID_c'))
),
ticket_status AS (
    SELECT
          BaraqInternalID_c   AS baraq_internal_id
        , SrNumber            AS serial_number
        , QueueName           AS queue_name
        , TransactionID_c     AS transaction_id
        , CreationDate
        , ProblemDescription  AS problem_description
        , CategoryLevel2_c
        , CIF_c               AS cif
        , first_name
        , first_name_en
    FROM ticket_data
    WHERE StatusTypeCd != 'ORA_SVC_RESOLVED' OR StatusTypeCd IS NULL
),
-- Card Dispute Flow booklet account allow-lists.
-- Hold flow: only these Hold accounts. Settle flow: only these Clearing accounts.
hold_provider_accounts AS (
    SELECT account_number FROM (
        SELECT '120316' AS account_number  -- Visa Local POS Issuer Hold Account
        UNION ALL SELECT '120318'          -- Visa International ATM Issuer Hold Account
        UNION ALL SELECT '120317'          -- Visa International POS Issuer Hold Account
    ) h
),
settle_provider_accounts AS (
    SELECT account_number FROM (
        SELECT '120212' AS account_number  -- Visa Local ATM Issuer Clearing Account
        UNION ALL SELECT '120213'          -- Visa Local POS Issuer Clearing Account
        UNION ALL SELECT '120210'          -- Visa International ATM Issuer Clearing Account
        UNION ALL SELECT '120211'          -- Visa International POS Issuer Clearing Account
        UNION ALL SELECT '120217'          -- MADA Local POS Issuer Clearing Account
        UNION ALL SELECT '120218'          -- MADA Local ATM Issuer Clearing Account
        UNION ALL SELECT '120215'          -- MADA GCC POS Issuer Clearing Account
        UNION ALL SELECT '120216'          -- MADA GCC ATM Issuer Clearing Account
        UNION ALL SELECT '120219'          -- MADA Local Ecomm Issuer Clearing Account
    ) s
),
ticket_txns AS (
    SELECT DISTINCT
        UPPER(TRIM(CAST(transaction_id AS VARCHAR))) AS transaction_id
    FROM ticket_status
),
account_ledger_filtered AS (
    SELECT
        al.*
    FROM polaris.`base.core_accounting_db`.account_ledger al
    JOIN ticket_txns tt
      ON UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
       = tt.transaction_id
),
wallet_txn AS (
    SELECT
        UPPER(TRIM(CAST(txn_category_txn_id AS VARCHAR))) AS txn_id,
        txn_amount,
        pre_txn_balance,
        txn_type
    FROM polaris.`base.core_wallet_db`.customer_wallet_ledger
    WHERE UPPER(TRIM(CAST(txn_category_txn_id AS VARCHAR))) IN (
        SELECT transaction_id
        FROM ticket_txns
    )
),
card_provision AS (
    SELECT
          cif
        , card_alias
        , provision_status
        , provision_status_within_two_weeks
    FROM (
        SELECT
              cp.cif
            , cp.card_alias
            , cp.status AS provision_status
            , CASE
                WHEN cp.modified_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                THEN 1 ELSE 0
              END AS provision_status_within_two_weeks
            , ROW_NUMBER() OVER (
                PARTITION BY cp.cif
                ORDER BY cp.modified_date DESC
              ) AS rn
        FROM polaris.`base.card_hub_app_db`.card_provision cp
        WHERE cp.cif IN (SELECT cif FROM ticket_status WHERE cif IS NOT NULL)
    ) ranked_prov
    WHERE rn = 1
),
card_rejection AS (
    SELECT
          card_alias
        , rejection_description
    FROM (
        SELECT
              rat.card_alias
            , rat.rejection_reason AS rejection_description
            , ROW_NUMBER() OVER (
                PARTITION BY rat.card_alias
                ORDER BY rat.created_date DESC
              ) AS rn
        FROM polaris.`base.card_hub_payment_engine_db`.rejected_authorization_transaction rat
        WHERE rat.card_alias IN (SELECT card_alias FROM card_provision)
    ) ranked_rej
    WHERE rn = 1
),
card_network AS (
    -- Resolves the card's scheme (Mada vs. everything else) for the
    -- specific card involved in this dispute, anchored to card_provision's
    -- already-resolved card_alias (itself anchored to the ticket's own cif).
    SELECT
          cso.card_alias
        , cp2.type AS card_scheme_type
    FROM (
        SELECT
              alias AS card_alias
            , card_product_id
            , ROW_NUMBER() OVER (
                PARTITION BY alias
                ORDER BY modified_date DESC
              ) AS rn
        FROM polaris.`base.card_hub_app_db`.card_subscription_order
        WHERE alias IN (SELECT card_alias FROM card_provision WHERE card_alias IS NOT NULL)
    ) cso
    JOIN polaris.`base.card_hub_app_db`.card_product cp2
        ON cp2.id = cso.card_product_id
    WHERE cso.rn = 1
),
subscription_stop AS (
    -- Most recent CANCELED/STOPPED subscription-payment-instruction for
    -- this cif + merchant (card_acceptor_id), so we can tell "customer
    -- says they cancelled this subscription" apart from a real dispute.
    -- ASSUMPTION (flag if wrong): modified_date is the stop-effective
    -- timestamp -- i.e. when `status` last changed to CANCELED/STOPPED.
    SELECT
          cif
        , card_acceptor_id
        , merchant_name
        , status
        , modified_date AS stopped_at
    FROM (
        SELECT
              spi.cif
            , spi.card_acceptor_id
            , spi.merchant_name
            , spi.status
            , spi.modified_date
            , ROW_NUMBER() OVER (
                PARTITION BY spi.cif, spi.card_acceptor_id
                ORDER BY spi.modified_date DESC
              ) AS rn
        FROM polaris.`base.card_hub_app_db`.subscription_payment_instruction spi
        WHERE spi.status IN ('CANCELED', 'STOPPED')
          AND spi.cif IN (SELECT cif FROM ticket_status WHERE cif IS NOT NULL)
    ) ranked_stop
    WHERE rn = 1
),
-- Confirmed duplicate charge: another SUCCESS authorization for the same
-- cif + card_acceptor_id + wallet txn_amount within 5 minutes of this txn.
-- ASSUMPTION: amount comes from customer_wallet_ledger.txn_amount linked by
-- txn_category_txn_id (same source as balance-matching above). If amount is
-- missing on either side, is_billed_twice stays 0 (fail closed).
billed_twice AS (
    SELECT
          base.transaction_id
        , CASE WHEN MAX(other.transaction_id) IS NOT NULL THEN 1 ELSE 0 END AS is_billed_twice
    FROM (
        SELECT
              at2.transaction_id
            , at2.cif
            , at2.card_acceptor_id
            , at2.created_date
            , cwl.txn_amount
        FROM polaris.`base.card_hub_payment_engine_db`.authorization_transaction at2
        LEFT JOIN polaris.`base.core_wallet_db`.customer_wallet_ledger cwl
            ON UPPER(TRIM(CAST(cwl.txn_category_txn_id AS VARCHAR)))
             = UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
        WHERE UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR))) IN (
            SELECT UPPER(TRIM(CAST(transaction_id AS VARCHAR)))
            FROM ticket_status
        )
          AND at2.cif IN (SELECT cif FROM ticket_status WHERE cif IS NOT NULL)
    ) base
    LEFT JOIN polaris.`base.card_hub_payment_engine_db`.authorization_transaction other
        ON TRIM(CAST(other.cif AS VARCHAR)) = TRIM(CAST(base.cif AS VARCHAR))
       AND other.card_acceptor_id = base.card_acceptor_id
       AND UPPER(TRIM(CAST(other.transaction_id AS VARCHAR)))
        <> UPPER(TRIM(CAST(base.transaction_id AS VARCHAR)))
       AND UPPER(TRIM(CAST(other.payment_transaction_status AS VARCHAR))) = 'SUCCESS'
       AND ABS(
            UNIX_TIMESTAMP(other.created_date) - UNIX_TIMESTAMP(base.created_date)
       ) <= 300
    LEFT JOIN polaris.`base.core_wallet_db`.customer_wallet_ledger other_cwl
        ON UPPER(TRIM(CAST(other_cwl.txn_category_txn_id AS VARCHAR)))
         = UPPER(TRIM(CAST(other.transaction_id AS VARCHAR)))
       AND base.txn_amount IS NOT NULL
       AND other_cwl.txn_amount = base.txn_amount
    WHERE other_cwl.txn_amount IS NOT NULL
    GROUP BY base.transaction_id
),
-- Posted wallet credit: non-zero REFUND or REVERSAL on this transaction
-- (Card Dispute Flow booklet "Refunded" examples).
wallet_refund AS (
    SELECT
          UPPER(TRIM(CAST(cwl.txn_category_txn_id AS VARCHAR))) AS txn_id
        , MAX(
            CASE
                WHEN UPPER(TRIM(CAST(cwl.txn_type AS VARCHAR))) IN ('REFUND', 'REVERSAL')
                 AND cwl.txn_amount IS NOT NULL
                 AND cwl.txn_amount <> 0
                THEN 1
                ELSE 0
            END
          ) AS wallet_refund_posted
    FROM polaris.`base.core_wallet_db`.customer_wallet_ledger cwl
    WHERE UPPER(TRIM(CAST(cwl.txn_category_txn_id AS VARCHAR))) IN (
        SELECT UPPER(TRIM(CAST(transaction_id AS VARCHAR)))
        FROM ticket_status
    )
    GROUP BY UPPER(TRIM(CAST(cwl.txn_category_txn_id AS VARCHAR)))
),
latest_provider_account AS (
    SELECT *
    FROM (
        SELECT
            al.txn_category_txn_id,
            TRIM(CAST(pa.account_number AS VARCHAR)) AS account_number,
            pa.account_name,
            al.amount,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
                ORDER BY al.updated_at DESC,
                         al.auto_id DESC
            ) AS rn
        FROM account_ledger_filtered al
        JOIN polaris.`base.core_accounting_db`.provider_accounts pa
            ON pa.account_number = al.account_number
    ) x
    WHERE rn = 1
),
ledger_summary AS (
    SELECT
          UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR))) AS txn_id
        , MAX(
            CASE
                WHEN LOWER(pa.account_name) LIKE '%clearing%'
                THEN 1
                ELSE 0
            END
          ) AS has_clearing
        , SUM(
            CASE
                WHEN LOWER(pa.account_name) LIKE '%clearing%'
                THEN ABS(al.amount)
                ELSE 0
            END
          ) AS clearing_amount
    FROM account_ledger_filtered al
    JOIN polaris.`base.core_accounting_db`.provider_accounts pa
        ON pa.account_number = al.account_number
    GROUP BY
        UPPER(TRIM(CAST(al.txn_category_txn_id AS VARCHAR)))
)
SELECT DISTINCT
      at2.transaction_id
    , tcs.queue_name
    , tcs.baraq_internal_id
    , tcs.CategoryLevel2_c
    , tcs.first_name
    , tcs.first_name_en
    , tcs.cif
    , at2.transaction_status
    , at2.payment_transaction_status
    , tpa.account_number AS provider_account_number
    , tpa.account_name AS provider_account_name
    , wt.txn_type
    -- On-hold: booklet Hold account numbers only (120316/120317/120318).
    , CASE
	    	WHEN ls.has_clearing = 1
	    	 AND ABS(ls.clearing_amount) >= ABS(wt.txn_amount)
	   		 THEN 0
	   		 WHEN LOWER(tpa.account_name) LIKE '%hold%'
	    	THEN 1
	    	ELSE 0
	  END AS is_hold_account
    -- Settled: booklet Clearing account numbers only.
    , CASE
		    WHEN ls.has_clearing = 1
		     AND ABS(ls.clearing_amount) >= ABS(wt.txn_amount)
		    THEN 1
		    WHEN LOWER(tpa.account_name) LIKE '%clearing%'
		    THEN 1
		    ELSE 0
		END AS is_settle_account
    , CASE
    WHEN LOWER(tpa.account_name) LIKE '%hold%'
		    OR LOWER(tpa.account_name) LIKE '%clearing%'
		    THEN 1
		    ELSE 0
	END AS is_dispute_provider_account
    , CASE
    WHEN wt.txn_amount = wt.pre_txn_balance
	    THEN 1
	    ELSE 0
	END AS is_balance_matching
    , cp.provision_status
    , COALESCE(cp.provision_status_within_two_weeks, 0) AS provision_status_within_two_weeks
    , cr.rejection_description
    -- Prefer card_product.type (CLASSIC=MADA). If that join is missing,
    -- fall back to provider account name so the Mada/Visa settle rules fire.
    , CASE
        WHEN cn.card_scheme_type IS NOT NULL
         AND UPPER(cn.card_scheme_type) = 'CLASSIC' THEN 'MADA'
        WHEN cn.card_scheme_type IS NOT NULL THEN 'VISA'
        WHEN LOWER(tpa.account_name) LIKE '%mada%' THEN 'MADA'
        WHEN LOWER(tpa.account_name) LIKE '%visa%' THEN 'VISA'
        ELSE NULL
      END AS card_network
    -- 1 only when we found a CANCELED/STOPPED instruction for this exact
    -- merchant+cif AND the disputed charge happened after that stop.
    -- 0 if a stop instruction exists but the charge predates it (or no
    -- stop instruction exists at all for this merchant).
    , CASE
        WHEN ss.stopped_at IS NOT NULL AND at2.created_date > ss.stopped_at THEN 1
        ELSE 0
      END AS charged_after_stop
    -- Distinct from charged_after_stop: 1 if ANY CANCELED/STOPPED
    -- instruction exists for this merchant+cif at all, regardless of
    -- charge timing. Used by the LLM-fallback issue classifier to tell
    -- "no stop on file at all" apart from "stop exists, this charge
    -- predates it" -- those need different customer messaging.
    , CASE
        WHEN ss.stopped_at IS NOT NULL THEN 1
        ELSE 0
      END AS has_stop_record
    , ss.merchant_name AS stopped_subscription_merchant
    , ss.stopped_at AS subscription_stopped_at
    -- 1 when another SUCCESS auth exists for same cif + merchant + amount
    -- within 5 minutes (confirmed duplicate charge). See billed_twice CTE.
    , COALESCE(bt.is_billed_twice, 0) AS is_billed_twice
    -- 1 only when this transaction has a non-zero REFUND row in the wallet.
    , COALESCE(wr.wallet_refund_posted, 0) AS wallet_refund_posted
FROM ticket_status tcs
INNER JOIN polaris.`base.card_hub_payment_engine_db`.authorization_transaction at2
    ON UPPER(TRIM(CAST(tcs.transaction_id AS VARCHAR)))
     = UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
    AND TRIM(CAST(at2.cif AS VARCHAR)) = TRIM(CAST(tcs.cif AS VARCHAR))
LEFT JOIN latest_provider_account tpa
    ON UPPER(TRIM(CAST(tpa.txn_category_txn_id AS VARCHAR)))
     = UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
LEFT JOIN ledger_summary ls
    ON ls.txn_id =
       UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
LEFT JOIN wallet_txn wt
    ON wt.txn_id =
       UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
    LEFT JOIN card_provision cp
    ON cp.cif = tcs.cif
LEFT JOIN card_rejection cr
    ON cr.card_alias = cp.card_alias
LEFT JOIN card_network cn
    ON cn.card_alias = cp.card_alias
LEFT JOIN subscription_stop ss
    ON ss.cif = tcs.cif
   AND ss.card_acceptor_id = at2.card_acceptor_id
LEFT JOIN billed_twice bt
    ON UPPER(TRIM(CAST(bt.transaction_id AS VARCHAR)))
     = UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
LEFT JOIN wallet_refund wr
    ON wr.txn_id = UPPER(TRIM(CAST(at2.transaction_id AS VARCHAR)))
