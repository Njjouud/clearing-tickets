-- Static RMT rules enrichment (StarRocks / mysql-connector).
-- @TransactionID_c replaced in product_services._run_query
--
-- SECURITY: forced ownership check added below. TransactionID_c is
-- customer-supplied text on the ticket; without this, a ticket referencing
-- someone else's transaction ID would leak that other user's transaction
-- data. We resolve the transaction's own owning account
-- (b_account_id -> accounts.account_no -> user_accounts_maps -> user_id)
-- and require it to equal the ticket's own BaraqInternalID_c. INNER JOINs
-- on the ownership chain mean an unresolvable or mismatched owner drops
-- the row entirely (fail closed) rather than returning it with nulls.

SELECT
    tcs.QueueName
    , tcs.BaraqInternalID_c
    , tcs.ProblemDescription
    , tcs.SrNumber
    , ROUND((UNIX_TIMESTAMP(CURRENT_DATE()) - UNIX_TIMESTAMP(t.transaction_date_time))/(3600*24),1) AS transaction_datetime_till_now
    , ROUND((UNIX_TIMESTAMP(CURRENT_DATE()) - UNIX_TIMESTAMP(tcs.CreationDate))/(3600*24),1) AS ticket_date_datetime_till_now
    , t.reference_id
    , tp.total_amount_to_collect_from_sender
    , UPPER(ci.name) AS country
    , t.mto_txn_reference_number
    , t.transaction_date_time
    , t.expected_date_of_delivery
    , t.mto_transaction_status
    , t.baraq_status
    , t.baraq_sub_status
    , t.frontend_status
    , t.finscan_status
    , t.sas_status
    , t.compliance_status
    , t.core_banking_status
    , t.last_mto_status_updated_on
    , t.created_at
    , t.updated_at
    , t.deleted_at
    , t.sas_reference_id
    , t.amend_required
    , t.otp_status
    , t.error_code
    , t.error_message
    , t.mto_transaction_sub_status
    , UPPER(doi.name) AS delivery_option
    , CASE
        WHEN t.terrapay_quote_id IS NOT NULL THEN 'TP'
        WHEN t.western_union_transaction_reference_id IS NOT NULL THEN 'WU'
        WHEN t.moneygram_mgi_transaction_session_id IS NOT NULL THEN 'MG'
        WHEN t.thunes_transaction_reference_id IS NOT NULL THEN 'Thunes'
        ELSE 'Issue'
      END AS provider
    , u.cif
    , u.first_name
    , u.first_name_en
FROM polaris.`base.oci_crm_db`.all_service_requests tcs
JOIN polaris.`base.remittance_baraq`.transaction t
    ON UPPER(TRIM(CAST(tcs.TransactionID_c AS VARCHAR)))
     = UPPER(TRIM(CAST(t.reference_id AS VARCHAR)))
LEFT JOIN polaris.`base.remittance_baraq`.transaction_price tp
    ON t.transaction_price = tp.id
LEFT JOIN polaris.`base.remittance_baraq`.beneficiary_ir bi
    ON t.beneficiary = bi.id
LEFT JOIN polaris.`base.remittance_baraq`.country_receive_details_ir crdi
    ON bi.corridor = crdi.id
LEFT JOIN polaris.`base.remittance_baraq`.country_ir ci
    ON crdi.country = ci.id
LEFT JOIN polaris.`base.remittance_baraq`.delivery_option_ir doi
    ON crdi.delivery_option = doi.id
LEFT JOIN polaris.`base.customer_identity_db`.users u
    ON TRIM(CAST(u.user_id AS VARCHAR)) = TRIM(CAST(tcs.BaraqInternalID_c AS VARCHAR))
-- Forced ownership check: resolve who actually owns this transaction's account
INNER JOIN polaris.`base.customer_identity_db`.accounts owner_acct
    ON TRIM(CAST(owner_acct.account_no AS VARCHAR)) = TRIM(CAST(t.b_account_id AS VARCHAR))
INNER JOIN polaris.`base.customer_identity_db`.user_accounts_maps owner_map
    ON owner_map.account_id = owner_acct.id
WHERE t.baraq_status != 'PRE_COMMIT'
  AND UPPER(TRIM(CAST(t.reference_id AS VARCHAR)))
      = UPPER(TRIM('@TransactionID_c'))
  -- Forced filter: the transaction's real owner must equal the ticket's own user
  AND TRIM(CAST(owner_map.user_id AS VARCHAR)) = TRIM(CAST(tcs.BaraqInternalID_c AS VARCHAR));
