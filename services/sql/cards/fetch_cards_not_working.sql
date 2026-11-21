-- fetch_cards_delivery.sql
-- Card delivery / card received investigation

WITH ticket_data AS (
    SELECT
          t.BaraqInternalID_c
        , t.TransactionID_c
        , u.cif
        , u.first_name
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

latest_card_order AS (
    SELECT *
    FROM (
        SELECT
              cso.cif
            , cso.alias AS card_alias
            , cso.card_product_id
            , cso.status
            , cso.created_date
            , cso.modified_date
            , ROW_NUMBER() OVER (
                PARTITION BY cso.cif
                ORDER BY cso.modified_date DESC,
                         cso.created_date DESC
              ) AS rn
        FROM polaris.`base.card_hub_app_db`.card_subscription_order cso
        WHERE cso.cif IN (
            SELECT cif
            FROM ticket_data
            WHERE cif IS NOT NULL
        )
    ) x
    WHERE rn = 1
),

-- Latest card provision + latest rejection (same logic as
-- fetch_cards_transaction.sql). Card Not Working rules 10-14 key off
-- provision_status / rejection_description / provision_status_within_two_weeks.
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
        WHERE cp.cif IN (SELECT cif FROM ticket_data WHERE cif IS NOT NULL)
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

card_product_info AS (
    SELECT
          cp.id
        , cp.type AS card_type
    FROM polaris.`base.card_hub_app_db`.card_product cp
)

SELECT
      td.cif
    , td.first_name
    , td.first_name_en
    , lco.card_alias
    , lco.status AS card_order_status
    , lco.created_date AS card_order_created_date
    , lco.modified_date AS card_order_modified_date
    , cpi.card_type
    , prov.provision_status
    , COALESCE(prov.provision_status_within_two_weeks, 0) AS provision_status_within_two_weeks
    , rej.rejection_description

    , CASE
        WHEN cpi.card_type IS NOT NULL
        THEN 1
        ELSE 0
      END AS has_physical_card

    , CASE
        WHEN LOWER(COALESCE(cpi.card_type, '')) = 'digital'
        THEN 1
        ELSE 0
      END AS digital_only

FROM ticket_data td

LEFT JOIN latest_card_order lco
    ON lco.cif = td.cif

LEFT JOIN card_product_info cpi
    ON cpi.id = lco.card_product_id

LEFT JOIN card_provision prov
    ON prov.cif = td.cif

LEFT JOIN card_rejection rej
    ON rej.card_alias = prov.card_alias;