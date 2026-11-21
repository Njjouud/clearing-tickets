-- Account & Registration tickets: customer / account profile by User ID.
-- @BaraqInternalID_c is the ticket's own User ID (already trimmed and
-- quote-escaped by AccountsEngine), so every lookup is scoped to the
-- ticket owner -- no customer-supplied transaction id is involved.
--
-- Joins (per Account Operation):
--   users.user_id = user_accounts_maps.user_id
--   accounts.id   = user_accounts_maps.account_id

WITH u AS (
    SELECT
          u.user_id
        , u.cif
        , u.cif_status
        , u.cif_statuses
        , u.cif_super_status
        , u.id_expiry_date_g
        , u.first_name
        , u.first_name_en
    FROM polaris.`base.customer_identity_db`.users u
    WHERE u.user_id = '@BaraqInternalID_c'
),

-- A user can have many application-check records: use the latest one
-- (ignoring soft-deleted records).
latest_check AS (
    SELECT *
    FROM (
        SELECT
              c.user_id
            , c.check_type
            , c.status
            , c.mode
            , c.needs_user_input
            , c.updated_at
            , ROW_NUMBER() OVER (
                PARTITION BY c.user_id
                ORDER BY c.updated_at DESC, c.created_at DESC, c.id DESC
              ) AS rn
        FROM polaris.`base.customer_identity_db`.user_application_checks c
        WHERE c.user_id = '@BaraqInternalID_c'
          AND c.deleted_at IS NULL
    ) x
    WHERE rn = 1
),

-- The customer's account: MAIN first, then the most recently updated.
main_account AS (
    SELECT *
    FROM (
        SELECT
              a.id
            , a.account_no
            , a.account_status
            , a.account_type
            , ROW_NUMBER() OVER (
                PARTITION BY m.user_id
                ORDER BY CASE WHEN a.account_type = 'MAIN' THEN 0 ELSE 1 END,
                         a.updated_at DESC,
                         a.id DESC
              ) AS rn
        FROM polaris.`base.customer_identity_db`.user_accounts_maps m
        JOIN polaris.`base.customer_identity_db`.accounts a
          ON a.id = m.account_id
        WHERE m.user_id = '@BaraqInternalID_c'
    ) x
    WHERE rn = 1
),

-- Wallet balance of that account (sum over its buckets).
wallet AS (
    SELECT
          w.account_no
        , SUM(w.balance) AS wallet_balance
        , COUNT(*)       AS wallet_buckets
    FROM polaris.`base.core_wallet_db`.customer_wallet w
    WHERE w.account_no IN (SELECT account_no FROM main_account)
    GROUP BY w.account_no
),

-- Account currently blocked (row not soft-deleted).
blocked AS (
    SELECT b.account_no, 1 AS is_blocked_account
    FROM polaris.`base.customer_identity_db`.blocked_accounts b
    WHERE b.account_no IN (SELECT account_no FROM main_account)
      AND b.deleted_at IS NULL
    GROUP BY b.account_no
),

-- The ticket owner is a minor (child in a parent/child mapping).
minor AS (
    SELECT p.child_user_id, 1 AS is_minor
    FROM polaris.`base.customer_identity_db`.parent_child_map p
    WHERE p.child_user_id = '@BaraqInternalID_c'
      AND p.deleted_at IS NULL
    GROUP BY p.child_user_id
)

SELECT
      u.user_id
    , u.cif
    , u.cif_status
    , u.cif_statuses
    , u.cif_super_status
    , u.id_expiry_date_g
    , u.first_name
    , u.first_name_en
    , lc.check_type        AS app_check_type
    , lc.status            AS app_check_status
    , lc.mode              AS app_check_mode
    , lc.needs_user_input  AS app_check_needs_user_input
    , lc.updated_at        AS app_check_updated_at
    , ma.account_no
    , ma.account_status
    , ma.account_type
    , wl.wallet_balance
    , wl.wallet_buckets
    , COALESCE(bl.is_blocked_account, 0) AS is_blocked_account
    , COALESCE(mn.is_minor, 0)           AS is_minor
FROM u
LEFT JOIN latest_check lc ON lc.user_id = u.user_id
LEFT JOIN main_account ma ON 1 = 1
LEFT JOIN wallet wl       ON wl.account_no = ma.account_no
LEFT JOIN blocked bl      ON bl.account_no = ma.account_no
LEFT JOIN minor mn        ON mn.child_user_id = u.user_id;
