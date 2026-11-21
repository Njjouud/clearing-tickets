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

-- A user has many application-check records (one or more per check type,
-- e.g. AML, FINSCAN, ADDRESS). Take the latest non-deleted record of EACH
-- check type, then summarise:
--   any FAILED      -> FAILED      (Compliance)
--   else any PENDING    -> PENDING     (Compliance)
--   else any INCOMPLETE -> INCOMPLETE  (Technology)
--   all PASSED          -> PASSED
--   anything else       -> OTHER       (matches no rule -> review)
latest_check_per_type AS (
    SELECT *
    FROM (
        SELECT
              c.user_id
            , c.check_type
            , UPPER(TRIM(c.status)) AS status
            , ROW_NUMBER() OVER (
                PARTITION BY c.user_id, c.check_type
                ORDER BY c.updated_at DESC, c.created_at DESC, c.id DESC
              ) AS rn
        FROM polaris.`base.customer_identity_db`.user_application_checks c
        WHERE c.user_id = '@BaraqInternalID_c'
          AND c.deleted_at IS NULL
    ) x
    WHERE rn = 1
),

check_summary AS (
    SELECT
          user_id
        , COUNT(*)                                              AS app_checks_total
        , SUM(CASE WHEN status = 'PASSED'     THEN 1 ELSE 0 END) AS app_checks_passed
        , SUM(CASE WHEN status = 'FAILED'     THEN 1 ELSE 0 END) AS app_checks_failed
        , SUM(CASE WHEN status = 'PENDING'    THEN 1 ELSE 0 END) AS app_checks_pending
        , SUM(CASE WHEN status = 'INCOMPLETE' THEN 1 ELSE 0 END) AS app_checks_incomplete
        , GROUP_CONCAT(
            CASE WHEN status <> 'PASSED' OR status IS NULL
                 THEN CONCAT(check_type, ':', COALESCE(status, 'NULL')) END
          ) AS app_checks_not_passed
    FROM latest_check_per_type
    GROUP BY user_id
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
    , CASE
        WHEN cs.app_checks_total IS NULL               THEN NULL
        WHEN cs.app_checks_failed > 0                  THEN 'FAILED'
        WHEN cs.app_checks_pending > 0                 THEN 'PENDING'
        WHEN cs.app_checks_incomplete > 0              THEN 'INCOMPLETE'
        WHEN cs.app_checks_passed = cs.app_checks_total THEN 'PASSED'
        ELSE 'OTHER'
      END AS app_check_status
    , cs.app_checks_total
    , cs.app_checks_not_passed
    , ma.account_no
    , ma.account_status
    , ma.account_type
    , wl.wallet_balance
    , wl.wallet_buckets
    , COALESCE(bl.is_blocked_account, 0) AS is_blocked_account
    , COALESCE(mn.is_minor, 0)           AS is_minor
FROM u
LEFT JOIN check_summary cs ON cs.user_id = u.user_id
LEFT JOIN main_account ma ON 1 = 1
LEFT JOIN wallet wl       ON wl.account_no = ma.account_no
LEFT JOIN blocked bl      ON bl.account_no = ma.account_no
LEFT JOIN minor mn        ON mn.child_user_id = u.user_id;
