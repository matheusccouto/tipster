WITH users AS (
    SELECT DISTINCT
        user
    FROM
        {{ ref("stg_user_bookmaker") }}
)
SELECT
    u.user,
    COALESCE(b.bankroll, 0) AS bankroll
FROM
    users AS u
LEFT JOIN {{ ref("user_bankroll") }} AS b ON u.user = b.user