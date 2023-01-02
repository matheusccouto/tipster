WITH users AS (
    SELECT DISTINCT user
    FROM
        {{ ref("stg_user_bookmaker") }}
)

SELECT
    u.user,
    COALESCE(b.bankroll, 0) AS bankroll
FROM
    users AS u
LEFT JOIN {{ ref("user_bankroll") }} AS b ON u.user = b.user
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user ORDER BY b.updated_at DESC) = 1
