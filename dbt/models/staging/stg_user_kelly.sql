WITH users AS (
    SELECT DISTINCT
        user
    FROM
        {{ ref("user_bookmaker") }}
)
SELECT
    u.user,
    COALESCE(k.fraction, 0) AS fraction
FROM
    users AS u
LEFT JOIN {{ ref("user_kelly") }} AS k ON u.user = k.user