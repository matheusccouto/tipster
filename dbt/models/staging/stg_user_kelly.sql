WITH users AS (
    SELECT DISTINCT
        user
    FROM
        {{ ref("stg_user_bookmaker") }}
)
SELECT
    u.user,
    COALESCE(k.fraction, 0.25) AS fraction
FROM
    users AS u
LEFT JOIN {{ ref("user_kelly") }} AS k ON u.user = k.user