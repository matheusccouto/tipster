WITH users AS (
    SELECT DISTINCT user
    FROM
        {{ ref("stg_user_bookmaker") }}
)

SELECT
    u.user,
    COALESCE(k.fraction, 0.25) AS fraction
FROM
    users AS u
LEFT JOIN {{ source("tipster", "user_kelly") }} AS k ON u.user = k.user
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user ORDER BY k.updated_at DESC) = 1
