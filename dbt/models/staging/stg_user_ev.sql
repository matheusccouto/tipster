WITH users AS (
    SELECT DISTINCT user
    FROM
        {{ ref("stg_user_bookmaker") }}
)

SELECT
    u.user,
    COALESCE(e.ev, 0) AS ev
FROM
    users AS u
LEFT JOIN {{ source("tipster", "user_ev") }} AS e ON u.user = e.user
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user ORDER BY e.updated_at DESC) = 1
