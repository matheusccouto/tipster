WITH users AS (
    SELECT DISTINCT
        user
    FROM
        {{ ref("stg_user_bookmaker") }}
)
SELECT
    u.user,
    COALESCE(ev.ev, 0) AS ev
FROM
    users AS u
LEFT JOIN {{ ref("user_ev") }} AS ev ON u.user = ev.user