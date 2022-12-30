WITH users AS (
    SELECT DISTINCT
        user
    FROM
        {{ ref("user_bookmaker") }}
)
SELECT
    u.user,
    COALESCE(ev.ev, 0)
FROM
    users AS u
LEFT JOIN {{ ref("user_ev") }} AS ev ON u.user = ev.ev