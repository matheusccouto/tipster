SELECT
    u.user,
    l.key,
    l.name
FROM
    {{ source("tipster", "user_league") }} AS u
LEFT JOIN {{ ref("stg_league") }} AS l ON u.league = l.key
