SELECT
    u.user,
    b.key,
    b.name
FROM
    {{ source("tipster", "user_bookmaker") }} AS u
LEFT JOIN {{ ref("stg_bookmaker") }} AS b ON u.bookmaker = b.key
