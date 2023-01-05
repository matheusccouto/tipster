SELECT
    u.user,
    b.key,
    b.name
FROM
    {{ source("tipster", "user_bookmaker") }} AS u
LEFT JOIN {{ ref("bookmaker") }} AS b ON u.bookmaker = b.key
