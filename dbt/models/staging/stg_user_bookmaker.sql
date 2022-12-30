SELECT
    u.user,
    b.key,
    b.bookmaker AS name  -- noqa: L029
FROM
    {{ ref("user_bookmaker") }} AS u
LEFT JOIN {{ ref("bookmaker") }} AS b ON u.bookmaker = b.key