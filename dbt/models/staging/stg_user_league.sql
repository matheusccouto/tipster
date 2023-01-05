SELECT
    u.user,
    l.id AS key,  -- noqa: L029
    concat(f.emoji, ' ', l.tipster) AS name  -- noqa: L029
FROM
    {{ source("tipster", "user_league") }} AS u
LEFT JOIN {{ ref("league") }} AS l ON u.league = l.id
LEFT JOIN {{ ref("flag") }} AS f ON l.country = f.country
