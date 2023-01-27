SELECT
    l.id AS key,  -- noqa: L029
    concat(s.emoji, f.emoji, ' ', l.tipster) AS name  -- noqa: L029
FROM
    {{ ref("league") }} AS l
LEFT JOIN
    {{ ref("flag") }} AS f ON l.country = f.country
LEFT JOIN
    {{ ref("sport") }} AS s on l.sport = s.key
WHERE
    l.theoddsapi IS NOT NULL