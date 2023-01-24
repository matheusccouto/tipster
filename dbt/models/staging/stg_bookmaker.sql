SELECT
    b.key,
    b.name
FROM
    {{ ref("bookmaker") }} AS b
