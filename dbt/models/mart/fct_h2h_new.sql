SELECT b.*
FROM
    {{ ref("fct_h2h") }} AS b
LEFT JOIN {{ ref("stg_sent") }} AS s ON b.user = s.user AND b.id = s.id
WHERE
    b.ev > COALESCE(s.ev, 0)
