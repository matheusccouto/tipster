SELECT
  b.*
FROM
  {{ ref("dim_bets") }} AS b
  LEFT JOIN {{ ref("stg_sent") }} AS s ON b.user = s.user AND b.id = s.id
WHERE
  b.ev > IFNULL(s.ev, 0)
