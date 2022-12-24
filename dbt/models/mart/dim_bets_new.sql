SELECT
  b.*
FROM
  {{ ref("dim_bets") }} AS b
  LEFT JOIN {{ ref("stg_sent") }} AS s ON b.user = s.user AND b.id = s.id
WHERE
  round(b.ev, 3) > fnull(round(s.ev, 3), 0)
