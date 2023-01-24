WITH tips AS (
  SELECT
    *
  FROM
    `tipster-main`.`tipster`.`fct_h2h_snapshot`
  WHERE
    bookmaker_key IN ('{bookmakers}') AND league_id IN ({leagues})
  QUALIFY
    row_number() OVER (PARTITION BY id ORDER BY ev DESC) = 1
),
bets AS (
  SELECT
    *,
    0.1 * kelly * 100 AS amount
  FROM
    tips
  WHERE
    ev > 0

)
SELECT
  replace(message, '{kelly}', CAST(ROUND(amount, 2) AS STRING)) AS message,
  start_at,
  date(start_at) AS start_date,
  bookmaker_key,
  amount,
  price,
  ev,
  CASE
    WHEN outcome IS TRUE THEN amount * price
    ELSE -1 * amount
  END AS result
FROM
  bets
WHERE
  outcome IS NOT NULL
ORDER BY
  start_at
