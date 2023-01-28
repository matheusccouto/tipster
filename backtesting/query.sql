WITH tips AS (
  SELECT
    *
  FROM
    `tipster`.`fct_h2h_snapshot`
  WHERE
    bookmaker_key IN ('{bookmakers}')
    AND league_id IN ({leagues})
    AND date(start_at) >= '{start}'
    AND date(start_at) <= '{end}'
  QUALIFY
    row_number() OVER (PARTITION BY id ORDER BY ev DESC) = 1
),
bets AS (
  SELECT
    *,
    {factor} * kelly * 100 AS amount
  FROM
    tips
  WHERE
    ev > {ev}

)
SELECT
  replace(message, '{kelly}', CAST(ROUND(amount, 2) AS STRING)) AS message,
  start_at,
  date(start_at) AS start_date,
  bookmaker_key,
  league_id,
  amount,
  price,
  ev,
  CASE
    WHEN outcome IS TRUE THEN amount * price
    ELSE 0
  END AS prize,
  CASE
    WHEN outcome IS TRUE THEN amount * price - amount
    ELSE -1 * amount
  END AS result
FROM
  bets
WHERE
  outcome IS NOT NULL