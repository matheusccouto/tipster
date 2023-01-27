WITH bet AS (
  SELECT
    bet.user,
    bet.message,
    bet.updated_at AS placed_at,
    h2h.* EXCEPT (message, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to)
  FROM
    {{ ref("stg_bet") }} AS bet
  INNER JOIN
     {{ ref("fct_h2h_snapshot") }} AS h2h 
      ON REGEXP_REPLACE(bet.message, r'\$[0-9]+(\.[0-9]{2})?', '${kelly}') = REGEXP_REPLACE(h2h.message, r'on \[(.*?)\]\((.*?)\) at', 'on \\1 at')
        AND bet.updated_at > h2h.loaded_at
  QUALIFY
    row_number() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
),
res AS (
  SELECT
    bet.* EXCEPT(outcome),
    h2h.outcome,
    CAST(REGEXP_EXTRACT(bet.message, r'Bet \$([0-9]*\.[0-9]+)') AS FLOAT64) as amount
  FROM
    bet
  INNER JOIN
    {{ ref("fct_h2h") }} AS h2h
      ON bet.id = h2h.id
      AND bet.bookmaker_key = h2h.bookmaker_key
      AND bet.bet = h2h.bet
)
SELECT
  *,
  CASE
    WHEN outcome IS TRUE THEN amount * price
    WHEN outcome IS FALSE THEN 0
  END AS prize,
  CASE
    WHEN outcome IS TRUE THEN amount * price
    WHEN outcome IS FALSE THEN -1 * amount
  END AS result
FROM
  res