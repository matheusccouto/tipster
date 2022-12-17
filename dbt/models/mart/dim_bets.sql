WITH match AS (
  SELECT
    *,
    greatest(ev_home, ev_draw, ev_away) AS ev
  FROM
    {{ ref("fct_match") }}
)
SELECT
  m.id,
  m.start_at,
  m.league_key,
  m.league_name,
  m.home,
  m.away,
  m.bookmaker_key,
  m.bookmaker_name,
  m.updated_at,
  m.market_key,
  m.price_home,
  m.price_draw,
  m.price_away,
  ubk.user,
  m.ev,
  CASE
    WHEN m.ev_home = m.ev THEN 'home'
    WHEN m.ev_draw = m.ev THEN 'draw'
    WHEN m.ev_away = m.ev THEN 'away'
  END AS bet,
  CASE
    WHEN m.ev_home = m.ev THEN price_home
    WHEN m.ev_draw = m.ev THEN price_draw
    WHEN m.ev_away = m.ev THEN price_away
  END AS price
FROM
  match AS m
  INNER JOIN {{ ref("user_bookmaker") }} AS ubk ON m.bookmaker_key = ubk.bookmaker
  LEFT JOIN {{ ref("user_ev") }} AS uev ON ubk.user = uev.user
WHERE
  m.start_at > current_timestamp()
  AND m.ev >= uev.ev
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.id, ubk.user ORDER BY m.ev DESC) = 1
