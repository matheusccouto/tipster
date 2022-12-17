WITH match AS (
  SELECT
    *,
    greatest(ev_home, ev_draw, ev_away) AS ev,
    CASE
      WHEN ev_home = greatest(ev_home, ev_draw, ev_away) THEN 'home'
      WHEN ev_draw = greatest(ev_home, ev_draw, ev_away) THEN 'draw'
      WHEN ev_away = greatest(ev_home, ev_draw, ev_away) THEN 'away'
    END AS bet
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
  m.bet,
  m.ev,
FROM
  match AS m
  INNER JOIN {{ ref("user_bookmaker") }} AS ubk ON m.bookmaker_key = ubk.bookmaker
  LEFT JOIN {{ ref("user_ev") }} AS uev ON ubk.user = uev.user
WHERE
  m.start_at > current_timestamp()
  AND m.ev >= uev.ev
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.id, ubk.user ORDER BY m.ev DESC) = 1
