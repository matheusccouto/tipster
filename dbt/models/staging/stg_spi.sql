SELECT
    spi.season AS season,
    spi.league_id AS league_id,
    spi.league AS league,
    home.tipster AS home,
    away.tipster AS away,
    spi.prob1 AS prob_home,
    spi.prob2 AS prob_away,
    spi.probtie AS prob_draw,
    DATE(spi.date) AS date
FROM
    {{ source ('fivethirtyeight', 'spi') }} AS spi
    LEFT JOIN {{ ref("name") }} As home ON spi.team1 = home.fivethirtyeight
    LEFT JOIN {{ ref("name") }} As away ON spi.team2 = away.fivethirtyeight
WHERE  -- FIXME: Temporary
  league = 'English League Championship'
  and season = 2022
