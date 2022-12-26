SELECT
    spi.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.tipster AS home,
    away.tipster AS away,
    spi.prob1 AS prob_home,
    spi.prob2 AS prob_away,
    spi.probtie AS prob_draw,
    DATE(spi.date) AS date
FROM
    {{ source ('fivethirtyeight', 'spi') }} AS spi
    LEFT JOIN {{ ref("league") }} AS league ON spi.league_id = league.fivethirtyeight
    LEFT JOIN {{ ref("club") }} AS home ON spi.team1 = home.fivethirtyeight
    LEFT JOIN {{ ref("club") }} AS away ON spi.team2 = away.fivethirtyeight
