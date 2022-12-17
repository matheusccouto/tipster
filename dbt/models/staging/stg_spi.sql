SELECT
    spi.season AS season,
    spi.league_id AS league_id,
    spi.league AS league,
    spi.team1 AS home,
    spi.team2 AS away,
    spi.prob1 AS prob_home,
    spi.prob2 AS prob_away,
    spi.probtie AS prob_draw,
    DATE(spi.date) AS date
FROM
    {{ source ('fivethirtyeight', 'spi') }} AS spi
