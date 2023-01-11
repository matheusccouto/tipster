WITH spi AS (
    SELECT
        *,
        (2 * spi.spi1 * spi.spi2) / (spi.spi1 + spi.spi2) AS quality,
        (spi.importance1 + spi.importance2) / 2 AS importance
    FROM
        {{ source ('fivethirtyeight', 'spi') }} AS spi
)

SELECT
    spi.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.new AS home,
    away.new AS away,
    spi.prob1 AS prob_home,
    spi.prob2 AS prob_away,
    spi.probtie AS prob_draw,
    spi.spi1 AS spi_home,
    spi.spi2 AS spi_away,
    spi.proj_score1 AS proj_score_home,
    spi.proj_score2 AS proj_score_away,
    spi.importance1 AS importance_home,
    spi.importance2 AS importance_away,
    spi.quality AS quality,
    spi.importance AS importance,
    spi.score1 AS score_home,
    spi.score2 AS score_away,
    (spi.quality + spi.importance) / 2 AS rating,
    DATE(spi.date) AS date  -- noqa: L029
FROM
    spi
LEFT JOIN
    {{ ref("league") }} AS league ON spi.league_id = league.fivethirtyeight
LEFT JOIN {{ ref("club") }} AS home ON spi.team1 = home.old
LEFT JOIN {{ ref("club") }} AS away ON spi.team2 = away.old
