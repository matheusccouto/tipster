SELECT
    spi.season AS season,
    league.tipster AS league,
    concat(emoji.emoji, ' ', league.tipster) AS league_emoji,
    home.tipster AS home,
    away.tipster AS away,
    spi.prob1 AS prob_home,
    spi.prob2 AS prob_away,
    spi.probtie AS prob_draw,
    DATE(spi.date) AS date
FROM
    {{ source ('fivethirtyeight', 'spi') }} AS spi
    LEFT JOIN {{ ref("league_name") }} AS league ON spi.league_id = league.fivethirtyeight
    LEFT JOIN {{ ref("emoji") }} AS emoji ON league.country = emoji.country
    LEFT JOIN {{ ref("club_name") }} AS home ON spi.team1 = home.fivethirtyeight
    LEFT JOIN {{ ref("club_name") }} AS away ON spi.team2 = away.fivethirtyeight
