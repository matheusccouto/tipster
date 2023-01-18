SELECT
    nfl.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.new AS home,
    away.new AS away,
    nfl.qbelo_prob1 AS prob_home,
    nfl.qbelo_prob2 AS prob_away,
    nfl.quality AS quality,
    nfl.importance AS importance,
    nfl.total_rating AS rating,
    nfl.score1 AS score_home,
    nfl.score2 AS score_away,
    DATE(nfl.date) AS date  -- noqa: L029
FROM
    {{ source ('fivethirtyeight', 'nfl') }} AS nfl
LEFT JOIN
    {{ ref("league") }} AS league ON league.id = 10010
LEFT JOIN
    {{ ref("nfl") }} AS home ON nfl.team1 = home.old
LEFT JOIN
    {{ ref("nfl") }} AS away ON nfl.team2 = away.old
