SELECT
    nba.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.new AS home,
    away.new AS away,
    nba.raptor_prob1 AS prob_home,
    nba.raptor_prob2 AS prob_away,
    nba.quality AS quality,
    nba.importance AS importance,
    nba.total_rating AS rating,
    nba.score1 AS score_home,
    nba.score2 AS score_away,
    DATE(nba.date) AS date  -- noqa: L029
FROM
    {{ source ('fivethirtyeight', 'nba') }} AS nba
LEFT JOIN
    {{ ref("league") }} AS league ON league.id = 20010
LEFT JOIN
    {{ ref("nba") }} AS home ON nba.team1 = home.old
LEFT JOIN
    {{ ref("nba") }} AS away ON nba.team2 = away.old
