SELECT
    mlb.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.new AS home,
    away.new AS away,
    mlb.rating_prob1 AS prob_home,
    mlb.rating_prob2 AS prob_away,
    mlb.score1 AS score_home,
    mlb.score2 AS score_away,
    DATE(mlb.date) AS date  -- noqa: L029
FROM
    {{ source ('fivethirtyeight', 'mlb') }} AS mlb
LEFT JOIN
    {{ ref("league") }} AS league ON league.id = 30010
LEFT JOIN
    {{ ref("mlb") }} AS home ON mlb.team1 = home.old
LEFT JOIN
    {{ ref("mlb") }} AS away ON mlb.team2 = away.old
