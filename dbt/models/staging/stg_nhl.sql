SELECT
    nhl.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.new AS home,
    away.new AS away,
    nhl.home_team_winprob AS prob_home,
    nhl.away_team_winprob AS prob_away,
    nhl.game_quality_rating AS quality,
    nhl.game_importance_rating AS importance,
    nhl.game_overall_rating AS rating,
    nhl.home_team_score AS score_home,
    nhl.away_team_score AS score_away,
    DATE(nhl.date) AS date  -- noqa: L029
FROM
    {{ source ('fivethirtyeight', 'nhl') }} AS nhl
LEFT JOIN
    {{ ref("league") }} AS league ON league.id = 40010
LEFT JOIN
    {{ ref("nhl") }} AS home ON nhl.home_team = home.old
LEFT JOIN
    {{ ref("nhl") }} AS away ON nhl.away_team = away.old
