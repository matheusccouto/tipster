SELECT
    wnba.season AS season,
    league.id AS league_id,
    league.tipster AS league_name,
    home.new AS home,
    away.new AS away,
    wnba.home_team_winprob AS prob_home,
    wnba.away_team_winprob AS prob_away,
    wnba.home_team_score AS score_home,
    wnba.away_team_score AS score_away,
    DATE(wnba.date) AS date  -- noqa: L029
FROM
    {{ source ('fivethirtyeight', 'wnba') }} AS wnba
LEFT JOIN
    {{ ref("league") }} AS league ON league.id = 21010
LEFT JOIN
    {{ ref("wnba") }} AS home ON wnba.home_team = home.old
LEFT JOIN
    {{ ref("wnba") }} AS away ON wnba.away_team = away.old
