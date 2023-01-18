WITH odds AS (
    SELECT DISTINCT
        commence_time,
        sport_key,
        home_team,
        away_team
    FROM
        {{ source("theoddsapi", "odds") }}
),

teams AS (
    SELECT
        o.sport_key,
        o.home_team AS team
    FROM odds AS o
    LEFT JOIN {{ ref("club") }} AS club ON o.home_team = club.old
    LEFT JOIN {{ ref("mlb") }} AS mlb ON o.home_team = mlb.old
    LEFT JOIN {{ ref("nba") }} AS nba ON o.home_team = nba.old
    LEFT JOIN {{ ref("nfl") }} AS nfl ON o.home_team = nfl.old
    LEFT JOIN {{ ref("nhl") }} AS nhl ON o.home_team = nhl.old
    LEFT JOIN {{ ref("wnba") }} AS wnba ON o.home_team = wnba.old
    WHERE
        club.new IS NULL
        AND mlb.new IS NULL 
        AND nba.new IS NULL 
        AND nfl.new IS NULL 
        AND nhl.new IS NULL 
        AND wnba.new IS NULL 

    UNION ALL

    SELECT
        o.sport_key,
        o.away_team AS team
    FROM odds AS o
    LEFT JOIN {{ ref("club") }} AS club ON o.away_team = club.old
    LEFT JOIN {{ ref("mlb") }} AS mlb ON o.away_team = mlb.old
    LEFT JOIN {{ ref("nba") }} AS nba ON o.away_team = nba.old
    LEFT JOIN {{ ref("nfl") }} AS nfl ON o.away_team = nfl.old
    LEFT JOIN {{ ref("nhl") }} AS nhl ON o.away_team = nhl.old
    LEFT JOIN {{ ref("wnba") }} AS wnba ON o.away_team = wnba.old
    WHERE
        club.new IS NULL
        AND mlb.new IS NULL 
        AND nba.new IS NULL 
        AND nfl.new IS NULL 
        AND nhl.new IS NULL 
        AND wnba.new IS NULL 
)

SELECT DISTINCT *
FROM teams
ORDER BY sport_key, team
