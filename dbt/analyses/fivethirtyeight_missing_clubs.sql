WITH spi AS (
    SELECT DISTINCT
        date,
        league,
        team1,
        team2,
        c1.new AS team1_new,
        c2.new as team2_new
    FROM
        {{ source("fivethirtyeight", "spi") }}
    LEFT JOIN
        {{ ref("club") }} AS c1 ON team1 = c1.old
    LEFT JOIN
        {{ ref("club") }} AS c2 ON team2 = c2.old
    WHERE
        c1.new IS NULL OR c2.new IS NULL
    
    UNION ALL

    SELECT DISTINCT
        date,
        'NFL',
        team1,
        team2,
        c1.new AS team1_new,
        c2.new as team2_new
    FROM
        {{ source("fivethirtyeight", "nfl") }}
    LEFT JOIN
        {{ ref("nfl") }} AS c1 ON team1 = c1.old
    LEFT JOIN
        {{ ref("nfl") }} AS c2 ON team2 = c2.old
    WHERE
        c1.new IS NULL OR c2.new IS NULL
    
    UNION ALL

    SELECT DISTINCT
        date,
        'NBA',
        team1,
        team2,
        c1.new AS team1_new,
        c2.new as team2_new
    FROM
        {{ source("fivethirtyeight", "nba") }}
    LEFT JOIN
        {{ ref("nba") }} AS c1 ON team1 = c1.old
    LEFT JOIN
        {{ ref("nba") }} AS c2 ON team2 = c2.old
    WHERE
        c1.new IS NULL OR c2.new IS NULL
    
    UNION ALL

    SELECT DISTINCT
        date,
        'WNBA',
        home_team AS team1,
        away_team AS team2,
        c1.new AS team1_new,
        c2.new as team2_new
    FROM
        {{ source("fivethirtyeight", "wnba") }}
    LEFT JOIN
        {{ ref("wnba") }} AS c1 ON home_team = c1.old
    LEFT JOIN
        {{ ref("wnba") }} AS c2 ON away_team = c2.old
    WHERE
        c1.new IS NULL OR c2.new IS NULL

    UNION ALL

    SELECT DISTINCT
        date,
        'MLB',
        team1,
        team2,
        c1.new AS team1_new,
        c2.new as team2_new
    FROM
        {{ source("fivethirtyeight", "mlb") }}
    LEFT JOIN
        {{ ref("mlb") }} AS c1 ON team1 = c1.old
    LEFT JOIN
        {{ ref("mlb") }} AS c2 ON team2 = c2.old
    WHERE
        c1.new IS NULL OR c2.new IS NULL

    UNION ALL

    SELECT DISTINCT
        date,
        'NHL',
        home_team AS team1,
        away_team AS team2,
        c1.new AS team1_new,
        c2.new as team2_new
    FROM
        {{ source("fivethirtyeight", "nhl") }}
    LEFT JOIN
        {{ ref("nhl") }} AS c1 ON home_team = c1.old
    LEFT JOIN
        {{ ref("nhl") }} AS c2 ON away_team = c2.old
    WHERE
        c1.new IS NULL OR c2.new IS NULL
),

teams AS (
    SELECT
        s.league,
        s.team1 AS team,
        s.team1_new AS team_new
    FROM spi AS s

    UNION ALL

    SELECT
        s.league,
        s.team2 AS team,
        s.team2_new AS team_new
    FROM spi AS s
)

SELECT DISTINCT *
FROM teams
WHERE team_new IS NULL
ORDER BY league, team
