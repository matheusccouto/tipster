WITH spi AS (
    SELECT DISTINCT
        date,
        league,
        team1,
        team2
    FROM
        {{ source("fivethirtyeight", "spi") }}
),

teams AS (
    SELECT
        s.league,
        s.team1 AS team
    FROM spi AS s
    LEFT JOIN {{ ref("club") }} AS c ON s.team1 = c.old
    WHERE c.new IS NULL

    UNION ALL

    SELECT
        s.league,
        s.team2 AS team
    FROM spi AS s
    LEFT JOIN {{ ref("club") }} AS c ON s.team2 = c.old
    WHERE c.new IS NULL
)

SELECT DISTINCT *
FROM teams
ORDER BY league, team
