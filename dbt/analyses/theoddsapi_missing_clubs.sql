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
    LEFT JOIN {{ ref("club") }} AS c ON o.home_team = c.old
    WHERE c.new IS NULL

    UNION ALL

    SELECT
        o.sport_key,
        o.away_team AS team
    FROM odds AS o
    LEFT JOIN {{ ref("club") }} AS c ON o.away_team = c.old
    WHERE c.new IS NULL
)

SELECT DISTINCT *
FROM teams
ORDER BY sport_key, team
