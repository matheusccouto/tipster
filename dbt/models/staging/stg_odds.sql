WITH teams AS (
    SELECT
        t.new,
        t.old
    FROM
        {{ ref("club")}} AS t
    
    UNION ALL

    SELECT
        t.new,
        t.old
    FROM
        {{ ref("mlb")}} AS t
    
    UNION ALL

    SELECT
        t.new,
        t.old
    FROM
        {{ ref("nba")}} AS t
    
    UNION ALL

    SELECT
        t.new,
        t.old
    FROM
        {{ ref("nfl")}} AS t
    
    UNION ALL

    SELECT
        t.new,
        t.old
    FROM
        {{ ref("nhl")}} AS t
    
    UNION ALL

    SELECT
        t.new,
        t.old
    FROM
        {{ ref("wnba")}} AS t
)
SELECT
    odd.id,
    league.sport,
    league.id AS league_id,
    league.tipster AS league_name,
    league.country AS league_country,
    odd.commence_time AS start_at,
    home.new AS home,
    away.new AS away,
    odd.bookmaker_key,
    book.name AS bookmaker_name,
    book.url AS bookmaker_url,
    odd.bookmaker_last_update AS updated_at,
    odd.market_key,
    odd.price_home_team AS price_home,
    odd.price_draw,
    odd.price_away_team AS price_away,
    odd.loaded_at
FROM
    {{ source ('theoddsapi', 'odds') }} AS odd
LEFT JOIN {{ ref("league") }} AS league ON odd.sport_key = league.theoddsapi
LEFT JOIN {{ ref("bookmaker") }} AS book ON odd.bookmaker_key = book.key
LEFT JOIN teams AS home ON odd.home_team = home.old
LEFT JOIN teams AS away ON odd.away_team = away.old
