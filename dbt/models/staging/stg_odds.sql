SELECT
    odd.id,
    odd.sport_key,
    league.tipster AS league,
    concat(emoji.unicode, ' ',league.tipster) AS league_emoji,
    odd.commence_time AS start_at,
    home.tipster AS home,
    away.tipster AS away,
    odd.bookmaker_key,
    odd.bookmaker_title,
    odd.bookmaker_last_update AS updated_at,
    odd.market_key,
    odd.price_home_team AS price_home,
    odd.price_draw,
    odd.price_away_team AS price_away,
    odd.loaded_at,
FROM
    {{ source ('theoddsapi', 'odds') }} AS odd
    LEFT JOIN {{ ref("league_name") }} AS league ON odd.sport_key = league.theoddsapi
    LEFT JOIN {{ ref("emoji") }} AS emoji ON league.country = emoji.country
    LEFT JOIN {{ ref("club_name") }} AS home ON odd.home_team = home.theoddsapi
    LEFT JOIN {{ ref("club_name") }} AS away ON odd.away_team = away.theoddsapi
