SELECT
    odd.id,
    odd.sport_key,
    odd.sport_title,
    odd.commence_time AS start_at,
    odd.home_team,
    odd.away_team,
    odd.bookmaker_key,
    odd.bookmaker_title,
    odd.bookmaker_last_update AS updated_at,
    odd.market_key,
    odd.price_home_team,
    odd.price_draw,
    odd.price_away_team,
    odd.loaded_at,
FROM
    {{ source ('theoddsapi', 'odds') }} AS odd
