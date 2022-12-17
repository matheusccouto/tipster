SELECT
    odd.id,
    odd.start_at,
    odd.sport_key AS league_key,
    spi.league AS league_name,
    odd.home,
    odd.away,
    odd.bookmaker_key,
    odd.bookmaker_title AS bookmaker_name,
    odd.updated_at,
    odd.market_key,
    odd.price_home_team,
    odd.price_draw,
    odd.price_away_team,
    spi.prob_home,
    spi.prob_draw,
    spi.prob_away
FROM
    {{ ref("stg_odds") }} AS odd
LEFT JOIN
    {{ ref("stg_spi") }} AS spi 
        ON date(odd.start_at) = spi.date 
        AND odd.home = spi.home 
        AND odd.away = spi.away
QUALIFY ROW_NUMBER() OVER (PARTITION BY odd.id, odd.bookmaker_key, odd.market_key, odd.updated_at ORDER BY odd.loaded_at DESC) = 1
