SELECT
    odd.id,
    odd.start_at,
    odd.league,
    spi.league_emoji,
    odd.home,
    odd.away,
    odd.bookmaker_key,
    odd.bookmaker_title AS bookmaker_name,
    odd.updated_at,
    odd.market_key,
    odd.price_home,
    odd.price_draw,
    odd.price_away,
    spi.prob_home,
    spi.prob_draw,
    spi.prob_away,
    spi.prob_home * (odd.price_home - 1) - (1 - spi.prob_home) AS ev_home,
    spi.prob_draw * (odd.price_draw - 1) - (1 - spi.prob_draw) AS ev_draw,
    spi.prob_away * (odd.price_away - 1) - (1 - spi.prob_away) AS ev_away
FROM
    {{ ref("stg_odds") }} AS odd
LEFT JOIN
    {{ ref("stg_spi") }} AS spi 
        ON date(odd.start_at) = spi.date 
        AND odd.home = spi.home 
        AND odd.away = spi.away
WHERE
    odd.market_key = 'h2h'
QUALIFY ROW_NUMBER() OVER (PARTITION BY odd.id, odd.bookmaker_key, odd.market_key ORDER BY odd.loaded_at DESC) = 1
