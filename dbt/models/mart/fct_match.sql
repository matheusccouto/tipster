WITH future_odds AS (
    SELECT
        *
    FROM
        {{ ref("stg_odds") }}
    WHERE
        start_at > current_timestamp()
        AND market_key = 'h2h'
)
SELECT
    odd.id,
    odd.start_at,
    odd.league_id,
    odd.league_name,
    odd.league_country,
    odd.home,
    odd.away,
    odd.bookmaker_key,
    odd.bookmaker_name,
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
    future_odds AS odd
INNER JOIN
    {{ ref("stg_spi") }} AS spi 
        ON date(odd.start_at) = spi.date
        AND odd.league_id = spi.league_id
        AND odd.home = spi.home 
        AND odd.away = spi.away
QUALIFY ROW_NUMBER() OVER (PARTITION BY odd.id, odd.bookmaker_key, odd.market_key ORDER BY odd.loaded_at DESC) = 1
