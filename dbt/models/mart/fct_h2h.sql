WITH odds AS (
    SELECT *
    FROM
        {{ ref("stg_odds" ) }}
    WHERE
        market_key = 'h2h'
),

ev AS (
    SELECT
        odds.*,
        flag.emoji AS flag_emoji,
        spi.prob_home,
        spi.prob_draw,
        spi.prob_away,
        spi.importance,
        spi.quality,
        spi.rating,
        spi.score_home,
        spi.score_away,
        spi.prob_home * (odds.price_home - 1) - (1 - spi.prob_home) AS ev_home,
        spi.prob_draw * (odds.price_draw - 1) - (1 - spi.prob_draw) AS ev_draw,
        spi.prob_away * (odds.price_away - 1) - (1 - spi.prob_away) AS ev_away
    FROM
        odds
    INNER JOIN
        {{ ref("stg_spi" ) }} AS spi
        ON date(odds.start_at, 'America/Los_Angeles') = spi.date
            AND odds.league_id = spi.league_id
            AND odds.home = spi.home
            AND odds.away = spi.away
    LEFT JOIN
        {{ ref("flag" ) }} AS flag ON
            odds.league_country = flag.country
),

bets AS (
    SELECT
        id,
        tipster.message_h2h(
            flag_emoji,
            league_name,
            date(start_at),
            home,
            away,
            home,
            bookmaker_name,
            bookmaker_url,
            price_home
        ) AS message,  -- noqa: L029
        league_id,
        start_at,
        home AS club,
        bookmaker_key,
        updated_at,
        'home' AS bet,
        price_home AS price,
        prob_home AS prob,
        ev_home AS ev,
        quality,
        importance,
        rating,
        score_home > score_away AS outcome,
        loaded_at
    FROM
        ev

    UNION ALL

    SELECT
        id,
        tipster.message_h2h(
            flag_emoji,
            league_name,
            date(start_at),
            home,
            away,
            'Draw',
            bookmaker_name,
            bookmaker_url,
            price_draw
        ) AS message,  -- noqa: L029
        league_id,
        start_at,
        NULL AS club,
        bookmaker_key,
        updated_at,
        'draw' AS bet,
        price_draw AS price,
        prob_draw AS prob,
        ev_draw AS ev,
        quality,
        importance,
        rating,
        score_home = score_away AS outcome,
        loaded_at
    FROM
        ev

    UNION ALL

    SELECT
        id,
        tipster.message_h2h(
            flag_emoji,
            league_name,
            date(start_at),
            home,
            away,
            away,
            bookmaker_name,
            bookmaker_url,
            price_away
        ) AS message,  -- noqa: L029
        league_id,
        start_at,
        away AS club,
        bookmaker_key,
        updated_at,
        'away' AS bet,
        price_away AS price,
        prob_away AS prob,
        ev_away AS ev,
        quality,
        importance,
        rating,
        score_home < score_away AS outcome,
        loaded_at
    FROM
        ev
)

SELECT
    *,
    timestamp_diff(start_at, loaded_at, HOUR) AS hours_left,
    count(DISTINCT bookmaker_key) OVER (PARTITION BY id, bet) AS market_count,
    min(price) OVER (PARTITION BY id, bet) AS market_price_min,
    percentile_cont(
        price, 0.5
    ) OVER (PARTITION BY id, bet) AS market_price_median,
    max(price) OVER (PARTITION BY id, bet) AS market_price_max,
    (((price - 1) * prob) - (1 - prob)) / (if(price < 1.01, 1.01, price) - 1) AS kelly
FROM
    bets
