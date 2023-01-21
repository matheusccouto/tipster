WITH odds AS (
    SELECT *
    FROM
        {{ ref("stg_odds" ) }}
    WHERE
        market_key = 'h2h'
        AND NOT (sport = 'icehockey' AND price_draw IS NOT NULL)
),

preds AS (
    SELECT
        date,  -- noqa: L029
        league_id,
        home,
        away,
        prob_home,
        prob_draw,
        prob_away,
        importance,
        quality,
        rating,
        score_home,
        score_away
    FROM
        {{ ref("stg_spi" ) }}
    
    UNION ALL

    SELECT
        date,  -- noqa: L029
        league_id,
        home,
        away,
        prob_home,
        NULL AS prob_draw,
        prob_away,
        importance,
        quality,
        rating,
        score_home,
        score_away
    FROM
        {{ ref("stg_nfl" ) }}
    
    UNION ALL

    SELECT
        date,  -- noqa: L029
        league_id,
        home,
        away,
        prob_home,
        NULL AS prob_draw,
        prob_away,
        importance,
        quality,
        rating,
        score_home,
        score_away
    FROM
        {{ ref("stg_nba" ) }}
    
    UNION ALL

    SELECT
        date,  -- noqa: L029
        league_id,
        home,
        away,
        prob_home,
        NULL AS prob_draw,
        prob_away,
        NULL AS importance,
        NULL AS quality,
        NULL AS rating,
        score_home,
        score_away
    FROM
        {{ ref("stg_wnba" ) }}
    
    UNION ALL

    SELECT
        date,  -- noqa: L029
        league_id,
        home,
        away,
        prob_home,
        NULL AS prob_draw,
        prob_away,
        NULL AS importance,
        NULL AS quality,
        NULL AS rating,
        score_home,
        score_away
    FROM
        {{ ref("stg_mlb" ) }}
    
    UNION ALL

    SELECT
        date,  -- noqa: L029
        league_id,
        home,
        away,
        prob_home,
        NULL AS prob_draw,
        prob_away,
        NULL AS importance,
        NULL AS quality,
        NULL AS rating,
        score_home,
        score_away
    FROM
        {{ ref("stg_nhl" ) }}
),

ev AS (
    SELECT
        odds.*,
        flag.emoji AS flag_emoji,
        sport.emoji AS sport_emoji,
        preds.prob_home,
        preds.prob_draw,
        preds.prob_away,
        preds.importance,
        preds.quality,
        preds.rating,
        preds.score_home,
        preds.score_away,
        preds.prob_home * (odds.price_home - 1) - (1 - preds.prob_home) AS ev_home,
        preds.prob_draw * (odds.price_draw - 1) - (1 - preds.prob_draw) AS ev_draw,
        preds.prob_away * (odds.price_away - 1) - (1 - preds.prob_away) AS ev_away
    FROM
        odds
    INNER JOIN
        preds
        ON date(odds.start_at, 'America/Los_Angeles') = preds.date
            AND odds.league_id = preds.league_id
            AND odds.home = preds.home
            AND odds.away = preds.away
    LEFT JOIN {{ ref("flag" ) }} AS flag ON odds.league_country = flag.country
    LEFT JOIN {{ ref("sport") }} AS sport ON odds.sport = sport.key

),

bets AS (
    SELECT
        id,
        tipster.message_h2h(
            sport_emoji,
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
            sport_emoji,
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
            sport_emoji,
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
    (
        ((price - 1) * prob) - (1 - prob)
    ) / (if(price < 1.01, 1.01, price) - 1) AS kelly
FROM
    bets
