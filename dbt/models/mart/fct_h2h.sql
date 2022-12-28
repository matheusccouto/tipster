WITH odds AS (
    SELECT *
    FROM
        {{ ref("stg_odds") }}
    WHERE
        start_at > current_timestamp()
        AND market_key = 'h2h'
    QUALIFY
        row_number() OVER (
            PARTITION BY id, bookmaker_key, market_key ORDER BY loaded_at DESC
        ) = 1
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
        spi.prob_home * (odds.price_home - 1) - (1 - spi.prob_home) AS ev_home,
        spi.prob_draw * (odds.price_draw - 1) - (1 - spi.prob_draw) AS ev_draw,
        spi.prob_away * (odds.price_away - 1) - (1 - spi.prob_away) AS ev_away
    FROM
        odds
    INNER JOIN
        {{ ref("stg_spi") }} AS spi
        ON date(odds.start_at, 'America/Los_Angeles') = spi.date
            AND odds.league_id = spi.league_id
            AND odds.home = spi.home
            AND odds.away = spi.away
    LEFT JOIN
        `tipster-main`.tipster.flag AS flag ON
            odds.league_country = flag.country
),

bets AS (
    SELECT
        id,
        concat(
            flag_emoji,
            ' ',
            league_name,
            ' ',
            date(start_at),
            '\\n',
            '_*',
            home,
            '*_ x ',
            away,
            '\\n[',
            bookmaker_name,
            '](',
            bookmaker_url,
            ') ',
            price_home
        ) AS message,  -- noqa: L029
        league_id,
        league_name,
        league_country,
        start_at,
        home AS club,
        bookmaker_key,
        bookmaker_name,
        bookmaker_url,
        updated_at,
        'home' AS bet,
        price_home AS price,
        prob_home AS prob,
        ev_home AS ev,
        quality,
        importance,
        rating,
        loaded_at
    FROM
        ev

    UNION ALL

    SELECT
        id,
        concat(
            flag_emoji,
            ' ',
            league_name,
            ' ',
            date(start_at),
            '\\n',
            home,
            ' _*x*_ ',
            away,
            '\\n[',
            bookmaker_name,
            '](',
            bookmaker_url,
            ') ',
            price_draw
        ) AS message,  -- noqa: L029
        league_id,
        league_name,
        league_country,
        start_at,
        NULL AS club,
        bookmaker_key,
        bookmaker_name,
        bookmaker_url,
        updated_at,
        'draw' AS bet,
        price_draw AS price,
        prob_draw AS prob,
        ev_draw AS ev,
        quality,
        importance,
        rating,
        loaded_at
    FROM
        ev

    UNION ALL

    SELECT
        id,
        concat(
            flag_emoji,
            ' ',
            league_name,
            ' ',
            date(start_at),
            '\\n',
            home,
            ' x _*',
            away,
            '*_\\n[',
            bookmaker_name,
            '](',
            bookmaker_url,
            ') ',
            price_away
        ) AS message,  -- noqa: L029
        league_id,
        league_name,
        league_country,
        start_at,
        away AS club,
        bookmaker_key,
        bookmaker_name,
        bookmaker_url,
        updated_at,
        'away' AS bet,
        price_away AS price,
        prob_away AS prob,
        ev_away AS ev,
        quality,
        importance,
        rating,
        loaded_at
    FROM
        ev
)

SELECT
    bets.id,
    bets.message,
    bets.start_at,
    bets.club,
    bets.league_id,
    bets.league_name,
    bets.bookmaker_key,
    bets.updated_at,
    bets.bet,
    bets.price,
    bets.prob,
    bets.ev,
    bets.loaded_at,
    timestamp_diff(bets.start_at, bets.updated_at, HOUR) AS hours_left
FROM
    bets
