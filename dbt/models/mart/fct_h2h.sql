WITH odds AS (
    SELECT *
    FROM
        {{ ref("stg_odds") }} AS odds
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
        spi.prob_home * (odds.price_home - 1) - (1 - spi.prob_home) AS ev_home,
        spi.prob_draw * (odds.price_draw - 1) - (1 - spi.prob_draw) AS ev_draw,
        spi.prob_away * (odds.price_away - 1) - (1 - spi.prob_away) AS ev_away
    FROM
        odds
    INNER JOIN
        {{ ref("stg_spi") }} AS spi
        ON date(odds.start_at) = spi.date
            AND odds.league_id = spi.league_id
            AND odds.home = spi.home
            AND odds.away = spi.away
),

bets AS (
    SELECT
        *,
        greatest(ev_home, ev_draw, ev_away) AS ev
    FROM
        ev
)

SELECT
    bets.id,
    bets.start_at,
    bets.league_id,
    bets.league_name,
    flag.emoji AS flag_emoji,
    bets.home,
    bets.away,
    bets.bookmaker_key,
    bets.bookmaker_name,
    bets.bookmaker_url,
    bets.updated_at,
    bets.market_key,
    ubk.user,
    bets.ev,
    CASE
        WHEN bets.ev_home = bets.ev THEN 'home'
        WHEN bets.ev_draw = bets.ev THEN 'draw'
        WHEN bets.ev_away = bets.ev THEN 'away'
    END AS bet,
    CASE
        WHEN bets.ev_home = bets.ev THEN bets.price_home
        WHEN bets.ev_draw = bets.ev THEN bets.price_draw
        WHEN bets.ev_away = bets.ev THEN bets.price_away
    END AS price
FROM
    bets
INNER JOIN
    {{ ref("user_bookmaker") }} AS ubk ON bets.bookmaker_key = ubk.bookmaker
LEFT JOIN {{ ref("user_ev") }} AS uev ON ubk.user = uev.user
LEFT JOIN {{ ref("flag") }} AS flag ON bets.league_country = flag.country
WHERE
    bets.ev >= uev.ev
QUALIFY row_number() OVER (PARTITION BY bets.id, ubk.user ORDER BY bets.ev DESC) = 1
