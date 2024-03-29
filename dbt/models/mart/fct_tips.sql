WITH tips AS (
    SELECT
        bets.* EXCEPT (message),
        ub.user,
        round(ur.bankroll * uk.fraction * bets.kelly, 2) AS amount,
        replace(
            bets.message,
            '{kelly}',
            cast(round(ur.bankroll * uk.fraction * bets.kelly, 2) AS STRING)
        ) AS message  -- noqa: L029
    FROM
        {{ ref("fct_h2h") }} AS bets
    INNER JOIN
        {{ ref("stg_user_bookmaker") }} AS ub ON bets.bookmaker_key = ub.key
    INNER JOIN
        {{ ref("stg_user_league") }} AS ul ON
            ub.user = ul.user AND bets.league_id = ul.key
    LEFT JOIN
        {{ ref("stg_user_ev") }} AS ue ON ub.user = ue.user
    LEFT JOIN
        {{ ref("stg_user_kelly") }} AS uk ON ub.user = uk.user
    LEFT JOIN
        {{ ref("stg_user_bankroll") }} AS ur ON ub.user = ur.user
    WHERE
        current_timestamp() < bets.start_at
        AND bets.ev >= ue.ev
    QUALIFY
        row_number() OVER (
            PARTITION BY bets.id, ub.user ORDER BY bets.ev DESC
        ) = 1
)

SELECT tips.*
FROM
    tips
LEFT JOIN
    {{ ref("stg_sent") }} AS s ON tips.user = s.user AND tips.id = s.id
LEFT JOIN
    {{ ref("stg_bet") }} AS b ON s.user = b.user AND s.message = b.message
LEFT JOIN
    {{ ref("stg_ignore") }} AS i ON s.user = i.user AND s.message = i.message
WHERE
    b.message IS NULL
    AND i.message IS NULL
    AND REGEXP_REPLACE(tips.message, r'on \[(.*?)\]\((.*?)\) at', 'on \\1 at') != COALESCE(s.message, '')
