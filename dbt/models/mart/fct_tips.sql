WITH tips AS (
    SELECT
        bets.*,
        ubk.user
    FROM
        {{ ref("fct_h2h") }} AS bets
    INNER JOIN
        {{ ref("user_bookmaker") }} AS ubk ON bets.bookmaker_key = ubk.bookmaker
    LEFT JOIN
        {{ ref("user_ev") }} AS uev ON ubk.user = uev.user
    WHERE
        bets.ev >= uev.ev
    QUALIFY
        row_number() OVER (
            PARTITION BY bets.id, ubk.user ORDER BY bets.ev DESC
        ) = 1
)

SELECT tips.*
FROM
    tips
LEFT JOIN {{ ref("stg_sent") }} AS s ON tips.user = s.user AND tips.id = s.id
WHERE
    tips.ev > coalesce(s.ev, 0)
