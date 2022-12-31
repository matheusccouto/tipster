WITH tips AS (
    SELECT
        replace(bets.message, '{kelly}', CAST(ROUND(100 * uk.fraction * bets.kelly, 1) AS STRING)),
        bets.* except (message),
        ub.user
    FROM
        {{ ref("fct_h2h") }} AS bets
    INNER JOIN
        {{ ref("stg_user_bookmaker") }} AS ub ON bets.bookmaker_key = ub.name
    INNER JOIN
        {{ ref("stg_user_league") }} AS ul ON bets.league_id = ul.key
    LEFT JOIN
        {{ ref("stg_user_ev") }} AS ue ON ub.user = ue.user
    LEFT JOIN
        {{ ref("stg_user_kelly") }} AS uk ON ub.user = uk.user
    WHERE
        bets.ev >= ue.ev
    QUALIFY
        row_number() OVER (PARTITION BY bets.id, ub.user ORDER BY bets.ev DESC) = 1
)

SELECT
    tips.*
FROM
    tips
LEFT JOIN
    {{ ref("stg_sent") }} AS s ON tips.user = s.user AND tips.id = s.id
WHERE
    tips.ev > coalesce(s.ev, 0)
