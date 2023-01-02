{% macro message_h2h() %}

CREATE OR REPLACE FUNCTION {{ target.dataset }}.message_h2h(
    flag STRING,
    league STRING,
    date DATE,
    home STRING,
    away STRING,
    bet STRING,
    bookmaker STRING,
    url STRING,
    price FLOAT64
) RETURNS STRING AS (
    concat(
        flag, ' ', league, ' ', date, '\\n',
        home, ' x ', away, '\\n',
        'Bet {kelly}% on ', bet, ' on [', bookmaker, '](', url, ') at ', price
    )
);

{% endmacro %}