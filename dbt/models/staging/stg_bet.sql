WITH bet AS (
    SELECT *
    FROM
        {{ source("tipster", "bet") }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY user, message ORDER BY updated_at DESC
        ) = 1
)

SELECT
    bet.user,
    bet.message
FROM bet
WHERE bet.delete IS FALSE
