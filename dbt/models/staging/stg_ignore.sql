WITH ig AS (
    SELECT *
    FROM
        {{ source("tipster", "ignore") }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY user, message ORDER BY updated_at DESC
        ) = 1
)

SELECT
    ig.user,
    ig.message
FROM ig
WHERE ig.delete IS FALSE
