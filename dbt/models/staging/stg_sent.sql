SELECT
    user,
    id,
    ev,
    sent_at
FROM
    {{ source("tipster", "sent") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user, id ORDER BY sent_at DESC) = 1
