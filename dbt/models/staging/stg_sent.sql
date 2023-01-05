SELECT
    user,
    id,
    message,  -- noqa: L029
    sent_at
FROM
    {{ source("tipster", "sent") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user, id ORDER BY sent_at DESC) = 1
