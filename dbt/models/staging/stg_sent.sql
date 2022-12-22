SELECT DISTINCT
    user,
    id,
    ev,
    sent_at
FROM
    {{ source("tipster", "sent") }}