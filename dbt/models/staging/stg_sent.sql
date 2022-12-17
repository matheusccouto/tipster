SELECT DISTINCT
    user,
    id
FROM
    {{ source("tipster", "sent") }}