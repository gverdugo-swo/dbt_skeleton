WITH _data AS (
    SELECT
        *
    FROM
        {{ ref('silver__sales') }}
)
select * from _data
