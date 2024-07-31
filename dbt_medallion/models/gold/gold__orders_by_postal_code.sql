WITH orders AS (
    SELECT
        postal_code,
        COUNT(*) as orders
    FROM
        {{ ref("silver__sales") }}
        group by all
),
FINAL AS (
    SELECT
        *
    FROM
        orders
)
SELECT
    *
FROM
    FINAL
