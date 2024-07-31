WITH orders AS (
    SELECT
        order_date AS `day`,
        postal_code,
        COUNT(*) AS orders
    FROM
        {{ ref("silver__sales") }}
    GROUP BY
        ALL
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
