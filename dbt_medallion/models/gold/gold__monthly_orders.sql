WITH orders AS (
    SELECT
        order_id,
        order_date
    FROM
        {{ ref("silver__sales") }}
),
monthly_orders AS (
    SELECT
        DATE_TRUNC(
            order_date,
            MONTH
        ) AS `month`,
        COUNT(order_id) AS orders
    FROM
        orders
    GROUP BY
        ALL
),
FINAL AS (
    SELECT
        *
    FROM
        monthly_orders
)
SELECT
    *
FROM
    FINAL
