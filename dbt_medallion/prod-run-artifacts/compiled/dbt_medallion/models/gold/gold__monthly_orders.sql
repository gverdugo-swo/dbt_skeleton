WITH orders AS (
    SELECT
        order_id,
        order_date
    FROM
        `ip-trabajo-gverdugo`.`silver`.`sales`
),
monthly_orders AS (
    SELECT
        DATE_TRUNC(
            order_date,
            month
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