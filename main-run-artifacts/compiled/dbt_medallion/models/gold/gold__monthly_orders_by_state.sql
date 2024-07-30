WITH orders AS (
    SELECT
        order_id,
        order_date,
        country,
        `state`,
        city
    FROM
        `ip-trabajo-gverdugo`.`silver`.`sales`
),
monthly_orders_by_country AS (
    SELECT
        DATE_TRUNC(
            order_date,
            MONTH
        ) AS `month`,
        country,
        `state`,
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
        monthly_orders_by_country
)
SELECT
    *
FROM
    FINAL