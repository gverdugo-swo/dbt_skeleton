WITH all_sales AS (
    SELECT
        t.sales,
        t.order_date,
        t.product_id,
        t.product_name
    FROM
        {{ ref('silver__sales') }} as t
),
monthly_product_sales AS (
    SELECT
        DATE_TRUNC(
            order_date,
            MONTH
        ) AS `month`,
        product_id,
        product_name,
        SUM(sales) AS sales
    FROM
        all_sales
    GROUP BY
        ALL
),
FINAL AS (
    SELECT
        *
    FROM
        monthly_product_sales
)
SELECT
    *
FROM
    FINAL
