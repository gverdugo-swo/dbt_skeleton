WITH all_sales AS (
    SELECT
        t.sales,
        t.order_date
    FROM
        {{ ref("silver__sales") }} AS t
),
monthly_sales AS (
    SELECT
        DATE_TRUNC(
            order_date,
            MONTH
        ) AS `month`,
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
        monthly_sales
)
SELECT
    *
FROM
    FINAL
