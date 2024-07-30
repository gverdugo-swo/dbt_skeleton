with
    all_sales as (
        select t.sales, t.order_date, t.product_id, from {{ ref("silver__sales") }} as t
    ),
    monthly_product_sales as (
        select date_trunc(order_date, month) as `month`, product_id, sum(sales) as sales
        from all_sales
        group by all
    ),
    final as (select * from monthly_product_sales)
select *
from final
