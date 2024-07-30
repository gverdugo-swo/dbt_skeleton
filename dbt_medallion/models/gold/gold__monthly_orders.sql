with
    orders as (select order_id, order_date from {{ ref("silver__sales") }}),
    monthly_orders as (
        select date_trunc(order_date, month) as `month`, count(order_id) as orders
        from orders
        group by all
    ),
    final as (select * from monthly_orders)
select *
from final
