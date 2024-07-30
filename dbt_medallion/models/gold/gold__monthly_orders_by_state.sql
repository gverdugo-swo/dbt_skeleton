with
    orders as (
        select order_id, order_date, country, `state`, city
        from {{ ref("silver__sales") }}
    ),
    monthly_orders_by_country as (
        select
            date_trunc(order_date, month) as `month`,
            country,
            `state`,
            count(order_id) as orders
        from orders
        group by all
    ),
    final as (select * from monthly_orders_by_country)
select *
from final
