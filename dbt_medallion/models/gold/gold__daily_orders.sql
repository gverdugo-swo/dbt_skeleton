with
    orders as (
        select order_date as `day`, count(*) as orders
        from {{ ref("silver__sales") }}
        group by all
    ),
    final as (select * from orders)
select *
from final
