with
    orders as (
        select postal_code, count(*) as orders
        from {{ ref("silver__sales") }}
        group by all
    ),
    final as (select * from orders)
select *
from final
