with
    all_sales as (select t.sales, t.order_date from `ip-trabajo-gverdugo`.`silver`.`sales` as t),
    monthly_sales as (
        select date_trunc(order_date, month) as `month`, sum(sales) as sales
        from all_sales
        group by all
    ),
    final as (select * from monthly_sales)
select *
from final