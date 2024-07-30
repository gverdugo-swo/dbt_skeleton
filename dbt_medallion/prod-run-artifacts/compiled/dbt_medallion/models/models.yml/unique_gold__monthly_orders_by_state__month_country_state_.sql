
    
    

with dbt_test__target as (

  select (month || '-' || country || '-' || state) as unique_field
  from `ip-trabajo-gverdugo`.`gold`.`monthly_orders_by_state`
  where (month || '-' || country || '-' || state) is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


