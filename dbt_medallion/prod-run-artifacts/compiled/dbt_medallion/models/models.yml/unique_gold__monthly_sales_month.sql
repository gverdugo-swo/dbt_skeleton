
    
    

with dbt_test__target as (

  select month as unique_field
  from `ip-trabajo-gverdugo`.`gold`.`monthly_sales`
  where month is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


