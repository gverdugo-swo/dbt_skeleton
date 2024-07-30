
    
    

with dbt_test__target as (

  select (month || '-' || product_id) as unique_field
  from `ip-trabajo-gverdugo`.`gold`.`monthly_product_sales`
  where (month || '-' || product_id) is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


