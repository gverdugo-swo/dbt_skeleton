
    
    

with dbt_test__target as (

  select row_id as unique_field
  from `ip-trabajo-gverdugo`.`silver`.`sales`
  where row_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


