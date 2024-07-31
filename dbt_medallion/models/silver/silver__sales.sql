{{
config(
    tags=['sales','full-materialization','auto-generated']
)
}}


with
    cleaning_view as ({{ get_cleaning_query("sales") }}),
    fields_view as ({{ get_fields_query("sales") }}),
    final as (select * from fields_view)
select *
from final
