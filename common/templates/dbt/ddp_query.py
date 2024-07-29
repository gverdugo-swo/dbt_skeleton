DDP_QUERY = """
with cleaning_view as (
    {{{{ get_cleaning_query("{source_table}") }}}}
), fields_view as (
    {{{{ get_fields_query("{source_table}") }}}}
), final as(
    select * from fields_view
)
select * from final
"""