{% macro get_table_info(table_name) %}

    {% if execute %}

    {% set query %}

        select * from {{ var("TABLE_INFO")}} WHERE `table` = '{{ table_name }}'

    {% endset %}

    {%set results = "" %}
    
        {% set results = run_query(query) %}

    {{return(results)}}
    {% endif %}

{% endmacro %}

{% macro get_field_name(table_name) %}

    {% if execute %}

    {% set query %}

        select * from {{ var("FIELD_NAME")}} WHERE `table` = '{{ table_name }}'

    {% endset %}
    {%set results = "" %}
    
        {% set results = run_query(query) %}
    {{return(results)}}
    {% endif %}

{% endmacro %}