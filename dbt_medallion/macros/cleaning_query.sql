{% macro get_cleaning_query(table_name) %}

    {% set table_info = get_table_info(table_name)%}
    {% set materialization = table_info[0].materialization %}

    {% set keys = table_info[0].materialization_keys %}

    {% set cleaning_query = "" %}

    {% if materialization == "pk"%}

        {% set cleaning_query = get_pk_consolidation_query(table_name,keys) %}
    {% elif materialization == "newest" %}

        {% set cleaning_query = get_newest_consolidation_query(table_name,keys) %}
    {% elif materialization == "full" %}

        {% set cleaning_query = get_full_consolidation_query(table_name,keys) %}

    {% endif %}
    {{return(cleaning_query)}}

{% endmacro %}

{% macro get_pk_consolidation_query(table_name, keys) %}

    {% set query %}

        SELECT
            *
            FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {{ keys }} ORDER BY inserted_at DESC) AS rn
            FROM
                {{ source('bronze',table_name) }} )
            WHERE 
            rn = 1

    {% endset %}

    {{return(query)}}

{% endmacro %}

{% macro get_newest_consolidation_query(table_name, keys) %}

    {% set query %}

        SELECT *
        FROM {{ source(table_name) }}
        WHERE inserted_at = (
                SELECT max(loading_date)
                FROM {{ source('bronze',table_name) }}
                )

    {% endset %}

    {{return(query)}}

{% endmacro %}

{% macro get_full_consolidation_query(table_name, keys) %}

    {% set query %}

        SELECT *
        FROM {{ source('bronze',table_name) }}

    {% endset %}

    {{return(query)}}

{% endmacro %}