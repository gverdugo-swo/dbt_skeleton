{% macro get_fields_query(table_name) %}

    {% if execute %}

        {% set fields = get_field_name(table_name) %}
        {% set fields_transformation = get_fields_tranformations(fields) %}

        {% set query %}
            select 
                {% for field in fields_transformation %}
                    {{ field }},
                {% endfor %}
            from cleaning_view
        {% endset %}

        {{ return(query) }}

    {% endif %}

{% endmacro %}

{% macro get_fields_tranformations(fields) %}

    {% set fields_transformation = [] %}

    {% for field in fields %}
        {{ fields_transformation.append(standard_field_transformation(field)) }}
    {% endfor %}

    {{ return(fields_transformation) }}

{% endmacro %}

{% macro standard_field_transformation(field) %}

    {% set to_field = field.name if field.name is not none else field.field %}
    
    {% if field.type == "DATE" or field.type == "DATETIME" or field.type == "TIMESTAMP" or field.type == "TIME" %}
        {% set conversion_func = get_date_field_conversion_func(field) %}
        {{ return(conversion_func ~ "('"~ field.format ~"', " ~ '`' ~ field.field ~ '`' ~ ") as `" ~ to_field ~ "`" ) }}
    {% elif field.type == None %}
        {{ return("SAFE_CAST(" ~ '`' ~ field.field ~ '`' ~ " as STRING) as `" ~ to_field ~ "`") }}
    {% else %}
        {{ return("SAFE_CAST(" ~ '`' ~ field.field ~ '`' ~ " as " ~ field.type ~ ") as `" ~ to_field ~ "`") }}
    {% endif %}

{% endmacro %}

{% macro get_date_field_conversion_func(field) %}

    {% if field.type == "DATE" %}
        {{ return("SAFE.PARSE_DATE") }}
    {% elif field.type == "DATETIME" %}
        {{ return("SAFE.PARSE_DATETIME") }}    
    {% elif field.type == "TIMESTAMP" %}
        {{ return("SAFE.PARSE_TIMESTAMP") }}   
    {% elif field.type == "TIME" %}
        {{ return("SAFE.PARSE_TIME") }}
    {% endif %}

{% endmacro %}
