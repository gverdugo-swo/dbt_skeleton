{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if node is not none and node.name is not none -%}
        {%- set name_parts = node.name.split('__', 1) -%}
        {{ name_parts[1] if name_parts | length > 1 else node.name }}
    {%- elif custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node.version -%}
        {{ node.name ~ "_v" ~ (node.version | replace(".", "_")) }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}

{%- endmacro %}

