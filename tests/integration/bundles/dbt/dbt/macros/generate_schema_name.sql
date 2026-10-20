{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ var("target_schema", target.schema) }}
{%- endmacro %}
