{#- Use a model's custom schema (+schema) as is, instead of dbt's default <target_schema>_<custom_schema>.
    Models without a custom schema keep the target schema (public, or analytics with --target analytics). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
