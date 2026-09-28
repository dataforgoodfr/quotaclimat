{#- Selects `columns` from a source table, or an empty result with the same text columns when the
    table does not exist (e.g. reference data not loaded in this database), so that the model still builds. -#}
{% macro source_or_empty(source_name, table_name, columns) %}
  {%- set relation = load_relation(source(source_name, table_name)) if execute else none -%}
  {%- if relation is not none or not execute %}
  SELECT {{ columns | join(', ') }}
  FROM {{ source(source_name, table_name) }}
  {%- else %}
  {{- log(model.name ~ ": " ~ source(source_name, table_name) ~ " not found, using an empty table", info=True) }}
  SELECT {% for c in columns %}NULL::text AS {{ c }}{{ ", " if not loop.last }}{% endfor %}
  WHERE FALSE
  {%- endif %}
{% endmacro %}
