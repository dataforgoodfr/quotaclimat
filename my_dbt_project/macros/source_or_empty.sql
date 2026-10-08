{#- Selects `columns` from a source table, or an empty result with the same text columns when the
    table does not exist (e.g. reference data not loaded in this database), so that the model still builds.
    A column missing from an existing table (e.g. an optional column not added to its Google Sheet tab yet)
    is selected as NULL text. -#}
{% macro source_or_empty(source_name, table_name, columns) %}
  {%- set relation = load_relation(source(source_name, table_name)) if execute else none -%}
  {%- if relation is not none or not execute %}
  {%- set existing = adapter.get_columns_in_relation(relation) | map(attribute='name') | list if relation is not none else columns -%}
  SELECT {% for c in columns %}{% if c in existing %}{{ c }}{% else %}NULL::text AS {{ c }}{% endif %}{{ ", " if not loop.last }}{% endfor %}
  FROM {{ source(source_name, table_name) }}
  {%- for c in columns if c not in existing %}
  {{- log(model.name ~ ": " ~ source(source_name, table_name) ~ " has no column " ~ c ~ ", using NULL", info=True) }}
  {%- endfor %}
  {%- else %}
  {{- log(model.name ~ ": " ~ source(source_name, table_name) ~ " not found, using an empty table", info=True) }}
  SELECT {% for c in columns %}NULL::text AS {{ c }}{{ ", " if not loop.last }}{% endfor %}
  WHERE FALSE
  {%- endif %}
{% endmacro %}
