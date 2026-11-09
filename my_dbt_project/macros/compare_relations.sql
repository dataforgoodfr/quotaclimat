{#- One row comparing two relations with the same columns (double run of models/daily): their numbers of rows and
    the rows found only in one of them (EXCEPT ALL, duplicates counted). The columns are those of `old`; numbers are
    rounded to 6 decimals (a float sum changes in its last digits with the order of the rows), json is compared as
    text (json has no equality operator). -#}
{% macro compare_relations(old, new, old_name, new_name) %}
  {%- set columns = [] -%}
  {%- if execute -%}
    {%- for column in adapter.get_columns_in_relation(old) -%}
      {%- set quoted = adapter.quote(column.name) -%}
      {%- if column.is_float() or column.is_numeric() -%}
        {%- do columns.append('round(CAST(' ~ quoted ~ ' AS numeric), 6)') -%}
      {%- elif column.dtype in ('json', 'jsonb') -%}
        {%- do columns.append('CAST(' ~ quoted ~ ' AS text)') -%}
      {%- else -%}
        {%- do columns.append(quoted) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- else -%}
    {%- do columns.append('1') -%}
  {%- endif -%}
  {%- set select_list = columns | join(', ') -%}
SELECT
  CAST('{{ old_name }}' AS text) AS old_table,
  CAST('{{ new_name }}' AS text) AS new_table,
  (SELECT count(*) FROM {{ old }}) AS old_rows,
  (SELECT count(*) FROM {{ new }}) AS new_rows,
  (SELECT count(*) FROM (SELECT {{ select_list }} FROM {{ old }} EXCEPT ALL SELECT {{ select_list }} FROM {{ new }}) AS d) AS only_in_old,
  (SELECT count(*) FROM (SELECT {{ select_list }} FROM {{ new }} EXCEPT ALL SELECT {{ select_list }} FROM {{ old }}) AS d) AS only_in_new,
  now() AS compared_at
{% endmacro %}
