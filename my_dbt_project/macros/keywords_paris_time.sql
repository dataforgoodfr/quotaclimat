{#- Paris local time of public.keywords.start: a timestamp with time zone in production, converted to Paris
    time; a timestamp without time zone in the dbt seed of the tests, whose values are already Paris times (the
    +01:00 / +02:00 offsets of the CSV are ignored by Postgres when loading a timestamp without time zone). -#}
{% macro keywords_paris_time(alias) %}
  {%- set ns = namespace(start_type=none) -%}
  {%- if execute -%}
    {%- set relation = adapter.get_relation(database=target.database, schema='public', identifier='keywords') -%}
    {%- if relation is not none -%}
      {%- for column in adapter.get_columns_in_relation(relation) if column.name == 'start' -%}
        {%- set ns.start_type = column.data_type -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {%- if ns.start_type is not none and 'with time zone' in ns.start_type -%}
    ({{ alias }}.start AT TIME ZONE 'Europe/Paris')
  {%- else -%}
    {{ alias }}.start::timestamp
  {%- endif -%}
{% endmacro %}
