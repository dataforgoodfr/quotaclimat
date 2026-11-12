{# Double run of models/daily: one row per table of models/dashboards and models/homepage, compared with the same
   table computed from the daily layer. only_in_old = only_in_new = 0 means the two tables are identical (numbers
   rounded to 6 decimals). Rebuilt on every run, after both; checked by tests/double_run_identical.sql. #}
{{ config(materialized='table') }}

{%- set pairs = [
    ('core_query_environmental_shares', 'core_query_environmental_shares_from_daily'),
    ('core_query_environmental_shares_be', 'core_query_environmental_shares_be_from_daily'),
    ('core_query_environmental_shares_i8n', 'core_query_environmental_shares_i8n_from_daily'),
    ('core_query_thematics_keywords', 'core_query_thematics_keywords_from_daily'),
    ('core_query_thematics_keywords_be', 'core_query_thematics_keywords_be_from_daily'),
    ('core_query_thematics_keywords_i8n', 'core_query_thematics_keywords_i8n_from_daily'),
    ('thematic_query_ocean', 'thematic_query_ocean_incremental'),
    ('homepage_environment_by_media_by_month', 'homepage_environment_by_media_by_month_from_daily'),
] %}

{% for old, new in pairs %}
{{ compare_relations(ref(old), ref(new), old, new) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
