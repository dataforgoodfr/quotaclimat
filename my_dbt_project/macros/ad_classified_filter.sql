{#- Occurrences analysed in the advertising models: ads that went through the classification pipeline,
    occurrence not deleted, broadcast from ad_analysis_start_date (dbt var, UTC). `occ` and `ad` are the
    aliases of the advertising.ad_occurrence and advertising.ad tables in the query. -#}
{% macro ad_classified_filter(occ='occ', ad='a') -%}
  {{ ad }}.prediction_status IN (
    'dict_miss','dict_tier1','dict_tier2','dict_tier2_no_kw',
    'dict_tier3','dict_tier3_no_kw','subcat_done'
  )
  AND {{ occ }}.deleted_at IS NULL
  -- first day of the ads analysed (occurrence_date is stored in UTC)
  AND {{ occ }}.occurrence_date >= '{{ var("ad_analysis_start_date", "2025-09-29") }}'::timestamp
{%- endmacro %}
