{#- The final values of advertising.ad.prediction_status (var final_prediction_statuses of dbt_project.yml),
    as a SQL list to put in IN (...): the classification gives a brand and a sector usable in the analyses.
    Same list as FINAL_PREDICTION_STATUSES in
    quotaclimat/data_ingestion/advertising/s03_classification/prediction_status.py, checked by
    test/advertising_classification/test_prediction_status.py. -#}
{% macro final_prediction_statuses() -%}
  {%- for status in var('final_prediction_statuses') -%}
    '{{ status }}'{% if not loop.last %}, {% endif %}
  {%- endfor -%}
{%- endmacro %}
