{{
    config(
        materialized='table',
    )
}}

{#- The classification reference table only exists in the main database (Metabase upload):
    elsewhere (CI, extended perimeter), labels are left empty instead of failing the run. -#}
{%- set classif_relation = load_relation(source('public', 'ad_classification')) if execute else none -%}
{%- if execute and classif_relation is none -%}
  {{ log("ad_occurrences_classified: " ~ source('public', 'ad_classification') ~ " not found, labels will be NULL", info=True) }}
{%- endif %}

WITH classif AS (
  {%- if classif_relation is not none %}
  SELECT sector_code, cat_code, sector_label_fr, product_category_fr
  FROM {{ classif_relation }}
  {%- else %}
  SELECT
    NULL::text AS sector_code,
    NULL::text AS cat_code,
    NULL::text AS sector_label_fr,
    NULL::text AS product_category_fr
  WHERE FALSE
  {%- endif %}
),
sector_ref AS (
  SELECT DISTINCT sector_code, sector_label_fr
  FROM classif
),
cat_ref AS (
  SELECT cat_code, product_category_fr
  FROM classif
),
channel_ref AS (
  SELECT DISTINCT
    channel_name,
    channel_title,
    infocontinue,
    radio,
    public,
    country
  FROM {{ source('public', 'program_metadata') }}
)
SELECT
  occ.id AS occurrence_id,
  occ.occurrence_date,
  occ.channel_name,
  ch.channel_title,
  ch.infocontinue,
  ch.radio,
  ch.public,
  ch.country,
  occ.ad_id,
  a.duration_sec,
  a.predicted_sector,
  a.predicted_product_category,
  a.predicted_brand,
  a.prediction_status,
  a.prediction_confidence,
  s.sector_label_fr,
  c.product_category_fr,
  COALESCE(c.product_category_fr, s.sector_label_fr) AS label_final
FROM {{ source('advertising', 'ad_occurrence') }} occ
JOIN {{ source('advertising', 'ad') }} a ON occ.ad_id = a.id
LEFT JOIN sector_ref  s  ON s.sector_code  = a.predicted_sector
LEFT JOIN cat_ref     c  ON c.cat_code     = a.predicted_product_category
LEFT JOIN channel_ref ch ON ch.channel_name = occ.channel_name
WHERE a.prediction_status IN (
    'dict_miss','dict_tier1','dict_tier2','dict_tier2_no_kw',
    'dict_tier3','dict_tier3_no_kw','subcat_done'
  )
  AND occ.deleted_at IS NULL
