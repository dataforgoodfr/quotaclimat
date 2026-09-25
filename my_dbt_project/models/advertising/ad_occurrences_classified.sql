{{
    config(
        materialized='table',
    )
}}

{#- The classification reference tables are loaded from a Google Sheet (external_sources.yml) before dbt
    runs: where they are missing (e.g. extended perimeter), labels are left empty instead of failing the run. -#}
WITH sector_ref AS (
  {{ source_or_empty('public', 'ad_sectors', ['sector_code', 'sector_label_fr']) }}
),
cat_ref AS (
  {{ source_or_empty('public', 'ad_categories', ['cat_code', 'product_category_fr']) }}
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
