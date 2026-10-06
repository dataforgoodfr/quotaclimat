{{
    config(
        materialized='table',
    )
}}

{#- The classification reference tables are the ref_ome_* seeds, downloaded from the private Google Sheet
    before dbt runs (see my_dbt_project/external_sources.yml): where they are missing (e.g. extended perimeter),
    labels are left empty instead of failing the run. -#}
WITH sector_ref AS (
  {{ source_or_empty('advertising', 'ad_sectors', ['sector_code', 'sector_label_fr']) }}
),
cat_ref AS (
  {{ source_or_empty('advertising', 'ad_categories', ['cat_code', 'product_category_fr']) }}
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
  t.tunnel_id,
  a.duration_sec,
  a.predicted_sector,
  a.predicted_product_category,
  a.predicted_brand,
  -- group / ultimate parent company of the brand (see ad_brands), the predicted brand itself when not listed
  COALESCE(br.brand_group, a.predicted_brand) AS brand_group,
  COALESCE(br.brand_ultimate_parent, a.predicted_brand) AS brand_ultimate_parent,
  COALESCE(br.brand_group_label, a.predicted_brand) AS brand_group_label,
  COALESCE(br.brand_ultimate_parent_label, a.predicted_brand) AS brand_ultimate_parent_label,
  a.prediction_status,
  a.prediction_confidence,
  s.sector_label_fr,
  c.product_category_fr,
  COALESCE(c.product_category_fr, s.sector_label_fr) AS label_final,
  mi.mesinfo_distance_sec,
  mi.nearest_mesinfo_task_aggregate_id,
  tp.inside_program,
  tp.program_before,
  tp.program_before_type,
  tp.program_before_gap_sec,
  tp.program_after,
  tp.program_after_type,
  tp.program_after_gap_sec
FROM {{ source('advertising', 'ad_occurrence') }} occ
JOIN {{ source('advertising', 'ad') }} a ON occ.ad_id = a.id
LEFT JOIN sector_ref  s  ON s.sector_code  = a.predicted_sector
LEFT JOIN cat_ref     c  ON c.cat_code     = a.predicted_product_category
LEFT JOIN channel_ref ch ON ch.channel_name = occ.channel_name
LEFT JOIN {{ ref('ad_brands') }} br ON br.brand_key = {{ name_key('a.predicted_brand') }}
LEFT JOIN {{ ref('ad_occurrence_tunnels') }} t ON t.occurrence_id = occ.id
LEFT JOIN {{ ref('ad_occurrence_mesinfo') }} mi ON mi.occurrence_id = occ.id
LEFT JOIN {{ ref('ad_tunnel_programs') }} tp ON tp.tunnel_id = t.tunnel_id
WHERE a.prediction_status IN (
    'dict_miss','dict_tier1','dict_tier2','dict_tier2_no_kw',
    'dict_tier3','dict_tier3_no_kw','subcat_done'
  )
  AND occ.deleted_at IS NULL
  -- first day of the ads analysed (occurrence_date is stored in UTC)
  AND occ.occurrence_date >= '{{ var("ad_analysis_start_date", "2025-09-29") }}'::timestamp
