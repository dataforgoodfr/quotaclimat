{{
    config(
        materialized='table',
        schema='analytics',
    )
}}

{#- One row per occurrence of a classified ad (formerly advertising.ad_occurrences_classified), built in
    the analytics schema after the advertising models it reads (see entrypoints/dbt.sh). The classification reference tables are the ref_ome_* seeds, downloaded from the private Google Sheet
    before dbt runs (see my_dbt_project/external_sources.yml): where they are missing (e.g. extended perimeter),
    labels are left empty instead of failing the run.
    Emission: the emission of program_emissions on the air at the occurrence (Programmes Google Sheet),
    NULL outside the emissions listed there. -#}
{#- time zone of advertising.ad_occurrence.occurrence_date (timestamp without time zone) -#}
{% set occurrence_tz = var('ad_occurrence_timezone', 'UTC') %}
WITH occurrences AS (
  SELECT id, channel_name, occurrence_date
  FROM {{ source('advertising', 'ad_occurrence') }}
  WHERE deleted_at IS NULL
    -- first day of the ads analysed (occurrence_date is stored in UTC)
    AND occurrence_date >= '{{ var("ad_analysis_start_date", "2025-09-29") }}'::timestamp
),
occurrence_emissions AS (
  {{ program_emission_at('occurrences', 'id', 'channel_name', 'occurrence_date', occurrence_tz) }}
),
sector_ref AS (
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
  -- company / ultimate parent company of the brand (see ad_brands), the predicted brand itself when not listed
  COALESCE(br.brand_company, a.predicted_brand) AS brand_company,
  COALESCE(br.brand_ultimate_parent, a.predicted_brand) AS brand_ultimate_parent,
  COALESCE(br.brand_company_label, a.predicted_brand) AS brand_company_label,
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
  tp.program_after_gap_sec,
  em.emission_id,
  em.emission,
  em.presentation AS emission_presentation,
  em.rediffusion AS emission_rediffusion,
  em.emission_start,
  em.emission_end
FROM {{ source('advertising', 'ad_occurrence') }} occ
JOIN {{ source('advertising', 'ad') }} a ON occ.ad_id = a.id
LEFT JOIN sector_ref  s  ON s.sector_code  = a.predicted_sector
LEFT JOIN cat_ref     c  ON c.cat_code     = a.predicted_product_category
LEFT JOIN channel_ref ch ON ch.channel_name = occ.channel_name
LEFT JOIN {{ ref('ad_brands') }} br ON br.brand_key = {{ name_key('a.predicted_brand') }}
LEFT JOIN {{ ref('ad_occurrence_tunnels') }} t ON t.occurrence_id = occ.id
LEFT JOIN {{ ref('ad_occurrence_mesinfo') }} mi ON mi.occurrence_id = occ.id
LEFT JOIN {{ ref('ad_tunnel_programs') }} tp ON tp.tunnel_id = t.tunnel_id
LEFT JOIN occurrence_emissions em ON em.id = occ.id
WHERE a.prediction_status IN (
    'dict_miss','dict_tier1','dict_tier2','dict_tier2_no_kw',
    'dict_tier3','dict_tier3_no_kw','subcat_done'
  )
  AND occ.deleted_at IS NULL
  -- first day of the ads analysed (occurrence_date is stored in UTC)
  AND occ.occurrence_date >= '{{ var("ad_analysis_start_date", "2025-09-29") }}'::timestamp
