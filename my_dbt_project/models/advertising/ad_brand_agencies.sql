{{
    config(
        materialized='table',
    )
}}

{#- Media agency of each advertised brand: one row per brand of analytics.publicites (brand_name, brand_company)
    whose company is in the tab agence_entreprise of the Inventaire_des_annonceurs Google Sheet (seed
    ref_annonceurs_agence_entreprise, see my_dbt_project/external_sources.yml), one row per company: a brand takes the
    agency of its company. A brand whose agency differs from its company's is made a company of its own in the brand
    inventory (e.g. Herta), not described in this tab. Companies are compared with name_key (case, accents,
    punctuation and spaces ignored), against brand_company of publicites (company as spelled in the brand inventory,
    see ad_brands). A row whose agence_media is empty still counts (agency unknown, reliability 'inconnue').
    Columns cast to text: dbt seed types an empty column as integer when the column_types of
    seeds/ref/_ref_seeds.yml are not applied (seed parsed before its CSV existed). -#}
WITH sheet AS (
  -- one row per company key (the unique_name_keys test of the seed rules out duplicates)
  SELECT DISTINCT ON (company_key) *
  FROM (
    SELECT
      {{ name_key('entreprise::text') }} AS company_key,
      NULLIF(TRIM(agence_media::text), '') AS agency,
      NULLIF(TRIM(groupe_agence::text), '') AS agency_group,
      NULLIF(TRIM(fiabilite::text), '') AS reliability,
      NULLIF(TRIM(perimetre_geo::text), '') AS geographic_scope,
      NULLIF(TRIM(perimetre_media::text), '') AS media_scope,
      NULLIF(TRIM(autres_agences::text), '') AS other_agencies,
      NULLIF(TRIM(agence_precedente::text), '') AS previous_agency,
      NULLIF(TRIM(date_info::text), '') AS info_date,
      NULLIF(TRIM(verifie_le::text), '') AS checked_on,
      NULLIF(TRIM(note::text), '') AS note,
      NULLIF(TRIM(source_1::text), '') AS source_1
    FROM ({{ source_or_empty('advertising', 'advertiser_agencies', [
        'entreprise', 'agence_media', 'groupe_agence', 'fiabilite', 'perimetre_geo', 'perimetre_media',
        'autres_agences', 'agence_precedente', 'date_info', 'verifie_le', 'note', 'source_1']) }}) s
  ) s
  WHERE NULLIF(company_key, '') IS NOT NULL
  ORDER BY company_key, agency NULLS LAST
),
brands AS (
  SELECT DISTINCT
    brand_name,
    brand_company,
    {{ name_key('brand_company') }} AS company_key
  FROM {{ ref('publicites') }}
  WHERE brand_name IS NOT NULL AND brand_company IS NOT NULL
)
SELECT
  b.brand_name,
  b.brand_company,
  a.agency,
  a.agency_group,
  a.reliability,
  a.geographic_scope,
  a.media_scope,
  a.other_agencies,
  a.previous_agency,
  a.info_date,
  a.checked_on,
  a.note,
  a.source_1
FROM brands b
JOIN sheet a ON a.company_key = b.company_key
