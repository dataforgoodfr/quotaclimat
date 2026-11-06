{{
    config(
        materialized='table',
    )
}}

{#- Media agency of each advertised brand: one row per brand of analytics.publicites (brand_name, brand_company)
    whose agency is known, from the tab agence_entreprise of the Inventaire_des_annonceurs Google Sheet
    (seed ref_annonceurs_agence_entreprise, see my_dbt_project/external_sources.yml). Two levels in the tab:
    - a row with an empty marque gives the agency of the whole company (entreprise);
    - a row with a marque gives the agency of that brand only, when it differs from its company's (e.g. Herta, in a
      Nestlé company, at Havas Media while Nestlé is at WPP).
    A brand takes the row of its brand when there is one, else the row of its company (agency_level 'marque' or
    'entreprise'); brands with neither are left out. Companies and brands are compared with name_key (case, accents,
    punctuation and spaces ignored), against brand_company and brand_name of publicites (company and brand as
    spelled in the brand inventory, see ad_brands). A row whose agence_media is empty still counts (agency unknown,
    with its reliability 'inconnue'): it overrides nothing for the brands of the company, but a brand row with an
    empty agency hides its company's agency on purpose. Columns cast to text: dbt seed types an empty column as
    integer when the column_types of seeds/ref/_ref_seeds.yml are not applied (seed parsed before its CSV existed). -#}
WITH sheet AS (
  SELECT
    {{ name_key('entreprise::text') }} AS company_key,
    NULLIF({{ name_key('marque::text') }}, '') AS brand_key,
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
      'entreprise', 'marque', 'agence_media', 'groupe_agence', 'fiabilite', 'perimetre_geo', 'perimetre_media',
      'autres_agences', 'agence_precedente', 'date_info', 'verifie_le', 'note', 'source_1']) }}) s
  WHERE NULLIF({{ name_key('entreprise::text') }}, '') IS NOT NULL
),
brands AS (
  SELECT DISTINCT
    brand_name,
    brand_company,
    {{ name_key('brand_name') }} AS brand_key,
    {{ name_key('brand_company') }} AS company_key
  FROM {{ ref('publicites') }}
  WHERE brand_name IS NOT NULL AND brand_company IS NOT NULL
)
SELECT
  b.brand_name,
  b.brand_company,
  CASE WHEN a.brand_key IS NOT NULL THEN 'marque' ELSE 'entreprise' END AS agency_level,
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
-- the brand row when there is one, else the company row (the unique_name_keys test of the seed rules out
-- two rows for the same company and brand)
JOIN LATERAL (
  SELECT *
  FROM sheet s
  WHERE s.company_key = b.company_key
    AND (s.brand_key = b.brand_key OR s.brand_key IS NULL)
  ORDER BY s.brand_key IS NULL
  LIMIT 1
) a ON TRUE
