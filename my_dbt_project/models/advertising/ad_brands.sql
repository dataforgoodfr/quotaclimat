{{
    config(
        materialized='table',
    )
}}

{#- Brand reference, built from the tab Marques of the Inventaires_des_marques Google Sheet only (no ad
    table): one row per brand (name_key of the brand), with the company that sells under the brand and its
    ultimate parent company. Used by ad_occurrences_classified, which falls back on the predicted brand
    for the brands not listed here.
    - entreprise: company of the brand (Activia -> Danone, Free -> Free: a brand can be its own company);
    - societe_mere_ultime: ultimate parent company of the company (consolidation level).
    Each value has its status (e_statut, smu_statut): 'vérifié', 'non vérifié' or 'à vérifier'. A value is
    used unless its status is 'à vérifier' (the job writes the ultimate parent company 'à vérifier').
    Brands are compared with name_key (case, accents, punctuation and spaces ignored). -#}
WITH inventory AS (
  SELECT
    {{ name_key('marque') }} AS brand_key,
    TRIM(marque) AS brand,
    NULLIF(TRIM(entreprise), '') AS company,
    NULLIF(TRIM(societe_mere_ultime), '') AS ultimate_parent,
    NULLIF(TRIM(e_statut), '') AS company_status,
    NULLIF(TRIM(smu_statut), '') AS ultimate_parent_status
  FROM ({{ source_or_empty('advertising', 'brand_inventory', ['marque', 'entreprise', 'societe_mere_ultime', 'e_statut', 'smu_statut']) }}) inventory
),
usable AS (
  SELECT
    *,
    CASE WHEN company_status IS DISTINCT FROM 'à vérifier' THEN company END AS usable_company,
    CASE WHEN ultimate_parent_status IS DISTINCT FROM 'à vérifier' THEN ultimate_parent END AS usable_ultimate_parent
  FROM inventory
  WHERE brand_key <> ''
),
brands AS (
  -- one row per brand key: when two spellings of a brand give the same key, the most checked one wins
  SELECT DISTINCT ON (brand_key) *
  FROM usable
  ORDER BY
    brand_key,
    (usable_ultimate_parent IS NOT NULL) DESC,
    (ultimate_parent_status = 'vérifié') DESC NULLS LAST,
    (usable_company IS NOT NULL) DESC,
    (company_status = 'vérifié') DESC NULLS LAST,
    brand
)
SELECT
  brand_key,
  brand,
  company,
  company_status,
  ultimate_parent,
  ultimate_parent_status,
  -- the company unless 'à vérifier', else the brand itself
  COALESCE(usable_company, brand) AS brand_company,
  -- the ultimate parent company unless 'à vérifier', else the company
  COALESCE(usable_ultimate_parent, usable_company, brand) AS brand_ultimate_parent
FROM brands
