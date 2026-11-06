{{
    config(
        materialized='table',
    )
}}

{#- Brand reference, built from the tab Marques of the Inventaire_des_marques Google Sheet only (no ad
    table): one row per brand (name_key of the brand), with the company that sells under the brand and its
    ultimate parent company. Used by analytics.publicites, which falls back on the predicted brand
    for the brands not listed here.
    - entreprise: company of the brand (Activia -> Danone, Free -> Free: a brand can be its own company),
      with the country of its head office (pays_siege_social), its SIREN and the department of its head
      office (departement_siege_social), these two empty for a company registered abroad;
    - societe_mere_ultime: ultimate parent company of the company (consolidation level), with the same
      three columns (smu_pays, smu_siren, smu_departement);
    - actionnaire_maj_ultime: ultimate majority shareholder (e.g. a family holding), passed through only.
    Each value has its status (e_statut, smu_statut, amu_statut): 'vérifié', 'non vérifié' or 'à vérifier'. A value is
    used unless its status is 'à vérifier' (the job writes the ultimate parent company 'à vérifier').
    Brands are compared with name_key (case, accents, punctuation and spaces ignored). -#}
WITH inventory AS (
  SELECT
    {{ name_key('marque') }} AS brand_key,
    TRIM(marque) AS brand,
    NULLIF(TRIM(entreprise), '') AS company,
    -- macros/brand_inventory_identity.sql
    {{ clean_country('pays_siege_social') }} AS company_country,
    {{ clean_siren('siren') }} AS company_siren,
    {{ clean_department('departement_siege_social') }} AS company_head_office_department,
    NULLIF(TRIM(societe_mere_ultime), '') AS ultimate_parent,
    {{ clean_country('smu_pays') }} AS ultimate_parent_country,
    {{ clean_siren('smu_siren') }} AS ultimate_parent_siren,
    {{ clean_department('smu_departement') }} AS ultimate_parent_head_office_department,
    NULLIF(TRIM(e_statut), '') AS company_status,
    NULLIF(TRIM(smu_statut), '') AS ultimate_parent_status,
    NULLIF(TRIM(actionnaire_maj_ultime), '') AS ultimate_majority_shareholder,
    NULLIF(TRIM(amu_statut), '') AS ultimate_majority_shareholder_status
  FROM ({{ source_or_empty('advertising', 'brand_inventory', ['marque', 'entreprise', 'pays_siege_social', 'siren', 'departement_siege_social', 'societe_mere_ultime', 'smu_pays', 'smu_siren', 'smu_departement', 'e_statut', 'smu_statut', 'actionnaire_maj_ultime', 'amu_statut']) }}) inventory
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
  company_country,
  company_siren,
  company_head_office_department,
  ultimate_parent,
  ultimate_parent_status,
  ultimate_parent_country,
  ultimate_parent_siren,
  ultimate_parent_head_office_department,
  ultimate_majority_shareholder,
  ultimate_majority_shareholder_status,
  -- the company unless 'à vérifier', else the brand itself
  COALESCE(usable_company, brand) AS brand_company,
  -- the ultimate parent company unless 'à vérifier', else the company
  COALESCE(usable_ultimate_parent, usable_company, brand) AS brand_ultimate_parent
FROM brands
