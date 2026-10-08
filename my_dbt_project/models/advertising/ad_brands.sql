{{
    config(
        materialized='table',
    )
}}

{#- Brand reference, built from the Inventaires_des_marques Google Sheet only (no ad table): one row
    per brand of the tab Marques (name_key of the brand), with the company that sells under the brand and
    its ultimate parent company. Used by ad_occurrences_classified, which falls back on the predicted
    brand for the brands not listed here.
    Three levels: brand (Activia) -> company, the one that sells under the brand and that a publication
    would name (Danone; Free -> Free, a brand can be its own company) -> ultimate parent company
    (consolidation level, holdings).
    - tab Marques: brand -> company, rows not verified by a human allowed (statut 'non vérifié');
    - tab Entreprises: one row per company, with its official name (column entreprise) that the column
      entreprise of the tab Marques must use, its ultimate parent company, only used when verified by a
      human (statut 'vérifié'), and its optional alias (column alias), a short name for reading the
      results: the official names stay in brand_company and brand_ultimate_parent, the labels use the
      alias. Two companies with the same alias (e.g. two IKEA holdings, alias IKEA) have the same label.
    Names are compared with name_key (case, accents, punctuation and spaces ignored). Nothing is merged
    automatically: a company of the tab Marques that is not in the tab Entreprises (company_in_inventory
    false) is kept as written, for a human to correct in the sheet. -#}
WITH inventory_brands AS (
  -- one row per brand key: when two spellings of a brand give the same key, the verified one wins
  SELECT DISTINCT ON (brand_key)
    brand_key,
    brand,
    inventory_company,
    brand_company_source,
    brand_company_status
  FROM (
    SELECT
      {{ name_key('marque') }} AS brand_key,
      TRIM(marque) AS brand,
      NULLIF(TRIM(entreprise), '') AS inventory_company,
      NULLIF(TRIM(source), '') AS brand_company_source,
      NULLIF(TRIM(statut), '') AS brand_company_status
    FROM ({{ source_or_empty('advertising', 'brand_inventory', ['marque', 'entreprise', 'source', 'statut']) }}) inventory
  ) keyed
  WHERE brand_key <> ''
  ORDER BY brand_key, (brand_company_status = 'vérifié') DESC NULLS LAST, (inventory_company IS NOT NULL) DESC, brand
),
inventory_companies AS (
  -- one row per company key: when two spellings of a company give the same key, the verified one wins
  SELECT DISTINCT ON (company_key)
    company_key,
    company_name,
    company_alias,
    company_id,
    ultimate_parent_verified,
    CASE WHEN ultimate_parent_verified THEN ultimate_parent END AS verified_ultimate_parent
  FROM (
    SELECT
      {{ name_key('entreprise') }} AS company_key,
      TRIM(entreprise) AS company_name,
      NULLIF(TRIM(alias), '') AS company_alias,
      NULLIF(TRIM(entreprise_id), '') AS company_id,
      NULLIF(TRIM(societe_mere_ultime), '') AS ultimate_parent,
      COALESCE(TRIM(statut) = 'vérifié', FALSE) AS ultimate_parent_verified
    FROM ({{ source_or_empty('advertising', 'company_inventory', ['entreprise', 'alias', 'entreprise_id', 'societe_mere_ultime', 'statut']) }}) inventory
  ) keyed
  WHERE company_key <> ''
  ORDER BY company_key, ultimate_parent_verified DESC, company_name
),
brands AS (
  SELECT
    b.brand_key,
    b.brand,
    b.inventory_company,
    b.brand_company_source,
    b.brand_company_status,
    c.company_key IS NOT NULL AS company_in_inventory,
    c.company_alias,
    c.company_id,
    COALESCE(c.ultimate_parent_verified, FALSE) AS ultimate_parent_verified,
    c.verified_ultimate_parent,
    -- official name of the company in the tab Entreprises, else the company as written in the tab Marques, else the brand
    COALESCE(c.company_name, b.inventory_company, b.brand) AS brand_company,
    -- verified ultimate parent company, else the company
    COALESCE(c.verified_ultimate_parent, c.company_name, b.inventory_company, b.brand) AS brand_ultimate_parent
  FROM inventory_brands b
  LEFT JOIN inventory_companies c ON c.company_key = {{ name_key('b.inventory_company') }}
)
SELECT
  b.*,
  -- for reading the results: the alias of the company, else its official name
  COALESCE(b.company_alias, b.brand_company) AS brand_company_label,
  -- the alias of the ultimate parent when it is itself a row of the tab Entreprises, else its official name
  COALESCE(p.company_alias, b.brand_ultimate_parent) AS brand_ultimate_parent_label
FROM brands b
LEFT JOIN inventory_companies p ON p.company_key = {{ name_key('b.brand_ultimate_parent') }}
