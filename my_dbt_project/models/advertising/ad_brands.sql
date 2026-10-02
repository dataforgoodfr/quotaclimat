{{
    config(
        materialized='table',
    )
}}

{#- Brand reference, built from the Inventaires_des_marques Google Sheet only (no ad table): one row
    per brand of the tab Marques (name_key of the brand), with its group and the ultimate parent company
    of the group. Used by ad_occurrences_classified, which falls back on the predicted brand for the
    brands not listed here.
    - tab Marques: brand -> group, rows not verified by a human allowed (statut 'non vérifié');
    - tab Groupes: one row per group, with its canonical name (column groupe) that the column groupe of
      the tab Marques must use, and its ultimate parent company, only used when verified by a human
      (statut 'vérifié').
    Names are compared with name_key (case, accents, punctuation and spaces ignored). Nothing is merged
    automatically: a group of the tab Marques that is not in the tab Groupes (group_in_inventory false)
    is kept as written, for a human to correct in the sheet. -#}
WITH inventory_brands AS (
  -- one row per brand key: when two spellings of a brand give the same key, the verified one wins
  SELECT DISTINCT ON (brand_key)
    brand_key,
    brand,
    inventory_group,
    brand_group_source,
    brand_group_status
  FROM (
    SELECT
      {{ name_key('marque') }} AS brand_key,
      TRIM(marque) AS brand,
      NULLIF(TRIM(groupe), '') AS inventory_group,
      NULLIF(TRIM(source), '') AS brand_group_source,
      NULLIF(TRIM(statut), '') AS brand_group_status
    FROM ({{ source_or_empty('advertising', 'brand_inventory', ['marque', 'groupe', 'source', 'statut']) }}) inventory
  ) keyed
  WHERE brand_key <> ''
  ORDER BY brand_key, (brand_group_status = 'vérifié') DESC NULLS LAST, (inventory_group IS NOT NULL) DESC, brand
),
inventory_groups AS (
  -- one row per group key: when two spellings of a group give the same key, the verified one wins
  SELECT DISTINCT ON (group_key)
    group_key,
    group_name,
    group_id,
    ultimate_parent_verified,
    CASE WHEN ultimate_parent_verified THEN ultimate_parent END AS verified_ultimate_parent
  FROM (
    SELECT
      {{ name_key('groupe') }} AS group_key,
      TRIM(groupe) AS group_name,
      NULLIF(TRIM(groupe_id), '') AS group_id,
      NULLIF(TRIM(societe_mere_ultime), '') AS ultimate_parent,
      COALESCE(TRIM(statut) = 'vérifié', FALSE) AS ultimate_parent_verified
    FROM ({{ source_or_empty('advertising', 'group_inventory', ['groupe', 'groupe_id', 'societe_mere_ultime', 'statut']) }}) inventory
  ) keyed
  WHERE group_key <> ''
  ORDER BY group_key, ultimate_parent_verified DESC, group_name
)
SELECT
  b.brand_key,
  b.brand,
  b.inventory_group,
  b.brand_group_source,
  b.brand_group_status,
  b.inventory_group IS NOT NULL AS has_group,
  g.group_key IS NOT NULL AS group_in_inventory,
  g.group_id,
  COALESCE(g.ultimate_parent_verified, FALSE) AS ultimate_parent_verified,
  g.verified_ultimate_parent,
  -- canonical name of the group in the tab Groupes, else the group as written in the tab Marques, else the brand
  COALESCE(g.group_name, b.inventory_group, b.brand) AS brand_group,
  -- verified ultimate parent company, else the group
  COALESCE(g.verified_ultimate_parent, g.group_name, b.inventory_group, b.brand) AS brand_ultimate_parent
FROM inventory_brands b
LEFT JOIN inventory_groups g ON g.group_key = {{ name_key('b.inventory_group') }}
