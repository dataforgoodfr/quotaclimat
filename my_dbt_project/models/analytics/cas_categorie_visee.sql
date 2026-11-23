{{
    config(
        materialized='table',
        schema='analytics',
        indexes=[
            {'columns': ['task_aggregate_id', 'cat_code'], 'unique': True},
            {'columns': ['cat_code']},
        ],
    )
}}

{#- analytics.cas_categorie_visee: one row per misinformation case and per ad product category its claims
    attack, rule of the binary version of the Cas_de_mesinfo Google Sheet (tab Lisez-moi): a case attacks a category
    when at least one of its claims goes the reference direction of a target (cas_affirmation_cible.est_sens_reference)
    whose row in the binary grid (seed ref_cas_grille_binaire) has produit_vise = 1. Left out on purpose: the
    claims in the other direction, the indirect links and the categories comforted by the claim (produit_vise = 0).
    The cases are the ones of the sheet, validated or not since: join with analytics.cas_de_mesinformation, whose
    column categories_visees lists the same categories. -#}
WITH grille AS (
    {{ source_or_empty('advertising', 'cas_grille_binaire', ['cible_code', 'cat_code', 'produit_vise']) }}
),
categories AS (
    {{ source_or_empty('advertising', 'ad_categories', ['cat_code', 'sector_code', 'product_category_fr']) }}
),
secteurs AS (
    {{ source_or_empty('advertising', 'ad_sectors', ['sector_code', 'sector_label_fr']) }}
)
SELECT
    ac.task_aggregate_id,
    trim(g.cat_code)::text AS cat_code,
    MIN(cat.product_category_fr)::text AS categorie,
    MIN(s.sector_label_fr)::text AS secteur,
    -- targets through which the case attacks the category, e.g. {ENR,PRX}
    array_agg(DISTINCT ac.cible_code ORDER BY ac.cible_code) AS cibles,
    COUNT(DISTINCT ac.affirmation_id) AS nb_affirmations
FROM {{ ref('cas_affirmation_cible') }} ac
JOIN grille g
  ON upper(trim(g.cible_code)) = ac.cible_code
 AND trim(g.produit_vise::text) = '1'
LEFT JOIN categories cat ON cat.cat_code = trim(g.cat_code)
LEFT JOIN secteurs s ON s.sector_code = cat.sector_code
WHERE ac.est_sens_reference
GROUP BY ac.task_aggregate_id, trim(g.cat_code)
