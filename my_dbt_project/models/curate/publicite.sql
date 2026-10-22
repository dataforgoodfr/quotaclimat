{{
    config(
        indexes=[
            {'columns': ['debut_paris']},
            {'columns': ['chaine_id']},
        ],
    )
}}

{#- curate.publicite: one row per broadcast of a classified ad, from analytics.publicites (French channels,
    from ad_analysis_start_date), with French column names, times in Paris time and the channel attributes of
    curate.chaine. Left out: the classification internals (prediction_status details, predicted codes, LLM
    confidence) and the ids of analytics.program, which is not in the curate schema. Documented in
    _curate.yml. -#}
SELECT
    p.occurrence_id::text AS publicite_id,
    ((p.occurrence_date AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::timestamp AS debut_paris,
    ((p.occurrence_date AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::date AS jour,
    p.channel_name::text AS chaine_id,
    c.chaine_nom,
    c.est_service_public,
    c.est_info_continue,
    c.est_radio,
    p.ad_id::text AS spot_id,
    p.tunnel_id::text AS ecran_id,
    p.duration_sec::double precision AS duree_sec,
    p.sector_label_fr::text AS secteur,
    p.product_category_fr::text AS categorie_produit,
    p.label_final::text AS categorie_ou_secteur,
    p.brand_name::text AS marque,
    p.brand_company::text AS entreprise,
    p.brand_ultimate_parent::text AS societe_mere,
    -- dict_miss, dict_tier2_no_kw and dict_tier3_no_kw wait for the sub-category LLM step (subcat_done)
    (CASE
        WHEN p.prediction_status IN ('dict_tier1', 'dict_tier2', 'dict_tier3') THEN 'dictionnaire'
        WHEN p.prediction_status = 'subcat_done' THEN 'modèle de langage'
        ELSE 'en cours'
    END)::text AS classement_par,
    p.inside_program AS est_dans_programme,
    p.program_before::text AS programme_avant,
    p.program_before_gap_sec::double precision AS ecart_programme_avant_sec,
    p.program_after::text AS programme_apres,
    p.program_after_gap_sec::double precision AS ecart_programme_apres_sec,
    p.nearest_mesinfo_task_aggregate_id::text AS cas_mesinformation_proche_id,
    p.mesinfo_distance_sec::double precision AS distance_cas_mesinformation_sec
FROM {{ ref('publicites') }} p
LEFT JOIN {{ ref('chaine') }} c ON c.chaine_id = p.channel_name
