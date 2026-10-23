{{
    config(
        indexes=[
            {'columns': ['debut_paris']},
            {'columns': ['chaine_id']},
        ],
    )
}}

{#- curated.publicite: one row per broadcast of a classified ad, from analytics.publicites (French channels,
    from ad_analysis_start_date), with French column names, times in Paris time and the channel attributes of
    curated.chaine. Left out: the classification internals (prediction_status details, predicted codes, LLM
    confidence) and the ids of analytics.program, which is not in the curated schema. Documented in
    _curated.yml.
    Links (computed when the model is built, twice a day):
    - lien_mediatree: Mediatree player on the occurrence, which keeps about a year of broadcasts: empty for an
      occurrence older than mediatree_link_max_age_days (dbt var) at build time;
    - lien_sauvegarde: folder of the ad (audio / video saved by the detection) in the Scaleway console;
    - lien_visualiseur: ad viewer of Metabase on the tunnel, highlighting the brand. The viewer matches the raw
      predicted brand (advertising.ad.predicted_brand, case insensitive), not brand_name. -#}

{% set mediatree_link_max_age_days = var('mediatree_link_max_age_days', 365) %}

WITH brand_param AS (
    -- url_encode runs one subquery per value: on the distinct brands only
    SELECT DISTINCT
        predicted_brand,
        {{ url_encode('predicted_brand') }} AS encoded
    FROM {{ ref('publicites') }}
    WHERE predicted_brand IS NOT NULL
)
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
    p.mesinfo_distance_sec::double precision AS distance_cas_mesinformation_sec,
    -- occurrence_date is in UTC: its epoch is the cts of Mediatree
    (CASE WHEN p.occurrence_date >= (now() AT TIME ZONE 'UTC') - interval '{{ mediatree_link_max_age_days }} days'
        THEN 'https://keywords.mediatree.fr/player/?fifo=' || p.channel_name
            || '&start_cts=' || FLOOR(EXTRACT(EPOCH FROM p.occurrence_date))::bigint
            || '&end_cts=' || CEIL(EXTRACT(EPOCH FROM p.occurrence_date) + p.duration_sec)::bigint
            || '&position_cts=' || FLOOR(EXTRACT(EPOCH FROM p.occurrence_date))::bigint
    END)::text AS lien_mediatree,
    ('https://console.scaleway.com/object-storage/buckets/fr-par/advertising-detection/files/ads/'
        || p.ad_id || '/')::text AS lien_sauvegarde,
    -- tunnel_id is channel_name@epoch, channel names being URL-safe slugs: only @ to encode
    ('https://metabase.quotaclimat.org/custom_viewers/visu_pubs.html?tunnel=' || replace(p.tunnel_id, '@', '%40')
        || COALESCE('&annonceur=' || b.encoded, ''))::text AS lien_visualiseur
FROM {{ ref('publicites') }} p
LEFT JOIN {{ ref('chaine') }} c ON c.chaine_id = p.channel_name
LEFT JOIN brand_param b ON b.predicted_brand = p.predicted_brand
