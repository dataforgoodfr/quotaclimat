{{
    config(
        materialized='table',
        schema='advertising',
        indexes=[
            {'columns': ['jour']},
            {'columns': ['emission_cle', 'jour']},
        ],
    )
}}

{#- advertising.pub_annonceur_emission_jour: seconds of ads of each brand around each emission, per day
    (Paris date of the occurrence), French channels. Emission: a row group of analytics.program keyed by
    emission_cle, as in public.emission_jour. From analytics.publicites and the rows of analytics.program before
    / after the ad tunnel of each occurrence (ad_tunnel_programs). An occurrence counts for:
    - the emission containing its whole tunnel (inside_program): all its duration;
    - else the emissions before and after the tunnel, when they are at most ad_emission_window_minutes (dbt
      var) away: half its duration each, all of it for the only one close enough.
    The shares of an advertiser over its emissions thus add up to 100 %. Occurrences far from any emission (and
    the fragments outside tunnels) are left out, so are the occurrences without a predicted brand.
    Three advertiser levels, each a function of the brand (see ad_brands): sums over brands give the seconds of
    a company or of a group, the level is chosen when reading (Metabase):
    - marque: brand_name, the predicted brand as spelled in ad_brands (the predicted brand itself when not listed);
    - entreprise: brand_company, the company of the brand (the brand itself when not in ad_brands);
    - groupe: brand_ultimate_parent, its ultimate parent company (the company, else the brand, when unknown).
    secteur, categorie: the predicted sector and product category of the ads (French label, else code; NULL when
    not predicted), also in the grain, a brand having ads in several sectors and categories; a category belongs
    to a sector. -#}

{% set window_minutes = var('ad_emission_window_minutes', 15) %}

WITH grille AS (
    SELECT
        p.id,
        concat_ws('|', p.channel_name,
            CASE WHEN p.emission IS NOT NULL THEN 'émission' ELSE 'programme' END,
            COALESCE(p.emission, p.channel_program, p.channel_title, p.channel_name),
            COALESCE(p.rediffusion, FALSE)::text) AS emission_cle,
        p.channel_name,
        CASE WHEN p.emission IS NOT NULL THEN 'émission' ELSE 'programme' END AS type,
        COALESCE(p.emission, p.channel_program, p.channel_title, p.channel_name) AS nom,
        COALESCE(p.rediffusion, FALSE) AS rediffusion
    FROM {{ ref('program') }} p
    WHERE p.country = 'france'
),
pubs AS (
    SELECT
        p.brand_name AS marque,
        p.brand_company AS entreprise,
        p.brand_ultimate_parent AS groupe,
        COALESCE(p.sector_label_fr, p.predicted_sector) AS secteur,
        COALESCE(p.product_category_fr, p.predicted_product_category) AS categorie,
        -- occurrence_date is in UTC (timestamp without time zone)
        ((p.occurrence_date AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::date AS jour,
        p.duration_sec,
        p.program_before_id,
        p.program_after_id,
        p.program_before_id IS NOT NULL
            AND (COALESCE(p.inside_program, FALSE) OR p.program_before_gap_sec <= {{ window_minutes }} * 60) AS avant_proche,
        NOT COALESCE(p.inside_program, FALSE)
            AND p.program_after_id IS NOT NULL
            AND p.program_after_gap_sec <= {{ window_minutes }} * 60 AS apres_proche
    FROM {{ ref('publicites') }} p
    WHERE p.brand_company IS NOT NULL
),
pubs_lignes AS (
    SELECT marque, entreprise, groupe, secteur, categorie, jour, program_before_id AS program_id,
        duration_sec * CASE WHEN apres_proche THEN 0.5 ELSE 1 END AS secondes
    FROM pubs
    WHERE avant_proche
    UNION ALL
    SELECT marque, entreprise, groupe, secteur, categorie, jour, program_after_id,
        duration_sec * CASE WHEN avant_proche THEN 0.5 ELSE 1 END
    FROM pubs
    WHERE apres_proche
)
SELECT
    pl.marque,
    pl.entreprise,
    pl.groupe,
    pl.secteur,
    pl.categorie,
    g.emission_cle,
    pl.jour,
    g.channel_name,
    g.type,
    g.nom,
    g.rediffusion,
    SUM(pl.secondes) AS pub_secondes,
    COUNT(*) AS nb_occurrences
FROM pubs_lignes pl
JOIN grille g ON g.id = pl.program_id
GROUP BY pl.marque, pl.entreprise, pl.groupe, pl.secteur, pl.categorie, g.emission_cle, pl.jour, g.channel_name, g.type, g.nom, g.rediffusion
