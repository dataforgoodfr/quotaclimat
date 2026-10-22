{{
    config(
        alias='emission_jour',
        indexes=[
            {'columns': ['jour']},
            {'columns': ['chaine_id']},
        ],
    )
}}

{#- curate.emission_jour (model curate_emission_jour: public.emission_jour has the same name): public.emission_jour (one row per emission and program day, French channels) with
    French column names in minutes, the channel attributes of curate.chaine and the shares, without the days the
    channel was not captured (outage, channel still in the grid but no longer broadcast): their airtime would
    count as 0 % environment. Documented in _curate.yml. -#}
{%- set sujets = [
    ('environnement', 'env_heures'),
    ('climat', 'climat_heures'),
    ('biodiversite', 'biodiversite_heures'),
    ('ressources', 'ressources_heures'),
] -%}
{%- set themes = [
    'climat_constat', 'climat_causes', 'climat_consequences', 'climat_attenuation', 'climat_adaptation',
    'biodiversite_constat', 'biodiversite_causes', 'biodiversite_consequences', 'biodiversite_solutions',
    'ressources_constat', 'ressources_solutions',
] %}
SELECT
    (e.emission_cle || '|' || e.jour)::text AS emission_jour_id,
    e.emission_cle::text AS emission_id,
    e.nom::text AS emission_nom,
    (e.type = 'émission') AS est_emission_identifiee,
    e.rediffusion AS est_rediffusion,
    e.jour::date AS jour,
    e.channel_name::text AS chaine_id,
    c.chaine_nom,
    c.est_service_public,
    c.est_info_continue,
    c.est_radio,
    e.nb_diffusions::integer AS nb_creneaux,
    (e.volume_heures * 60)::double precision AS duree_suivie_min,
{%- for sujet, colonne in sujets %}
    (e.{{ colonne }} * 60)::double precision AS duree_{{ sujet }}_min,
{%- endfor %}
{%- for theme in themes %}
    (e.{{ theme }}_heures * 60)::double precision AS duree_{{ theme }}_min,
{%- endfor %}
{%- for sujet, colonne in sujets %}
    (100 * e.{{ colonne }} / NULLIF(e.volume_heures, 0))::double precision AS part_{{ sujet }}_pct,
{%- endfor %}
    e.nb_cas::integer AS nb_cas_mesinformation
FROM {{ ref('emission_jour') }} e
LEFT JOIN {{ ref('chaine') }} c ON c.chaine_id = e.channel_name
WHERE e.chaine_captee
