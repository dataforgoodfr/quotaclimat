-- Taux de mésinformation et part d'environnement par émission, chaînes françaises
-- Unité d'analyse : l'émission quand analytics.program la connaît (Google Sheet Programmes, chaînes d'info
-- en continue), sinon le programme suivi de program_metadata (colonne type)
-- Variables Metabase, type Date (jours de programme, heure de Paris) :
--   debut premier jour inclus, ex. 2026-09-01
--   fin   dernier jour inclus, ex. 2026-09-30
-- Lit analytics.emission_jour, précalculée par dbt (voir my_dbt_project/models/schema.yml) : par émission et
-- par jour de programme (le jour où l'émission commence, une émission qui finit après minuit compte pour la
-- veille) :
--   volume_heures : durée de diffusion théorique d'après la grille (analytics.program) ;
--   env_heures : temps consacré à l'environnement, même calcul que core_query_environmental_shares
--     (public.keywords.number_of_keywords fenêtres de 20 s) ; chaque segment va au programme posé à
--     l'ingestion (keywords.program_metadata_id), comme dans les dashboards, puis à l'émission de ce
--     programme à l'antenne (la plus proche pour les segments des marges d'ingestion de 6 min) ;
--   nb_cas : cas validés (analytics.cas_de_mesinformation, 1re annotation 'Correct'), sans id de programme :
--     rattachés par leur heure, créneaux élargis comme à l'ingestion.
-- La période se compte en jours entiers de programme : pas de coupe à l'heure près.
-- NOT MATERIALIZED : les bornes sont recopiées dans la lecture comme des constantes, Postgres choisit l'index
-- sur jour dès la planification
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
emissions AS (
    SELECT
        ej.emission_cle,
        MAX(ej.channel_title) AS chaine,
        MAX(ej.type) AS type,
        MAX(ej.nom) AS nom,
        BOOL_OR(ej.rediffusion) AS rediffusion,
        SUM(ej.nb_diffusions) AS nb_diffusions,
        SUM(ej.volume_heures) AS volume_heures,
        SUM(ej.env_heures) AS env_heures,
        SUM(ej.nb_cas) AS nb_cas
    FROM analytics.emission_jour ej
    CROSS JOIN bornes b
    WHERE ej.jour BETWEEN b.debut AND b.fin
    GROUP BY ej.emission_cle
)
SELECT
    chaine,
    type,
    nom,
    rediffusion,
    nb_diffusions,
    ROUND(volume_heures::numeric, 1) AS volume_heures,
    ROUND(env_heures::numeric, 2) AS env_heures,
    ROUND((100 * env_heures / volume_heures)::numeric, 2) AS part_env_pct,
    nb_cas,
    ROUND((nb_cas / volume_heures)::numeric, 3) AS cas_par_heure,
    ROUND((10 * nb_cas / volume_heures)::numeric, 2) AS cas_pour_10h,
    -- NULL quand l'émission n'a pas parlé d'environnement sur la période
    ROUND((nb_cas / NULLIF(env_heures, 0))::numeric, 2) AS cas_par_heure_env
FROM emissions
-- seulement les émissions diffusées pendant la période
WHERE nb_diffusions > 0
  AND chaine != 'C8'
ORDER BY cas_par_heure_env DESC NULLS LAST, part_env_pct DESC
