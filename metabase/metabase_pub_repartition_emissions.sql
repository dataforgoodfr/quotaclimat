-- Répartition sur les émissions de la pub d'un annonceur (chaînes françaises), pour un camembert : dimension
-- emission, métrique pub_secondes. Pendant de metabase_pub_repartition_annonceurs.sql.
-- Variables Metabase :
--   debut      type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin        type Date, dernier jour inclus, ex. 2026-09-30
--   marque     type Filtre de champ sur Analytics > Pub Annonceur Emission Jour > Marque, sélection multiple
--   entreprise type Filtre de champ sur Analytics > Pub Annonceur Emission Jour > Entreprise, sélection multiple
--   groupe     type Filtre de champ sur Analytics > Pub Annonceur Emission Jour > Groupe, sélection multiple
--              les trois facultatifs et cumulés (ex. groupe seul : toutes ses marques ; groupe et marque : les
--              marques choisies de ce groupe) ; tous vides : toute la pub, tous annonceurs confondus
--   nb_emissions type Nombre, facultatif (15 si vide) : émissions affichées, les autres regroupées dans « Autres »
-- Lit analytics.pub_annonceur_emission_jour, précalculée par dbt : secondes de pub de chaque marque autour de
-- chaque émission, par jour (date de la pub en heure de Paris). Une pub compte pour l'émission qui contient tout
-- son écran (100 % de sa durée), sinon pour l'émission avant et l'émission après l'écran à moins de 15 min
-- (50 % chacune, 100 % à la seule des deux assez proche) ; les pubs loin de toute émission suivie sont laissées
-- de côté.
-- part_pct : part de l'émission dans la pub de l'annonceur rattachée à une émission sur la période (comme
-- part_annonceur_pct de metabase_pub_annonceurs_par_emission.sql) ; nb_jours : jours avec de la pub de
-- l'annonceur autour de l'émission.
-- Pour situer l'investissement, sur la même période :
--   pub_secondes_emission : toute la pub rattachée à l'émission, tous annonceurs confondus ;
--   part_dans_emission_pct : part de l'annonceur dans cette pub ;
--   volume_heures, nb_cas, cas_par_heure, cas_par_heure_env : métriques de l'émission (analytics.emission_jour,
--     comme metabase_taux_mesinformation_par_emission.sql), par jour de programme (une émission qui finit après
--     minuit compte pour la veille, alors que la pub est datée de son propre jour : écart possible aux bords de
--     la période).
-- Ligne « Autres » : sommes sur ses émissions, taux recalculés sur ces sommes.
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
parametres AS (
    SELECT COALESCE([[CAST({{nb_emissions}} AS integer),]] 15) AS nb_emissions
),
-- secondes de l'annonceur choisi par émission ; table sans alias : les filtres de champ de Metabase la nomment
-- en entier
par_emission AS MATERIALIZED (
    SELECT
        analytics.pub_annonceur_emission_jour.emission_cle,
        MAX(analytics.pub_annonceur_emission_jour.channel_name) AS channel_name,
        MAX(analytics.pub_annonceur_emission_jour.nom) AS nom,
        BOOL_OR(analytics.pub_annonceur_emission_jour.rediffusion) AS rediffusion,
        SUM(analytics.pub_annonceur_emission_jour.pub_secondes) AS pub_secondes,
        COUNT(DISTINCT analytics.pub_annonceur_emission_jour.jour) AS nb_jours
    FROM analytics.pub_annonceur_emission_jour
    CROSS JOIN bornes b
    WHERE analytics.pub_annonceur_emission_jour.jour BETWEEN b.debut AND b.fin
      AND {{marque}}
      AND {{entreprise}}
      AND {{groupe}}
    GROUP BY analytics.pub_annonceur_emission_jour.emission_cle
),
-- toute la pub rattachée à chaque émission retenue, tous annonceurs (index sur emission_cle, jour)
pub_emission AS (
    SELECT
        p.emission_cle,
        SUM(p.pub_secondes) AS pub_secondes_emission
    FROM par_emission pe
    JOIN analytics.pub_annonceur_emission_jour p ON p.emission_cle = pe.emission_cle
    CROSS JOIN bornes b
    WHERE p.jour BETWEEN b.debut AND b.fin
    GROUP BY p.emission_cle
),
-- volume, temps d'environnement et cas de chaque émission retenue sur la période
metriques AS (
    SELECT
        ej.emission_cle,
        SUM(ej.volume_heures) AS volume_heures,
        SUM(ej.env_heures) AS env_heures,
        SUM(ej.nb_cas) AS nb_cas
    FROM par_emission pe
    JOIN analytics.emission_jour ej ON ej.emission_cle = pe.emission_cle
    CROSS JOIN bornes b
    WHERE ej.jour BETWEEN b.debut AND b.fin
    GROUP BY ej.emission_cle
),
classes AS (
    SELECT
        pe.*,
        pu.pub_secondes_emission,
        m.volume_heures,
        m.env_heures,
        m.nb_cas,
        -- nom de la chaîne, dans la table des émissions (index sur emission_cle, jour), sinon son identifiant
        COALESCE(
            (SELECT ej.channel_title FROM analytics.emission_jour ej
             WHERE ej.emission_cle = pe.emission_cle ORDER BY ej.jour DESC LIMIT 1),
            pe.channel_name
        ) AS chaine,
        ROW_NUMBER() OVER (ORDER BY pe.pub_secondes DESC, pe.emission_cle) AS rang,
        SUM(pe.pub_secondes) OVER () AS pub_secondes_total
    FROM par_emission pe
    JOIN pub_emission pu ON pu.emission_cle = pe.emission_cle
    LEFT JOIN metriques m ON m.emission_cle = pe.emission_cle
),
-- les nb_emissions premières, les autres regroupées en une part (rang vide)
parts AS (
    SELECT
        CASE WHEN c.rang <= p.nb_emissions THEN c.rang END AS rang_affiche,
        MAX(c.chaine || ' / ' || c.nom || CASE WHEN c.rediffusion THEN ' (rediffusion)' ELSE '' END) AS emission,
        MIN(c.rang) AS rang,
        COUNT(*) AS nb_emissions,
        SUM(c.pub_secondes) AS pub_secondes,
        MAX(c.pub_secondes_total) AS pub_secondes_total,
        MAX(c.nb_jours) AS nb_jours,
        SUM(c.pub_secondes_emission) AS pub_secondes_emission,
        SUM(c.volume_heures) AS volume_heures,
        SUM(c.env_heures) AS env_heures,
        SUM(c.nb_cas) AS nb_cas
    FROM classes c
    CROSS JOIN parametres p
    GROUP BY 1
)
SELECT
    CASE WHEN rang_affiche IS NOT NULL THEN emission ELSE 'Autres (' || nb_emissions || ' émissions)' END AS emission,
    ROUND(pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * pub_secondes / pub_secondes_total)::numeric, 2) AS part_pct,
    -- sans objet pour « Autres »
    CASE WHEN rang_affiche IS NOT NULL THEN nb_jours END AS nb_jours,
    ROUND(pub_secondes_emission::numeric, 0) AS pub_secondes_emission,
    ROUND((100 * pub_secondes / pub_secondes_emission)::numeric, 2) AS part_dans_emission_pct,
    ROUND(volume_heures::numeric, 1) AS volume_heures,
    nb_cas,
    ROUND((nb_cas / NULLIF(volume_heures, 0))::numeric, 3) AS cas_par_heure,
    -- NULL quand l'émission n'a pas parlé d'environnement sur la période
    ROUND((nb_cas / NULLIF(env_heures, 0))::numeric, 2) AS cas_par_heure_env
FROM parts
-- par secondes décroissantes, « Autres » (rang nb_emissions + 1) en dernier
ORDER BY rang
