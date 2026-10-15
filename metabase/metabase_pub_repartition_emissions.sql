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
classes AS (
    SELECT
        pe.*,
        -- nom de la chaîne, dans la table des émissions (index sur emission_cle, jour), sinon son identifiant
        COALESCE(
            (SELECT ej.channel_title FROM analytics.emission_jour ej
             WHERE ej.emission_cle = pe.emission_cle ORDER BY ej.jour DESC LIMIT 1),
            pe.channel_name
        ) AS chaine,
        ROW_NUMBER() OVER (ORDER BY pe.pub_secondes DESC, pe.emission_cle) AS rang,
        SUM(pe.pub_secondes) OVER () AS pub_secondes_total
    FROM par_emission pe
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
        MAX(c.nb_jours) AS nb_jours
    FROM classes c
    CROSS JOIN parametres p
    GROUP BY 1
)
SELECT
    CASE WHEN rang_affiche IS NOT NULL THEN emission ELSE 'Autres (' || nb_emissions || ' émissions)' END AS emission,
    ROUND(pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * pub_secondes / pub_secondes_total)::numeric, 2) AS part_pct,
    -- sans objet pour « Autres »
    CASE WHEN rang_affiche IS NOT NULL THEN nb_jours END AS nb_jours
FROM parts
-- par secondes décroissantes, « Autres » (rang nb_emissions + 1) en dernier
ORDER BY rang
