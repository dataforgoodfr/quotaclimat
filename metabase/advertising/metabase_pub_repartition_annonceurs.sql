-- Répartition des annonceurs qui font de la pub autour d'une ou plusieurs émissions (chaînes françaises),
-- pour un camembert : dimension annonceur, métrique pub_secondes.
-- Variables Metabase :
--   debut   type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin     type Date, dernier jour inclus, ex. 2026-09-30
--   niveau  type Texte, échelle de l'annonceur : marque, entreprise ou groupe (société mère ultime) ; à régler en
--           liste déroulante de ces trois valeurs, valeur par défaut entreprise (une autre valeur ne renvoie rien)
--   emission type Filtre de champ sur Analytics > Pub Annonceur Emission Jour > Nom, sélection multiple ;
--           facultatif (vide : toutes les émissions)
--   chaine  type Filtre de champ sur Analytics > Pub Annonceur Emission Jour > Channel Name, sélection multiple ;
--           facultatif, pour distinguer deux émissions de même nom sur deux chaînes
--   nb_annonceurs type Nombre, facultatif (15 si vide) : annonceurs affichés, les autres regroupés dans « Autres »
-- Lit analytics.pub_annonceur_emission_jour, précalculée par dbt : secondes de pub de chaque marque autour de
-- chaque émission, par jour (date de la pub en heure de Paris). Une pub compte pour l'émission qui contient tout
-- son écran (100 % de sa durée), sinon pour l'émission avant et l'émission après l'écran à moins de 15 min
-- (50 % chacune, 100 % à la seule des deux assez proche).
-- part_pct : part de l'annonceur dans toute la pub rattachée aux émissions choisies sur la période (à ne pas
-- confondre avec part_annonceur_pct de metabase_pub_annonceurs_par_emission.sql, la part de l'émission dans la
-- pub de l'annonceur).
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
parametres AS (
    SELECT COALESCE([[CAST({{nb_annonceurs}} AS integer),]] 15) AS nb_annonceurs
),
-- secondes par annonceur (au niveau choisi) autour des émissions choisies ; table sans alias : les filtres de
-- champ de Metabase la nomment en entier
par_annonceur AS (
    SELECT
        CASE CAST({{niveau}} AS text)
            WHEN 'marque' THEN analytics.pub_annonceur_emission_jour.marque
            WHEN 'entreprise' THEN analytics.pub_annonceur_emission_jour.entreprise
            WHEN 'groupe' THEN analytics.pub_annonceur_emission_jour.groupe
        END AS annonceur,
        SUM(analytics.pub_annonceur_emission_jour.pub_secondes) AS pub_secondes
    FROM analytics.pub_annonceur_emission_jour
    CROSS JOIN bornes b
    WHERE analytics.pub_annonceur_emission_jour.jour BETWEEN b.debut AND b.fin
      AND CAST({{niveau}} AS text) IN ('marque', 'entreprise', 'groupe')
      AND {{emission}}
      AND {{chaine}}
    GROUP BY 1
),
classes AS (
    SELECT
        pa.*,
        ROW_NUMBER() OVER (ORDER BY pa.pub_secondes DESC, pa.annonceur) AS rang,
        SUM(pa.pub_secondes) OVER () AS pub_secondes_total
    FROM par_annonceur pa
),
-- les nb_annonceurs premiers, les autres regroupés en une part
parts AS (
    SELECT
        CASE WHEN c.rang <= p.nb_annonceurs THEN c.annonceur END AS annonceur,
        MIN(c.rang) AS rang,
        COUNT(*) AS nb_annonceurs,
        SUM(c.pub_secondes) AS pub_secondes,
        MAX(c.pub_secondes_total) AS pub_secondes_total
    FROM classes c
    CROSS JOIN parametres p
    GROUP BY 1
)
SELECT
    COALESCE(annonceur, 'Autres (' || nb_annonceurs || ' annonceurs)') AS annonceur,
    ROUND(pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * pub_secondes / pub_secondes_total)::numeric, 2) AS part_pct
FROM parts
-- par secondes décroissantes, « Autres » (rang nb_annonceurs + 1) en dernier
ORDER BY rang
