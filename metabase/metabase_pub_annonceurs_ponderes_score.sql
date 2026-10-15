-- Part des annonceurs pondérée par le score de mésinformation des émissions autour desquelles ils font de la pub
-- (cas par heure consacrée à l'environnement), toutes émissions et tous annonceurs confondus (chaînes françaises),
-- pour un camembert : dimension annonceur, métrique part_score_pct. Pendant de
-- metabase_pub_annonceurs_ponderes_cas.sql, qui pondère par le nombre de cas.
-- Variables Metabase :
--   debut   type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin     type Date, dernier jour inclus, ex. 2026-09-30
--   niveau  type Texte, échelle de l'annonceur : marque, entreprise ou groupe (société mère ultime) ; à régler en
--           liste déroulante de ces trois valeurs, valeur par défaut entreprise (une autre valeur ne renvoie rien)
--   env_heures_min type Nombre, facultatif (0.5 si vide) : émissions gardées, au moins ce temps consacré à
--           l'environnement sur la période ; en dessous le score repose sur trop peu d'heures (1 cas en 6 min
--           d'environnement donne 10 cas par heure) et l'émission est écartée de tout le calcul, pub comprise
--   nb_annonceurs type Nombre, facultatif (15 si vide) : annonceurs affichés, les autres regroupés dans « Autres »
-- Calcul : score de l'émission = nb_cas / env_heures sur la période ; poids d'un annonceur = somme sur ses
-- émissions de ses secondes de pub x score de l'émission. Une seconde de pub autour d'une émission à 2 cas par
-- heure d'environnement pèse deux fois plus qu'autour d'une émission à 1 cas par heure, quelle que soit la durée
-- des émissions (contrairement à la pondération par le nombre de cas).
--   part_score_pct : part de l'annonceur dans la somme des poids ;
--   part_pub_pct : part de l'annonceur dans la pub des émissions gardées, pour comparer ;
--   indice_surexposition : part_score_pct / part_pub_pct, soit le score moyen de ses émissions rapporté au score
--     moyen de toute la pub ; 1 : comme la moyenne, 2 : deux fois plus ;
--   cas_par_heure_env_moyen : score moyen des émissions de l'annonceur, pondéré par ses secondes de pub.
-- Sources (dbt) : analytics.pub_annonceur_emission_jour (secondes de pub de chaque marque autour de chaque
-- émission, par jour de la pub) et analytics.emission_jour (cas et temps d'environnement par émission, par jour
-- de programme : écart possible d'une journée aux bords de la période pour les émissions qui passent minuit).
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
parametres AS (
    SELECT
        COALESCE([[CAST({{env_heures_min}} AS numeric),]] 0.5) AS env_heures_min,
        COALESCE([[CAST({{nb_annonceurs}} AS integer),]] 15) AS nb_annonceurs
),
-- score de chaque émission sur la période, émissions avec assez de temps d'environnement
scores AS MATERIALIZED (
    SELECT
        ej.emission_cle,
        SUM(ej.nb_cas) / SUM(ej.env_heures) AS cas_par_heure_env
    FROM analytics.emission_jour ej
    CROSS JOIN bornes b
    WHERE ej.jour BETWEEN b.debut AND b.fin
    GROUP BY ej.emission_cle
    HAVING SUM(ej.env_heures) >= (SELECT env_heures_min FROM parametres)
),
-- secondes de pub par annonceur (au niveau choisi) autour des émissions gardées, pondérées par leur score
par_annonceur AS (
    SELECT
        CASE CAST({{niveau}} AS text)
            WHEN 'marque' THEN p.marque
            WHEN 'entreprise' THEN p.entreprise
            WHEN 'groupe' THEN p.groupe
        END AS annonceur,
        SUM(p.pub_secondes) AS pub_secondes,
        SUM(p.pub_secondes * s.cas_par_heure_env) AS poids
    FROM analytics.pub_annonceur_emission_jour p
    JOIN scores s ON s.emission_cle = p.emission_cle
    CROSS JOIN bornes b
    WHERE p.jour BETWEEN b.debut AND b.fin
      AND CAST({{niveau}} AS text) IN ('marque', 'entreprise', 'groupe')
    GROUP BY 1
),
classes AS (
    SELECT
        pa.*,
        ROW_NUMBER() OVER (ORDER BY pa.poids DESC, pa.pub_secondes DESC, pa.annonceur) AS rang,
        SUM(pa.poids) OVER () AS poids_total,
        SUM(pa.pub_secondes) OVER () AS pub_secondes_total
    FROM par_annonceur pa
),
-- les nb_annonceurs premiers, les autres regroupés en une part (rang vide)
parts AS (
    SELECT
        CASE WHEN c.rang <= p.nb_annonceurs THEN c.rang END AS rang_affiche,
        MAX(c.annonceur) AS annonceur,
        MIN(c.rang) AS rang,
        COUNT(*) AS nb_annonceurs,
        SUM(c.poids) AS poids,
        SUM(c.pub_secondes) AS pub_secondes,
        MAX(c.poids_total) AS poids_total,
        MAX(c.pub_secondes_total) AS pub_secondes_total
    FROM classes c
    CROSS JOIN parametres p
    GROUP BY 1
)
SELECT
    CASE WHEN rang_affiche IS NOT NULL THEN annonceur ELSE 'Autres (' || nb_annonceurs || ' annonceurs)' END AS annonceur,
    ROUND((100 * poids / NULLIF(poids_total, 0))::numeric, 2) AS part_score_pct,
    ROUND(pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * pub_secondes / pub_secondes_total)::numeric, 2) AS part_pub_pct,
    ROUND(((poids / NULLIF(poids_total, 0)) / (pub_secondes / pub_secondes_total))::numeric, 2)
        AS indice_surexposition,
    ROUND((poids / pub_secondes)::numeric, 3) AS cas_par_heure_env_moyen
FROM parts
-- par poids décroissant, « Autres » (rang nb_annonceurs + 1) en dernier
ORDER BY rang
