-- Part des annonceurs pondérée par les cas de mésinformation des émissions autour desquelles ils font de la pub,
-- toutes émissions et tous annonceurs confondus (chaînes françaises), pour un camembert : dimension annonceur,
-- métrique cas_attribues.
-- Variables Metabase :
--   debut   type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin     type Date, dernier jour inclus, ex. 2026-09-30
--   niveau  type Texte, échelle de l'annonceur : marque, entreprise ou groupe (société mère ultime) ; à régler en
--           liste déroulante de ces trois valeurs, valeur par défaut entreprise (une autre valeur ne renvoie rien)
-- Une ligne par annonceur : le regroupement des petites parts en « Autres » est laissé au camembert de Metabase
-- (réglage « Pourcentage minimum de part »).
-- Calcul : les cas de chaque émission sur la période sont répartis entre les annonceurs au prorata de leurs
-- secondes de pub autour de l'émission (cas_attribues), puis additionnés par annonceur. Un annonceur qui a 10 % de
-- la pub d'une émission à 20 cas se voit attribuer 2 cas. La somme des cas attribués est le nombre de cas des
-- émissions qui ont de la pub ; les cas des émissions sans pub ne comptent pas.
--   part_cas_pct : part de l'annonceur dans les cas attribués ;
--   part_pub_pct : part de l'annonceur dans la pub rattachée à une émission, pour comparer ;
--   indice_surexposition : part_cas_pct / part_pub_pct ; 1 : l'annonceur est autour des cas autant que sa pub le
--     laisse attendre, 2 : deux fois plus (sa pub va vers les émissions qui ont le plus de cas) ;
--   cas_par_heure_moyen : cas par heure de programme des émissions de l'annonceur, moyenne pondérée par ses
--     secondes de pub ; indépendant de la durée des émissions, alors que cas_attribues favorise les émissions
--     longues (plus d'heures, plus de cas).
-- Sources (dbt) : analytics.pub_annonceur_emission_jour (secondes de pub de chaque marque autour de chaque
-- émission, par jour de la pub) et analytics.emission_jour (cas et heures de programme par émission, par jour de
-- programme : écart possible d'une journée aux bords de la période pour les émissions qui passent minuit).
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
-- secondes de pub par annonceur (au niveau choisi) et par émission
pub AS MATERIALIZED (
    SELECT
        CASE CAST({{niveau}} AS text)
            WHEN 'marque' THEN p.marque
            WHEN 'entreprise' THEN p.entreprise
            WHEN 'groupe' THEN p.groupe
        END AS annonceur,
        p.emission_cle,
        SUM(p.pub_secondes) AS pub_secondes
    FROM analytics.pub_annonceur_emission_jour p
    CROSS JOIN bornes b
    WHERE p.jour BETWEEN b.debut AND b.fin
      AND CAST({{niveau}} AS text) IN ('marque', 'entreprise', 'groupe')
    GROUP BY 1, 2
),
-- toute la pub de chaque émission
pub_emission AS (
    SELECT emission_cle, SUM(pub_secondes) AS pub_secondes_emission
    FROM pub
    GROUP BY emission_cle
),
-- cas et heures de programme de chaque émission sur la période
metriques AS (
    SELECT
        ej.emission_cle,
        SUM(ej.nb_cas) AS nb_cas,
        SUM(ej.volume_heures) AS volume_heures
    FROM analytics.emission_jour ej
    CROSS JOIN bornes b
    WHERE ej.jour BETWEEN b.debut AND b.fin
    GROUP BY ej.emission_cle
),
par_annonceur AS (
    SELECT
        pu.annonceur,
        SUM(pu.pub_secondes) AS pub_secondes,
        -- cas de l'émission au prorata de la part de l'annonceur dans sa pub
        SUM(COALESCE(m.nb_cas, 0) * pu.pub_secondes / pe.pub_secondes_emission) AS cas_attribues,
        -- pour la moyenne pondérée des cas par heure (émissions sans heures de programme sur la période écartées)
        SUM(pu.pub_secondes * m.nb_cas / NULLIF(m.volume_heures, 0)) AS pub_x_cas_par_heure,
        SUM(pu.pub_secondes) FILTER (WHERE m.volume_heures > 0) AS pub_secondes_avec_taux
    FROM pub pu
    JOIN pub_emission pe ON pe.emission_cle = pu.emission_cle
    LEFT JOIN metriques m ON m.emission_cle = pu.emission_cle
    GROUP BY pu.annonceur
)
SELECT
    annonceur,
    ROUND(cas_attribues::numeric, 2) AS cas_attribues,
    ROUND((100 * cas_attribues / NULLIF(SUM(cas_attribues) OVER (), 0))::numeric, 2) AS part_cas_pct,
    ROUND(pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * pub_secondes / SUM(pub_secondes) OVER ())::numeric, 2) AS part_pub_pct,
    ROUND(((cas_attribues / NULLIF(SUM(cas_attribues) OVER (), 0)) / (pub_secondes / SUM(pub_secondes) OVER ()))::numeric, 2)
        AS indice_surexposition,
    ROUND((pub_x_cas_par_heure / NULLIF(pub_secondes_avec_taux, 0))::numeric, 3) AS cas_par_heure_moyen
FROM par_annonceur
ORDER BY cas_attribues DESC, pub_secondes DESC, annonceur
