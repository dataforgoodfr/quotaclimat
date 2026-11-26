-- 3 Un biais pro CNews de certaines agences ? Part du temps de pub de chaque groupe d'agences média diffusée sur
-- CNews, parmi les chaînes d'information en continu privées (BFM TV, CNews, LCI), comparée à celle de toute la pub de
-- ces chaînes (ligne « Toutes les publicités », en gris dans le graphique).
-- Barres horizontales, une par groupe d'agences.
-- SQL brut, sans variable : la période est le CTE plage, les paramètres le CTE parametres.
--
-- Agence d'après le Google Sheet Inventaire_des_annonceurs (advertising.ad_brand_agencies) ; les marques sans agence
-- connue sont dans « Agence inconnue ». Piège : un écart peut venir du secteur des clients de l'agence plutôt que de
-- ses choix d'achat.
--
-- Colonnes : groupe_agence ; part_cnews_pct (de 0 à 100) ; pub_min (BFM TV, CNews et LCI) et nb_diffusions : pour
-- vérifier. Groupes gardés : au moins pub_min_min minutes de pub.
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT 600::numeric AS pub_min_min
),
pub AS (
    SELECT
        COALESCE(ag.agency_group, 'Agence inconnue') AS groupe_agence,
        p.chaine_nom = 'CNews' AS est_cnews,
        p.duree_sec
    FROM curated.publicite p
    CROSS JOIN plage pl
    LEFT JOIN advertising.ad_brand_agencies ag ON ag.brand_name = p.marque AND ag.brand_company = p.entreprise
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND p.est_info_continue
      AND NOT p.est_service_public
),
lignes AS (
    SELECT
        groupe_agence,
        100 * SUM(duree_sec) FILTER (WHERE est_cnews) / SUM(duree_sec) AS part_cnews_pct,
        SUM(duree_sec) / 60 AS pub_min,
        COUNT(*) AS nb_diffusions
    FROM pub
    GROUP BY groupe_agence
    HAVING SUM(duree_sec) / 60 >= (SELECT pub_min_min FROM parametres)
    UNION ALL
    SELECT
        'Toutes les publicités',
        100 * SUM(duree_sec) FILTER (WHERE est_cnews) / SUM(duree_sec),
        SUM(duree_sec) / 60,
        COUNT(*)
    FROM pub
)
SELECT
    groupe_agence,
    ROUND(part_cnews_pct::numeric, 1) AS part_cnews_pct,
    ROUND(pub_min::numeric) AS pub_min,
    nb_diffusions
FROM lignes
ORDER BY part_cnews_pct DESC
