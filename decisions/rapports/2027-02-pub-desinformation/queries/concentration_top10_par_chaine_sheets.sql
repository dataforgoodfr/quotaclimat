-- 1.3 Concentration : part des 10 premières entreprises annonceuses dans le temps de publicité de chaque chaîne
-- (temps d'antenne, pas recettes). Pubs sans marque détectée écartées.
-- Barres horizontales, une par chaîne, colorées par type de chaîne : la valeur est dans la colonne « Généralistes »
-- ou « Chaînes d'information » selon le type (l'autre reste vide).
-- SQL brut, sans variable : la période est le CTE plage.
--
-- Colonnes : chaine ; « Généralistes », « Chaînes d'information » (noms des séries du graphique) : part des
-- 10 premières entreprises (de 0 à 100) ; type_chaine, nb_entreprises, top10_entreprises : pour vérifier.
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
pub AS (
    SELECT p.chaine_nom, BOOL_OR(p.est_info_continue) AS est_info_continue, p.entreprise, SUM(p.duree_sec) AS secondes
    FROM curated.publicite p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND p.entreprise IS NOT NULL
    GROUP BY p.chaine_nom, p.entreprise
),
rangs AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY chaine_nom ORDER BY secondes DESC, entreprise) AS rang,
        SUM(secondes) OVER (PARTITION BY chaine_nom) AS total
    FROM pub
),
chaines AS (
    SELECT
        chaine_nom,
        BOOL_OR(est_info_continue) AS est_info_continue,
        ROUND((100 * SUM(secondes) FILTER (WHERE rang <= 10) / MAX(total))::numeric, 1) AS part_top10_pct,
        COUNT(*) AS nb_entreprises,
        STRING_AGG(entreprise, ', ' ORDER BY rang) FILTER (WHERE rang <= 10) AS top10_entreprises
    FROM rangs
    GROUP BY chaine_nom
)
SELECT
    chaine_nom AS chaine,
    CASE WHEN NOT est_info_continue THEN part_top10_pct END AS "Généralistes",
    CASE WHEN est_info_continue THEN part_top10_pct END AS "Chaînes d'information",
    CASE WHEN est_info_continue THEN 'Chaînes d''information' ELSE 'Généralistes' END AS type_chaine,
    nb_entreprises,
    top10_entreprises
FROM chaines
ORDER BY est_info_continue, part_top10_pct DESC
