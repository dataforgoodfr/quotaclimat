-- 1.4 Répartition sectorielle du temps de pub par type de chaîne : part de chaque secteur dans le temps de
-- publicité des généralistes et dans celui des chaînes d'information.
-- Barres horizontales groupées : une ligne par secteur, une série par type de chaîne.
-- SQL brut, sans variable : la période est le CTE plage.
--
-- Colonnes : secteur ; « Généralistes », « Chaînes d'information » (noms des séries du graphique) : part du temps
-- de pub du type de chaîne (de 0 à 100) ;
-- generalistes_heures, chaines_info_heures : heures de pub ; part_tous_types_pct : part toutes chaînes confondues
-- (ordre des lignes).
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
agg AS (
    SELECT
        COALESCE(p.secteur, 'Secteur inconnu') AS secteur,
        p.est_info_continue,
        SUM(p.duree_sec) AS secondes
    FROM curated.publicite p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
    GROUP BY 1, 2
),
parts AS (
    SELECT
        secteur,
        est_info_continue,
        secondes,
        100 * secondes / SUM(secondes) OVER (PARTITION BY est_info_continue) AS part_pct,
        100 * SUM(secondes) OVER (PARTITION BY secteur) / SUM(secondes) OVER () AS part_tous_types_pct
    FROM agg
)
SELECT
    secteur,
    ROUND(COALESCE(MAX(part_pct) FILTER (WHERE NOT est_info_continue), 0)::numeric, 2) AS "Généralistes",
    ROUND(COALESCE(MAX(part_pct) FILTER (WHERE est_info_continue), 0)::numeric, 2) AS "Chaînes d'information",
    ROUND((COALESCE(MAX(secondes) FILTER (WHERE NOT est_info_continue), 0) / 3600)::numeric, 1) AS generalistes_heures,
    ROUND((COALESCE(MAX(secondes) FILTER (WHERE est_info_continue), 0) / 3600)::numeric, 1) AS chaines_info_heures,
    ROUND(MAX(part_tous_types_pct)::numeric, 2) AS part_tous_types_pct
FROM parts
GROUP BY secteur
ORDER BY part_tous_types_pct DESC, secteur
