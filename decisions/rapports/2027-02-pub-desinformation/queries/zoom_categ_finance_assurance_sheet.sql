-- 1.4 Zoom par catégorie de produit, secteur « Établissements financiers et assurance » : part de chaque catégorie dans le temps de pub
-- du secteur, par type de chaîne. Même requête pour les trois zooms (Automobile et transport, Alimentation,
-- Établissements financiers et assurance), seul le secteur du CTE parametres change : une règle modifiée se reporte
-- dans les trois fichiers zoom_categ_*_sheet.sql.
-- Barres horizontales groupées : une ligne par catégorie, une série par type de chaîne.
-- SQL brut, sans variable : la période est le CTE plage, le secteur le CTE parametres.
--
-- Colonnes : secteur ; categorie (« Catégorie inconnue » sans catégorie prédite) ; « Généralistes », « Chaînes
-- d'information » (noms des séries du graphique) : part du temps de pub du secteur sur le type de chaîne (de 0 à
-- 100) ; generalistes_heures, chaines_info_heures : heures de pub ; part_tous_types_pct : part dans le secteur
-- toutes chaînes confondues (ordre des lignes).
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT 'Établissements financiers et assurance'::text AS secteur
),
agg AS (
    SELECT
        p.secteur,
        COALESCE(p.categorie_produit, 'Catégorie inconnue') AS categorie,
        p.est_info_continue,
        SUM(p.duree_sec) AS secondes
    FROM curated.publicite p
    CROSS JOIN plage pl
    JOIN parametres pa ON pa.secteur = p.secteur
    WHERE p.jour BETWEEN pl.debut AND pl.fin
    GROUP BY 1, 2, 3
),
parts AS (
    SELECT
        secteur,
        categorie,
        est_info_continue,
        secondes,
        100 * secondes / SUM(secondes) OVER (PARTITION BY est_info_continue) AS part_pct,
        100 * SUM(secondes) OVER (PARTITION BY categorie) / SUM(secondes) OVER () AS part_tous_types_pct
    FROM agg
)
SELECT
    secteur,
    categorie,
    ROUND(COALESCE(MAX(part_pct) FILTER (WHERE NOT est_info_continue), 0)::numeric, 2) AS "Généralistes",
    ROUND(COALESCE(MAX(part_pct) FILTER (WHERE est_info_continue), 0)::numeric, 2) AS "Chaînes d'information",
    ROUND((COALESCE(MAX(secondes) FILTER (WHERE NOT est_info_continue), 0) / 3600)::numeric, 1)
        AS generalistes_heures,
    ROUND((COALESCE(MAX(secondes) FILTER (WHERE est_info_continue), 0) / 3600)::numeric, 1)
        AS chaines_info_heures,
    ROUND(MAX(part_tous_types_pct)::numeric, 2) AS part_tous_types_pct
FROM parts
GROUP BY secteur, categorie
ORDER BY part_tous_types_pct DESC, categorie
