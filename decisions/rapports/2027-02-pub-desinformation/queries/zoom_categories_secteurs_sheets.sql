-- 1.4 Zooms par catégorie de produit : part de chaque catégorie dans le temps de pub de son secteur, par type de
-- chaîne, pour les secteurs du CTE parametres (Automobile et transport, Alimentation, Établissements financiers et
-- assurance). Un onglet du Sheet par secteur, avec les lignes de ce secteur.
-- Barres horizontales groupées : une ligne par catégorie, une série par type de chaîne.
-- SQL brut, sans variable : la période est le CTE plage, les secteurs le CTE parametres.
--
-- Colonnes : secteur ; categorie (« Catégorie inconnue » sans catégorie prédite) ; « Généralistes », « Chaînes
-- d'information » (noms des séries du graphique) : part du temps de pub du secteur sur le type de chaîne (de 0 à
-- 100) ; generalistes_heures,
-- chaines_info_heures : heures de pub ; part_tous_types_pct : part dans le secteur toutes chaînes confondues (ordre
-- des lignes).
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT secteur, ordre
    FROM (VALUES
        ('Automobile et transport', 1),
        ('Alimentation', 2),
        ('Établissements financiers et assurance', 3)
    ) AS s (secteur, ordre)
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
        100 * secondes / SUM(secondes) OVER (PARTITION BY secteur, est_info_continue) AS part_pct,
        100 * SUM(secondes) OVER (PARTITION BY secteur, categorie) / SUM(secondes) OVER (PARTITION BY secteur)
            AS part_tous_types_pct
    FROM agg
)
SELECT
    pt.secteur,
    pt.categorie,
    ROUND(COALESCE(MAX(pt.part_pct) FILTER (WHERE NOT pt.est_info_continue), 0)::numeric, 2) AS "Généralistes",
    ROUND(COALESCE(MAX(pt.part_pct) FILTER (WHERE pt.est_info_continue), 0)::numeric, 2) AS "Chaînes d'information",
    ROUND((COALESCE(MAX(pt.secondes) FILTER (WHERE NOT pt.est_info_continue), 0) / 3600)::numeric, 1)
        AS generalistes_heures,
    ROUND((COALESCE(MAX(pt.secondes) FILTER (WHERE pt.est_info_continue), 0) / 3600)::numeric, 1)
        AS chaines_info_heures,
    ROUND(MAX(pt.part_tous_types_pct)::numeric, 2) AS part_tous_types_pct
FROM parts pt
JOIN parametres pa ON pa.secteur = pt.secteur
GROUP BY pt.secteur, pt.categorie, pa.ordre
ORDER BY pa.ordre, part_tous_types_pct DESC, pt.categorie
