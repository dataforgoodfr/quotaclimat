-- 1.3 Intensité publicitaire : minutes de publicité par heure de programme suivi, par chaîne.
-- Barres horizontales, une par chaîne, colorées par type de chaîne : la valeur est dans la colonne « Généralistes »
-- ou « Chaînes d'information » selon le type (l'autre reste vide).
-- SQL brut, sans variable : la période est le CTE plage.
--
-- Numérateur : toutes les publicités détectées (curated.publicite), qui ne sont cherchées qu'autour des programmes
-- suivis (+/- 15 min). Dénominateur : durée des programmes suivis (curated.emission_jour), jusqu'au dernier jour de
-- pub extrait seulement (sinon les jours sans pub extraite gonflent le dénominateur).
-- Pièges : pour les généralistes, les écrans juste avant et après un JT court pèsent lourd (ne pas les comparer aux
-- chaînes d'info) ; France Télévisions, Arte et France Médias Monde : surtout des parrainages.
--
-- Colonnes : chaine ; « Généralistes », « Chaînes d'information » (noms des séries du graphique) : minutes de pub
-- par heure suivie ; type_chaine, pub_min, heures_suivies : pour vérifier.
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, LEAST(DATE '2026-12-31', (SELECT MAX(jour) FROM curated.publicite)) AS fin
),
pub AS (
    SELECT p.chaine_id, MAX(p.chaine_nom) AS chaine_nom, BOOL_OR(p.est_info_continue) AS est_info_continue,
        SUM(p.duree_sec) / 60 AS pub_min
    FROM curated.publicite p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
    GROUP BY p.chaine_id
),
suivi AS (
    SELECT e.chaine_id, SUM(e.duree_suivie_min) / 60 AS heures_suivies
    FROM curated.emission_jour e
    CROSS JOIN plage pl
    WHERE e.jour BETWEEN pl.debut AND pl.fin
    GROUP BY e.chaine_id
),
taux AS (
    SELECT
        pub.chaine_nom,
        pub.est_info_continue,
        pub.pub_min,
        s.heures_suivies,
        ROUND((pub.pub_min / NULLIF(s.heures_suivies, 0))::numeric, 1) AS pub_min_par_heure
    FROM pub
    JOIN suivi s ON s.chaine_id = pub.chaine_id
)
SELECT
    chaine_nom AS chaine,
    CASE WHEN NOT est_info_continue THEN pub_min_par_heure END AS "Généralistes",
    CASE WHEN est_info_continue THEN pub_min_par_heure END AS "Chaînes d'information",
    CASE WHEN est_info_continue THEN 'Chaînes d''information' ELSE 'Généralistes' END AS type_chaine,
    ROUND(pub_min::numeric) AS pub_min,
    ROUND(heures_suivies::numeric) AS heures_suivies
FROM taux
ORDER BY est_info_continue, pub_min_par_heure DESC
