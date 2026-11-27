-- 2.1 Fréquence de mésinformation par chaîne : cas validés par heure consacrée au climat (fenêtres de 20 s avec au
-- moins un mot-clé climat, curated.emission_jour.duree_climat_min ; biodiversité et ressources exclues). Chaînes de
-- télévision dont la publicité est analysée (curated.publicite).
-- Barres horizontales, une par chaîne, colorées par type de chaîne : la valeur est dans la colonne « Généralistes »
-- ou « Chaînes d'information » selon le type (l'autre reste vide).
-- SQL brut, sans variable : la période est le CTE plage.
--
-- Numérateur et dénominateur sur les mêmes programmes suivis (curated.emission_jour) : un cas hors des programmes
-- suivis n'est pas compté. Pièges : le nombre de cas dépend de l'effort d'annotation ; peu d'heures de climat, taux
-- fragile.
--
-- Colonnes : chaine ; « Généralistes », « Chaînes d'information » (noms des séries du graphique) : cas validés par
-- heure de climat ; type_chaine, nb_cas, heures_climat : pour vérifier.
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
chaines AS (
    SELECT DISTINCT p.chaine_id
    FROM curated.publicite p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
),
taux AS (
    SELECT
        MAX(e.chaine_nom) AS chaine_nom,
        BOOL_OR(e.est_info_continue) AS est_info_continue,
        SUM(e.nb_cas_mesinformation) AS nb_cas,
        SUM(e.duree_climat_min) / 60 AS heures_climat,
        ROUND((SUM(e.nb_cas_mesinformation) / NULLIF(SUM(e.duree_climat_min) / 60, 0))::numeric, 2)
            AS cas_par_heure_climat
    FROM curated.emission_jour e
    CROSS JOIN plage pl
    JOIN chaines c ON c.chaine_id = e.chaine_id
    WHERE e.jour BETWEEN pl.debut AND pl.fin
    GROUP BY e.chaine_id
)
SELECT
    chaine_nom AS chaine,
    CASE WHEN NOT est_info_continue THEN cas_par_heure_climat END AS "Généralistes",
    CASE WHEN est_info_continue THEN cas_par_heure_climat END AS "Chaînes d'information",
    CASE WHEN est_info_continue THEN 'Chaînes d''information' ELSE 'Généralistes' END AS type_chaine,
    nb_cas,
    ROUND(heures_climat::numeric, 1) AS heures_climat
FROM taux
ORDER BY est_info_continue, cas_par_heure_climat DESC NULLS LAST
