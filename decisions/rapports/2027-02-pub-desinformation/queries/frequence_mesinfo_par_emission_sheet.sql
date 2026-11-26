-- 2.2 Fréquence de mésinformation par émission : cas validés par heure consacrée au climat (fenêtres de 20 s avec
-- au moins un mot-clé climat, curated.emission_jour.duree_climat_min ; biodiversité et ressources exclues) ;
-- émissions des chaînes de télévision dont la publicité est analysée, avec au moins un cas et au moins
-- climat_heures_min heures de climat (en dessous, un seul cas donne un taux très élevé).
-- Barres horizontales, une par émission (les nb_emissions premières), colorées par type de chaîne : la valeur est
-- dans la colonne « Généralistes » ou « Chaînes d'information » selon le type (l'autre reste vide).
-- SQL brut, sans variable : la période est le CTE plage, les paramètres le CTE parametres.
--
-- Colonnes : emission (« Nom (chaîne) ») ; « Généralistes », « Chaînes d'information » (noms des séries du
-- graphique) : cas validés par heure de climat ; chaine, nb_cas, heures_climat : pour vérifier.
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT 2::numeric AS climat_heures_min, 12 AS nb_emissions
),
chaines AS (
    SELECT DISTINCT p.chaine_id
    FROM curated.publicite p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
),
emissions AS (
    SELECT
        e.emission_id,
        MAX(e.emission_nom) AS emission_nom,
        MAX(e.chaine_nom) AS chaine,
        BOOL_OR(e.est_info_continue) AS est_info_continue,
        SUM(e.nb_cas_mesinformation) AS nb_cas,
        SUM(e.duree_climat_min) / 60 AS heures_climat
    FROM curated.emission_jour e
    CROSS JOIN plage pl
    JOIN chaines c ON c.chaine_id = e.chaine_id
    WHERE e.jour BETWEEN pl.debut AND pl.fin
    GROUP BY e.emission_id
),
classees AS (
    SELECT
        em.*,
        ROUND((em.nb_cas / em.heures_climat)::numeric, 2) AS cas_par_heure_climat,
        ROW_NUMBER() OVER (ORDER BY em.nb_cas / em.heures_climat DESC, em.nb_cas DESC, em.emission_nom) AS rang
    FROM emissions em
    CROSS JOIN parametres pa
    WHERE em.nb_cas > 0
      AND em.heures_climat >= pa.climat_heures_min
)
SELECT
    emission_nom || ' (' || chaine || ')' AS emission,
    CASE WHEN NOT est_info_continue THEN cas_par_heure_climat END AS "Généralistes",
    CASE WHEN est_info_continue THEN cas_par_heure_climat END AS "Chaînes d'information",
    chaine,
    nb_cas,
    ROUND(heures_climat::numeric, 1) AS heures_climat
FROM classees
WHERE rang <= (SELECT nb_emissions FROM parametres)
ORDER BY rang
