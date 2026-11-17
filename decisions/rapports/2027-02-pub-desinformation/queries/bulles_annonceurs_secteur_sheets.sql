-- Part du temps de pub de chaque entreprise annonceuse rattaché à une émission à fort taux de mésinformation, sur les
-- chaînes d'information en continu, rangée par secteur, avec la moyenne de chaque secteur.
-- Une ligne par bulle d'un graphique à bulles : étiquette, abscisse, ordonnée, série, taille.
-- SQL brut, sans variable : les paramètres sont les constantes du CTE parametres.
--
-- Colonnes :
--   etiquette : nom de l'entreprise ; vide pour les moyennes ;
--   x : rang du secteur (1 = premier secteur) ; les bulles d'un même secteur sont alignées verticalement ;
--   part_proche_pct : part du temps de pub rattaché à une émission à fort taux (de 0 à 100) ;
--   serie : nom du secteur pour les entreprises, « Moyenne du secteur » pour les moyennes ;
--   taille : racine carrée des minutes de pub (surface de la bulle proportionnelle aux minutes) ; 2 pour les moyennes ;
--   secteur, rang_secteur, pub_min : pour vérifier.
WITH plage AS (
    SELECT GREATEST(DATE '2025-09-29', (m.fin - INTERVAL '1 year')::date + 1) AS debut, m.fin
    FROM (SELECT LEAST(MAX(jour), DATE '2026-12-31') AS fin FROM curated.publicite) m
),
parametres AS (
    SELECT
        1::numeric AS seuil,
        0.5::numeric AS env_heures_min,
        9 AS nb_secteurs,
        6 AS nb_points
),
-- émissions à fort taux sur la période
emissions_fort_taux AS (
    SELECT ej.emission_cle
    FROM public.emission_jour ej
    CROSS JOIN plage pl
    WHERE ej.jour BETWEEN pl.debut AND pl.fin
    GROUP BY ej.emission_cle
    HAVING SUM(ej.env_heures) >= (SELECT env_heures_min FROM parametres)
       AND SUM(ej.nb_cas) / SUM(ej.env_heures) >= (SELECT seuil FROM parametres)
),
-- pubs des chaînes d'information en continu
pub AS (
    SELECT
        COALESCE(p.secteur, 'Secteur inconnu') AS secteur,
        p.entreprise AS annonceur,
        p.pub_secondes,
        ef.emission_cle IS NOT NULL AS est_proche
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    LEFT JOIN emissions_fort_taux ef ON ef.emission_cle = p.emission_cle
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND c.est_info_continue
),
-- rang des secteurs par temps de pub, toutes chaînes confondues
rang_secteurs AS (
    SELECT
        COALESCE(p.secteur, 'Secteur inconnu') AS secteur,
        ROW_NUMBER() OVER (ORDER BY SUM(p.pub_secondes) DESC, COALESCE(p.secteur, 'Secteur inconnu')) AS rang_secteur
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    WHERE p.jour BETWEEN pl.debut AND pl.fin
    GROUP BY 1
),
secteurs AS (
    SELECT
        pu.secteur,
        SUM(pu.pub_secondes) / 60 AS pub_min,
        100 * COALESCE(SUM(pu.pub_secondes) FILTER (WHERE pu.est_proche), 0) / SUM(pu.pub_secondes) AS part_pct,
        MAX(r.rang_secteur) AS rang_secteur
    FROM pub pu
    JOIN rang_secteurs r ON r.secteur = pu.secteur
    GROUP BY pu.secteur
),
annonceurs AS (
    SELECT
        secteur,
        annonceur,
        SUM(pub_secondes) / 60 AS pub_min,
        100 * COALESCE(SUM(pub_secondes) FILTER (WHERE est_proche), 0) / SUM(pub_secondes) AS part_pct,
        ROW_NUMBER() OVER (PARTITION BY secteur ORDER BY SUM(pub_secondes) DESC, annonceur) AS rang
    FROM pub
    WHERE annonceur IS NOT NULL
    GROUP BY secteur, annonceur
),
points AS (
    SELECT
        a.*,
        s.rang_secteur
    FROM annonceurs a
    JOIN secteurs s ON s.secteur = a.secteur
    CROSS JOIN parametres pa
    WHERE s.rang_secteur <= pa.nb_secteurs
      AND a.rang <= pa.nb_points
)
SELECT
    p.annonceur AS etiquette,
    p.rang_secteur AS x,
    ROUND(p.part_pct::numeric, 3) AS part_proche_pct,
    p.secteur AS serie,
    ROUND(SQRT(p.pub_min)::numeric, 2) AS taille,
    p.secteur,
    p.rang_secteur,
    ROUND(p.pub_min::numeric, 1) AS pub_min
FROM points p
UNION ALL
SELECT '', s.rang_secteur, ROUND(s.part_pct::numeric, 3), 'Moyenne du secteur', 2, s.secteur, s.rang_secteur,
    ROUND(s.pub_min::numeric, 1)
FROM secteurs s
CROSS JOIN parametres pa
WHERE s.rang_secteur <= pa.nb_secteurs
ORDER BY 7, 4, 3
