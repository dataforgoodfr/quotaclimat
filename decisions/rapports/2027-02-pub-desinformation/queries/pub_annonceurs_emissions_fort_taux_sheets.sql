-- 3 Focus sur les principaux annonceurs : minutes de pub des nb_annonceurs entreprises qui ont le plus de pub
-- rattachée à une émission à fort taux de mésinformation, sur les chaînes d'information en continu, réparties par
-- émission (les nb_emissions premières nommées, les autres en « Autres émissions à fort taux »).
-- Une ligne par annonceur et émission : dans le Sheet, un tableau croisé (formules SOMME.SI.ENS) met une colonne par
-- émission, et le graphique en barres horizontales empilées est construit dessus.
-- SQL brut, sans variable : la période est le CTE plage, les paramètres le CTE parametres.
--
-- Émission à fort taux (decisions.md) : sur la période, au moins seuil cas validés par heure consacrée au climat
-- (public.emission_jour : nb_cas / climat_heures) et au moins climat_heures_min heures de climat.
-- Rattachement des pubs aux émissions : advertising.pub_annonceur_emission_jour. Pubs sans marque détectée écartées.
--
-- Colonnes : rang (de l'annonceur, par minutes de pub autour d'émissions à fort taux), annonceur, emission
-- (« Nom (chaîne) » ou « Autres émissions à fort taux »), pub_min (minutes de la ligne), pub_min_fort_taux et
-- pub_min_total (de l'annonceur), part_fort_taux_pct (pub_min_fort_taux / pub_min_total, de 0 à 100).
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT
        1::numeric AS seuil,
        0.5::numeric AS climat_heures_min,
        15 AS nb_annonceurs,
        8 AS nb_emissions
),
emissions_fort_taux AS (
    SELECT ej.emission_cle
    FROM public.emission_jour ej
    CROSS JOIN plage pl
    WHERE ej.jour BETWEEN pl.debut AND pl.fin
    GROUP BY ej.emission_cle
    HAVING SUM(ej.climat_heures) >= (SELECT climat_heures_min FROM parametres)
       AND SUM(ej.nb_cas) / SUM(ej.climat_heures) >= (SELECT seuil FROM parametres)
),
pub AS (
    SELECT
        p.entreprise AS annonceur,
        p.emission_cle,
        MAX(p.nom) || ' (' || MAX(c.chaine_nom) || ')' AS emission,
        ef.emission_cle IS NOT NULL AS est_fort_taux,
        SUM(p.pub_secondes) AS pub_secondes
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    LEFT JOIN emissions_fort_taux ef ON ef.emission_cle = p.emission_cle
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND p.entreprise IS NOT NULL
      AND c.est_info_continue
    GROUP BY p.entreprise, p.emission_cle, ef.emission_cle
),
annonceurs AS (
    SELECT
        annonceur,
        SUM(pub_secondes) / 60 AS pub_min_total,
        COALESCE(SUM(pub_secondes) FILTER (WHERE est_fort_taux), 0) / 60 AS pub_min_fort_taux,
        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(pub_secondes) FILTER (WHERE est_fort_taux), 0) DESC, annonceur)
            AS rang
    FROM pub
    GROUP BY annonceur
),
gardees AS (
    SELECT pu.*, a.rang
    FROM pub pu
    JOIN annonceurs a ON a.annonceur = pu.annonceur
    WHERE a.rang <= (SELECT nb_annonceurs FROM parametres)
      AND pu.est_fort_taux
),
emissions_nommees AS (
    SELECT emission, ROW_NUMBER() OVER (ORDER BY SUM(pub_secondes) DESC, emission) AS rang_emission
    FROM gardees
    GROUP BY emission
),
lignes AS (
    SELECT
        g.rang,
        g.annonceur,
        CASE WHEN en.rang_emission <= (SELECT nb_emissions FROM parametres) THEN g.emission
             ELSE 'Autres émissions à fort taux' END AS emission,
        SUM(g.pub_secondes) / 60 AS pub_min
    FROM gardees g
    JOIN emissions_nommees en ON en.emission = g.emission
    GROUP BY g.rang, g.annonceur, 3
)
SELECT
    l.rang,
    l.annonceur,
    l.emission,
    ROUND(l.pub_min::numeric, 1) AS pub_min,
    ROUND(a.pub_min_fort_taux::numeric, 1) AS pub_min_fort_taux,
    ROUND(a.pub_min_total::numeric, 1) AS pub_min_total,
    ROUND((100 * a.pub_min_fort_taux / NULLIF(a.pub_min_total, 0))::numeric, 1) AS part_fort_taux_pct
FROM lignes l
JOIN annonceurs a ON a.annonceur = l.annonceur
ORDER BY l.rang, l.emission = 'Autres émissions à fort taux', l.pub_min DESC
