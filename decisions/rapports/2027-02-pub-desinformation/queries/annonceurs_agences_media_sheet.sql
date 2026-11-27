-- 3 Principaux annonceurs exposés à la mésinformation et leurs agences média : tableau (pas de graphique) des
-- nb_annonceurs entreprises auxquelles le plus de cas de mésinformation sont attribués, toutes chaînes.
-- SQL brut, sans variable : la période est le CTE plage, les paramètres le CTE parametres.
--
-- Cas attribués (calcul de metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql, niveau entreprise, sans
-- pondération par le temps de climat) : une pub est rattachée à son émission (advertising.pub_annonceur_emission_jour) ;
-- les cas validés de chaque émission sur la période (public.emission_jour) sont répartis entre les entreprises au
-- prorata de leurs secondes de pub autour de l'émission. Une entreprise qui a 10 % de la pub d'une émission à 20 cas
-- se voit attribuer 2 cas. Piège : une émission avec peu de pub (France Info TV) concentre ses cas sur quelques
-- annonceurs (voir decisions.md, remarques). Pubs sans marque détectée écartées.
-- Agence : celle de la marque, sinon de son entreprise (Google Sheet Inventaire_des_annonceurs) ; pour une entreprise,
-- l'agence qui achète le plus de ses secondes de pub, avec sa part.
--
-- Colonnes : rang, annonceur, agence (« Agence (Groupe) » si confirmée, « probable : ... », « indice : ... »,
-- « Agence inconnue »), secteur (principal), cas_attribues, part_cas_pct et part_pub_pct (de 0 à 100),
-- indice_surexposition (part_cas_pct / part_pub_pct), pub_min, chaine_principale (qui porte le plus de ses cas
-- attribués) et part_chaine_principale_pct, part_pub_agence_pct (part de sa pub achetée par l'agence).
WITH plage AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT 15 AS nb_annonceurs
),
pub AS (
    SELECT p.entreprise AS annonceur, p.emission_cle, MAX(c.chaine_nom) AS chaine, SUM(p.pub_secondes) AS pub_secondes
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    LEFT JOIN curated.chaine c ON c.chaine_id = p.channel_name
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND p.entreprise IS NOT NULL
    GROUP BY p.entreprise, p.emission_cle
),
cas_emission AS (
    SELECT ej.emission_cle, SUM(ej.nb_cas) AS nb_cas
    FROM public.emission_jour ej
    CROSS JOIN plage pl
    WHERE ej.jour BETWEEN pl.debut AND pl.fin
    GROUP BY ej.emission_cle
),
emissions AS (
    SELECT pe.emission_cle, COALESCE(ce.nb_cas, 0) / pe.pub_secondes AS cas_par_seconde
    FROM (SELECT emission_cle, SUM(pub_secondes) AS pub_secondes FROM pub GROUP BY emission_cle) pe
    LEFT JOIN cas_emission ce ON ce.emission_cle = pe.emission_cle
),
attribution AS (
    SELECT p.annonceur, p.chaine, p.pub_secondes, p.pub_secondes * e.cas_par_seconde AS cas_attribues
    FROM pub p
    JOIN emissions e ON e.emission_cle = p.emission_cle
),
annonceurs AS (
    SELECT
        annonceur,
        SUM(cas_attribues) AS cas_attribues,
        SUM(pub_secondes) AS pub_secondes,
        100 * SUM(cas_attribues) / SUM(SUM(cas_attribues)) OVER () AS part_cas_pct,
        100 * SUM(pub_secondes) / SUM(SUM(pub_secondes)) OVER () AS part_pub_pct,
        ROW_NUMBER() OVER (ORDER BY SUM(cas_attribues) DESC, annonceur) AS rang
    FROM attribution
    GROUP BY annonceur
),
secteur_principal AS (
    SELECT DISTINCT ON (p.entreprise) p.entreprise AS annonceur, p.secteur
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND p.entreprise IS NOT NULL
    GROUP BY p.entreprise, p.secteur
    ORDER BY p.entreprise, SUM(p.pub_secondes) DESC, p.secteur
),
agence_principale AS (
    SELECT DISTINCT ON (p.entreprise) p.entreprise AS annonceur, p.agence_media, p.groupe_agence, p.fiabilite_agence,
        SUM(p.pub_secondes) / SUM(SUM(p.pub_secondes)) OVER (PARTITION BY p.entreprise) AS part
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND p.entreprise IS NOT NULL
    GROUP BY p.entreprise, p.agence_media, p.groupe_agence, p.fiabilite_agence
    ORDER BY p.entreprise, p.agence_media IS NULL, SUM(p.pub_secondes) DESC, p.agence_media
),
chaine_principale AS (
    SELECT DISTINCT ON (annonceur) annonceur, chaine,
        SUM(cas_attribues) / NULLIF(SUM(SUM(cas_attribues)) OVER (PARTITION BY annonceur), 0) AS part
    FROM attribution
    GROUP BY annonceur, chaine
    ORDER BY annonceur, SUM(cas_attribues) DESC, chaine
)
SELECT
    a.rang,
    a.annonceur,
    CASE
        WHEN ag.agence_media IS NULL THEN 'Agence inconnue'
        ELSE CASE WHEN ag.fiabilite_agence IN ('probable', 'indice') THEN ag.fiabilite_agence || ' : ' ELSE '' END
            || ag.agence_media || COALESCE(' (' || NULLIF(ag.groupe_agence, ag.agence_media) || ')', '')
    END AS agence,
    s.secteur,
    ROUND(a.cas_attribues::numeric, 2) AS cas_attribues,
    ROUND(a.part_cas_pct::numeric, 2) AS part_cas_pct,
    ROUND(a.part_pub_pct::numeric, 2) AS part_pub_pct,
    ROUND((a.part_cas_pct / a.part_pub_pct)::numeric, 2) AS indice_surexposition,
    ROUND((a.pub_secondes / 60)::numeric) AS pub_min,
    c.chaine AS chaine_principale,
    ROUND((100 * c.part)::numeric, 1) AS part_chaine_principale_pct,
    CASE WHEN ag.agence_media IS NOT NULL THEN ROUND((100 * ag.part)::numeric, 1) END AS part_pub_agence_pct
FROM annonceurs a
LEFT JOIN secteur_principal s ON s.annonceur = a.annonceur
LEFT JOIN chaine_principale c ON c.annonceur = a.annonceur
LEFT JOIN agence_principale ag ON ag.annonceur = a.annonceur
WHERE a.rang <= (SELECT nb_annonceurs FROM parametres)
ORDER BY a.rang
