-- 4.2 Des pubs diffusées à proximité d'une mésinformation climatique visant leur propre produit : par catégorie de
-- produit visée par des cas, temps de pub de la catégorie diffusé autour d'émissions à fort taux de mésinformation
-- contre cette catégorie, sur les chaînes d'information en continu.
-- Barres horizontales : une barre par catégorie (pub_exposee_min), triées par minutes décroissantes.
-- SQL brut, sans variable : la période est le CTE periode, les paramètres les constantes du CTE parametres.
--
-- Règles (decisions.md) :
--   - catégorie visée : un cas vise une catégorie quand une de ses affirmations porte sur une cible, dans le sens
--     d'une ligne de la grille cibles et sens x catégories qui indique « Produit visé » = 1
--     (advertising.ref_cas_grille_binaire, analytics.cas_categorie_visee) ;
--   - groupe : les catégories visées par exactement les mêmes couples cible et sens forment un groupe, attaqué par
--     les mêmes cas (ex. véhicules électriques : PAU attaque, VE attaque) ;
--   - émission à fort taux contre une catégorie : sur la période, au moins seuil cas validés par heure de climat
--     (tous cas confondus, public.emission_jour) et au moins climat_heures_min heures de climat, et au moins
--     nb_cas_groupe_min cas contre le groupe de la catégorie ;
--   - période : l'année 2026 (CTE periode ; les cas ne sont rattachés aux cibles qu'à partir de 2026) ;
--   - pub autour d'une émission : advertising.pub_annonceur_emission_jour (rattachement par l'émission, sans notion
--     de jour : la pub et les cas peuvent tomber des jours différents).
--
-- Colonnes :
--   categorie, secteur : catégorie de produit et son secteur ;
--   pub_exposee_min : minutes de pub de la catégorie autour d'émissions à fort taux contre elle ;
--   pub_totale_min : toutes ses minutes de pub sur les chaînes d'information, sur la période ;
--   part_exposee_pct : pub_exposee_min / pub_totale_min, de 0 à 100 ;
--   nb_emissions : émissions à fort taux contre la catégorie où elle a de la pub ;
--   nb_cas : cas distincts contre la catégorie dans ces émissions ;
--   principaux_annonceurs : les 3 entreprises qui ont le plus de pub exposée, avec leurs minutes.
WITH periode AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT
        1::numeric AS seuil,
        0.5::numeric AS climat_heures_min,
        2 AS nb_cas_groupe_min
),
-- catégorie -> couples cible et sens qui la visent
cat_cibles AS (
    SELECT
        trim(g.cat_code) AS cat_code,
        string_agg(DISTINCT trim(g.cible_code) || ' ' || lower(trim(g.sens)), ', '
                   ORDER BY trim(g.cible_code) || ' ' || lower(trim(g.sens))) AS cibles
    FROM advertising.ref_cas_grille_binaire g
    WHERE trim(g.produit_vise) = '1'
    GROUP BY 1
),
cat_groupe AS (
    SELECT cc.cat_code, cc.cibles AS groupe
    FROM cat_cibles cc
),
-- cas validés de la période rattachés aux cibles, avec le groupe qu'ils attaquent
cas_groupe AS (
    SELECT DISTINCT c.task_aggregate_id, c.emission_cle, cg.groupe
    FROM analytics.cas_de_mesinformation c
    CROSS JOIN periode pe
    JOIN analytics.cas_categorie_visee v ON v.task_aggregate_id = c.task_aggregate_id
    JOIN cat_groupe cg ON cg.cat_code = v.cat_code
    WHERE c.cibles IS NOT NULL
      AND c.emission_cle IS NOT NULL
      AND (c.data_item_start AT TIME ZONE 'Europe/Paris')::date BETWEEN pe.debut AND pe.fin
),
cles AS (
    SELECT emission_cle, groupe, COUNT(DISTINCT task_aggregate_id) AS nb_cas
    FROM cas_groupe
    GROUP BY 1, 2
),
-- émissions au seuil de fort taux sur la période, tous cas confondus
emissions_fort_taux AS (
    SELECT ej.emission_cle
    FROM public.emission_jour ej
    CROSS JOIN periode pe
    WHERE ej.jour BETWEEN pe.debut AND pe.fin
      AND ej.emission_cle IN (SELECT emission_cle FROM cles)
    GROUP BY ej.emission_cle
    HAVING SUM(ej.climat_heures) >= (SELECT climat_heures_min FROM parametres)
       AND SUM(ej.nb_cas) >= (SELECT seuil FROM parametres) * SUM(ej.climat_heures)
),
-- couples émission et groupe à fort taux
cles_fort AS (
    SELECT k.emission_cle, k.groupe
    FROM cles k
    JOIN emissions_fort_taux USING (emission_cle)
    WHERE k.nb_cas >= (SELECT nb_cas_groupe_min FROM parametres)
),
-- pubs des catégories visées, chaînes d'information en continu
pub AS (
    SELECT
        cat.product_category_fr AS categorie,
        s.sector_label_fr AS secteur,
        p.entreprise,
        p.emission_cle,
        cg.groupe,
        p.pub_secondes,
        kf.emission_cle IS NOT NULL AS est_exposee
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN periode pe
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    JOIN advertising.ref_ome_categories cat ON cat.product_category_fr = p.categorie
    JOIN advertising.ref_ome_secteurs s ON s.sector_code = cat.sector_code AND s.sector_label_fr = p.secteur
    JOIN cat_groupe cg ON cg.cat_code = cat.cat_code
    LEFT JOIN cles_fort kf ON kf.emission_cle = p.emission_cle AND kf.groupe = cg.groupe
    WHERE p.jour BETWEEN pe.debut AND pe.fin
      AND c.est_info_continue
),
cas_exposes AS (
    SELECT e.categorie, COUNT(DISTINCT c.task_aggregate_id) AS nb_cas
    FROM (SELECT DISTINCT categorie, emission_cle, groupe FROM pub WHERE est_exposee) e
    JOIN cas_groupe c ON c.emission_cle = e.emission_cle AND c.groupe = e.groupe
    GROUP BY 1
),
annonceurs AS (
    SELECT
        categorie,
        entreprise,
        SUM(pub_secondes) AS secondes,
        ROW_NUMBER() OVER (PARTITION BY categorie ORDER BY SUM(pub_secondes) DESC, entreprise) AS rang
    FROM pub
    WHERE est_exposee AND entreprise IS NOT NULL
    GROUP BY 1, 2
),
categories AS (
    SELECT
        categorie,
        MIN(secteur) AS secteur,
        SUM(pub_secondes) FILTER (WHERE est_exposee) / 60 AS pub_exposee_min,
        SUM(pub_secondes) / 60 AS pub_totale_min,
        COUNT(DISTINCT emission_cle) FILTER (WHERE est_exposee) AS nb_emissions
    FROM pub
    GROUP BY 1
)
SELECT
    ca.categorie,
    ca.secteur,
    ROUND(ca.pub_exposee_min::numeric, 1) AS pub_exposee_min,
    ROUND(ca.pub_totale_min::numeric, 1) AS pub_totale_min,
    ROUND((100 * ca.pub_exposee_min / NULLIF(ca.pub_totale_min, 0))::numeric, 1) AS part_exposee_pct,
    ca.nb_emissions,
    ce.nb_cas,
    (SELECT string_agg(a.entreprise || ' (' || ROUND((a.secondes / 60)::numeric, 1) || ' min)', ', ' ORDER BY a.rang)
     FROM annonceurs a WHERE a.categorie = ca.categorie AND a.rang <= 3) AS principaux_annonceurs
FROM categories ca
JOIN cas_exposes ce ON ce.categorie = ca.categorie
WHERE ca.pub_exposee_min > 0
ORDER BY ca.pub_exposee_min DESC, ca.categorie
