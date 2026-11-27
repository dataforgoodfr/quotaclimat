-- 4.2 Les annonceurs dont la pub passe autour d'une mésinformation climatique visant leurs propres produits : par
-- entreprise, minutes de pub de ses catégories visées diffusées autour d'émissions à fort taux de mésinformation contre
-- ces catégories, minutes de ces catégories ailleurs, et reste de sa pub, sur les chaînes d'information en continu.
-- Barres horizontales empilées en minutes (pub_exposee_min, pub_categories_visees_hors_min, pub_autres_min), une barre
-- par entreprise, triées par pub des catégories visées décroissante (pub_categories_visees_min).
-- SQL brut, sans variable : la période est le CTE periode, les paramètres les constantes du CTE parametres.
--
-- Règles (decisions.md), les mêmes que pub_categories_visees_fort_taux_sheet.sql :
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
--     de jour : la pub et les cas peuvent tomber des jours différents) ;
--   - annonceur : l'entreprise ; pubs sans entreprise écartées.
-- Entreprises affichées : au moins pub_exposee_min_minutes minutes de pub exposée. Ce filtre ne change pas les
-- minutes des entreprises gardées.
--
-- Colonnes :
--   annonceur : l'entreprise ;
--   pub_exposee_min : minutes de pub de ses catégories visées autour d'émissions à fort taux contre ces catégories ;
--   pub_categories_visees_hors_min : minutes de pub de ses catégories visées ailleurs ;
--   pub_autres_min : minutes de sa pub dans les catégories non visées ;
--   pub_categories_visees_min : pub_exposee_min + pub_categories_visees_hors_min (clé du tri) ;
--   pub_totale_min : toutes ses minutes de pub sur les chaînes d'information, sur la période ;
--   part_exposee_pct : pub_exposee_min / pub_totale_min, de 0 à 100 ;
--   categories_exposees : les catégories de sa pub exposée, avec leurs minutes.
WITH periode AS (
    SELECT DATE '2026-01-01' AS debut, DATE '2026-12-31' AS fin
),
parametres AS (
    SELECT
        1::numeric AS seuil,
        0.5::numeric AS climat_heures_min,
        2 AS nb_cas_groupe_min,
        10::numeric AS pub_exposee_min_minutes
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
-- toute la pub des entreprises sur les chaînes d'information en continu, avec son groupe si sa catégorie est visée
pub AS (
    SELECT
        p.entreprise AS annonceur,
        p.categorie,
        p.pub_secondes,
        cg.groupe IS NOT NULL AS est_visee,
        kf.emission_cle IS NOT NULL AS est_exposee
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN periode pe
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    LEFT JOIN advertising.ref_ome_categories cat ON cat.product_category_fr = p.categorie
    LEFT JOIN advertising.ref_ome_secteurs s ON s.sector_code = cat.sector_code AND s.sector_label_fr = p.secteur
    LEFT JOIN cat_groupe cg ON cg.cat_code = cat.cat_code AND s.sector_code IS NOT NULL
    LEFT JOIN cles_fort kf ON kf.emission_cle = p.emission_cle AND kf.groupe = cg.groupe
    WHERE p.jour BETWEEN pe.debut AND pe.fin
      AND p.entreprise IS NOT NULL
      AND c.est_info_continue
),
annonceurs AS (
    SELECT
        annonceur,
        COALESCE(SUM(pub_secondes) FILTER (WHERE est_exposee), 0) / 60 AS pub_exposee_min,
        COALESCE(SUM(pub_secondes) FILTER (WHERE est_visee), 0) / 60 AS pub_categories_visees_min,
        SUM(pub_secondes) / 60 AS pub_totale_min
    FROM pub
    GROUP BY 1
),
categories_exposees AS (
    SELECT annonceur, string_agg(categorie || ' (' || ROUND((secondes / 60)::numeric, 1) || ' min)', ', '
                                 ORDER BY secondes DESC, categorie) AS categories
    FROM (
        SELECT annonceur, categorie, SUM(pub_secondes) AS secondes
        FROM pub
        WHERE est_exposee
        GROUP BY 1, 2
    ) e
    GROUP BY 1
)
SELECT
    a.annonceur,
    ROUND(a.pub_exposee_min::numeric, 1) AS pub_exposee_min,
    ROUND((a.pub_categories_visees_min - a.pub_exposee_min)::numeric, 1) AS pub_categories_visees_hors_min,
    ROUND((a.pub_totale_min - a.pub_categories_visees_min)::numeric, 1) AS pub_autres_min,
    ROUND(a.pub_categories_visees_min::numeric, 1) AS pub_categories_visees_min,
    ROUND(a.pub_totale_min::numeric, 1) AS pub_totale_min,
    ROUND((100 * a.pub_exposee_min / NULLIF(a.pub_totale_min, 0))::numeric, 1) AS part_exposee_pct,
    ce.categories AS categories_exposees
FROM annonceurs a
LEFT JOIN categories_exposees ce ON ce.annonceur = a.annonceur
WHERE a.pub_exposee_min >= (SELECT pub_exposee_min_minutes FROM parametres)
ORDER BY a.pub_categories_visees_min DESC, a.annonceur
