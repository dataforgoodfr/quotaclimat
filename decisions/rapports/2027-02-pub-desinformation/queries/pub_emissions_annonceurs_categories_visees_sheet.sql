-- 4.2 Où passe la pub exposée des principaux annonceurs : par groupe de catégories visées, minutes de pub de chaque
-- annonceur dans chaque émission à fort taux de mésinformation contre ce groupe, sur les chaînes d'information en
-- continu. Une ligne par groupe, émission et annonceur : dans le Sheet, des formules (QUERY, SUMIFS) font un tableau à
-- double entrée par groupe (une ligne par émission, une colonne par annonceur) avec une mini-barre (SPARKLINE) par case.
-- SQL brut, sans variable : la période est le CTE periode, les paramètres les constantes du CTE parametres.
--
-- Règles (decisions.md), les mêmes que pub_annonceurs_categories_visees_fort_taux_sheet.sql :
--   - catégorie visée : un cas vise une catégorie quand une de ses affirmations porte sur une cible, dans le sens
--     d'une ligne de la grille cibles et sens x catégories qui indique « Produit visé » = 1
--     (advertising.ref_cas_grille_binaire, analytics.cas_categorie_visee) ;
--   - groupe : les catégories visées par exactement les mêmes couples cible et sens forment un groupe, attaqué par
--     les mêmes cas (ex. véhicules électriques : PAU attaque, VE attaque) ;
--   - émission à fort taux contre un groupe : sur la période, au moins seuil cas validés par heure de climat (tous cas
--     confondus, public.emission_jour) et au moins climat_heures_min heures de climat, et au moins nb_cas_groupe_min
--     cas contre le groupe ;
--   - période : l'année 2026 (CTE periode) ;
--   - pub autour d'une émission : advertising.pub_annonceur_emission_jour (rattachement par l'émission, sans notion
--     de jour) ;
--   - annonceur : l'entreprise ; pubs sans entreprise écartées.
-- Annonceurs gardés : ceux du graphique pub_annonceurs_categories_visees_fort_taux (au moins pub_exposee_min_minutes
-- minutes de pub exposée, tous groupes confondus).
--
-- Colonnes :
--   groupe_categories : les catégories du groupe, séparées par « + » (ex. « Électricité + Énergies renouvelables ») ;
--   emission : « Nom (chaîne) » ;
--   annonceur : l'entreprise ;
--   pub_exposee_min : minutes de pub du groupe de l'annonceur autour de l'émission, sur la période.
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
-- groupe (couples cible et sens) et son libellé (ses catégories)
libelles AS (
    SELECT cc.cibles, string_agg(cat.product_category_fr, ' + ' ORDER BY cat.product_category_fr) AS groupe_libelle
    FROM cat_cibles cc
    JOIN advertising.ref_ome_categories cat ON cat.cat_code = cc.cat_code
    GROUP BY 1
),
cat_groupe AS (
    SELECT cc.cat_code, cc.cibles AS groupe, l.groupe_libelle
    FROM cat_cibles cc
    JOIN libelles l USING (cibles)
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
-- pub exposée : pub d'une catégorie visée autour d'une émission à fort taux contre son groupe, chaînes d'info
pub AS (
    SELECT
        cg.groupe_libelle,
        p.emission_cle,
        p.nom,
        c.chaine_nom,
        p.entreprise AS annonceur,
        p.pub_secondes
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN periode pe
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    JOIN advertising.ref_ome_categories cat ON cat.product_category_fr = p.categorie
    JOIN advertising.ref_ome_secteurs s ON s.sector_code = cat.sector_code AND s.sector_label_fr = p.secteur
    JOIN cat_groupe cg ON cg.cat_code = cat.cat_code
    JOIN cles_fort kf ON kf.emission_cle = p.emission_cle AND kf.groupe = cg.groupe
    WHERE p.jour BETWEEN pe.debut AND pe.fin
      AND p.entreprise IS NOT NULL
      AND c.est_info_continue
),
-- annonceurs du graphique pub_annonceurs_categories_visees_fort_taux
annonceurs AS (
    SELECT annonceur
    FROM pub
    GROUP BY 1
    HAVING SUM(pub_secondes) / 60 >= (SELECT pub_exposee_min_minutes FROM parametres)
),
emissions AS (
    SELECT emission_cle, MAX(nom) || ' (' || MAX(chaine_nom) || ')' AS emission
    FROM pub
    GROUP BY 1
)
SELECT
    pu.groupe_libelle AS groupe_categories,
    e.emission,
    pu.annonceur,
    ROUND((SUM(pu.pub_secondes) / 60)::numeric, 1) AS pub_exposee_min
FROM pub pu
JOIN annonceurs a USING (annonceur)
JOIN emissions e USING (emission_cle)
GROUP BY 1, 2, 3
HAVING SUM(pu.pub_secondes) > 0
ORDER BY 1, 4 DESC, 2, 3
