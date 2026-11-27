-- Répartition par secteur et sous-secteur (catégorie de produit) de la pub d'un annonceur (chaînes françaises),
-- pour un camembert à deux niveaux : dimensions secteur puis categorie, métrique pub_secondes. Pendant de
-- metabase_pub_repartition_emissions.sql, avec les mêmes filtres d'annonceur.
-- Variables Metabase :
--   debut      type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin        type Date, dernier jour inclus, ex. 2026-09-30
--   marque     type Filtre de champ sur Advertising > Pub Annonceur Emission Jour > Marque, sélection multiple
--   entreprise type Filtre de champ sur Advertising > Pub Annonceur Emission Jour > Entreprise, sélection multiple
--   groupe     type Filtre de champ sur Advertising > Pub Annonceur Emission Jour > Groupe, sélection multiple
--              les trois facultatifs et cumulés (ex. entreprise seule : toutes ses marques) ; tous vides : toute
--              la pub, tous annonceurs confondus
-- Lit advertising.pub_annonceur_emission_jour, précalculée par dbt : secondes de pub de chaque marque autour de
-- chaque émission, par jour (date de la pub en heure de Paris), avec le secteur et la catégorie de produit
-- prédits de ses pubs. Seule la pub rattachée à une émission suivie compte (même périmètre que
-- metabase_pub_repartition_emissions.sql).
-- Colonnes :
--   secteur, categorie : secteur et catégorie de produit prédits ; « Non classé » quand la prédiction manque ;
--   pub_secondes : secondes de pub de l'annonceur dans ce secteur et cette catégorie sur la période ;
--   part_pct : part de cette catégorie dans toute la pub de l'annonceur sur la période ;
--   part_secteur_pct : part du secteur entier dans toute la pub de l'annonceur (même valeur sur chaque ligne du
--     secteur) ;
--   nb_marques : marques de l'annonceur dans cette catégorie ; marques : leurs noms, par secondes décroissantes.
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
-- secondes par secteur, catégorie et marque ; table sans alias : les filtres de champ de Metabase la nomment en
-- entier
par_marque AS (
    SELECT
        COALESCE(advertising.pub_annonceur_emission_jour.secteur, 'Non classé') AS secteur,
        COALESCE(advertising.pub_annonceur_emission_jour.categorie, 'Non classé') AS categorie,
        advertising.pub_annonceur_emission_jour.marque,
        SUM(advertising.pub_annonceur_emission_jour.pub_secondes) AS pub_secondes
    FROM advertising.pub_annonceur_emission_jour
    CROSS JOIN bornes b
    WHERE advertising.pub_annonceur_emission_jour.jour BETWEEN b.debut AND b.fin
      AND {{marque}}
      AND {{entreprise}}
      AND {{groupe}}
    GROUP BY 1, 2, 3
),
par_categorie AS (
    SELECT
        secteur,
        categorie,
        SUM(pub_secondes) AS pub_secondes,
        COUNT(*) AS nb_marques,
        STRING_AGG(marque, ', ' ORDER BY pub_secondes DESC, marque) AS marques
    FROM par_marque
    GROUP BY 1, 2
),
totaux AS (
    SELECT
        pc.*,
        SUM(pc.pub_secondes) OVER (PARTITION BY pc.secteur) AS pub_secondes_secteur,
        SUM(pc.pub_secondes) OVER () AS pub_secondes_total
    FROM par_categorie pc
)
SELECT
    secteur,
    categorie,
    ROUND(pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * pub_secondes / NULLIF(pub_secondes_total, 0))::numeric, 2) AS part_pct,
    ROUND((100 * pub_secondes_secteur / NULLIF(pub_secondes_total, 0))::numeric, 2) AS part_secteur_pct,
    nb_marques,
    marques
FROM totaux
-- secteurs par secondes décroissantes, puis leurs catégories
ORDER BY pub_secondes_secteur DESC, secteur, pub_secondes DESC, categorie
