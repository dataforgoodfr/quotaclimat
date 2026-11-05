-- Spots publicitaires d'un annonceur (chaînes françaises), une ligne par spot, pour un tableau : de quoi voir et
-- écouter les publicités derrière les parts de metabase_pub_repartition_secteurs.sql. Sur le tableau de bord, un
-- clic sur un secteur ou un sous-secteur du camembert remplit les variables secteur et categorie.
-- Variables Metabase :
--   debut      type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin        type Date, dernier jour inclus, ex. 2026-09-30
--   marque     type Filtre de champ sur Curated > Publicités diffusées > Marque, sélection multiple
--   entreprise type Filtre de champ sur Curated > Publicités diffusées > Entreprise, sélection multiple
--   groupe     type Filtre de champ sur Curated > Publicités diffusées > Société mère, sélection multiple
--              les trois facultatifs et cumulés ; tous vides : tous les annonceurs
--   secteur    type Texte, facultatif : secteur exact, tel qu'affiché par le camembert (« Non classé » pour les
--              spots sans secteur)
--   categorie  type Texte, facultatif : catégorie de produit (sous-secteur) exacte, même règle
-- Lit curated.publicite (une ligne par diffusion), advertising.ad (transcription du spot) et
-- advertising.ad_screenshot (mosaïque d'images du spot). Même périmètre que le camembert
-- (advertising.pub_annonceur_emission_jour) : seules comptent les diffusions rattachées à une émission suivie,
-- c'est-à-dire dans une émission, ou à moins de 15 min de la fin de l'émission d'avant ou du début de celle
-- d'après (variable dbt ad_emission_window_minutes, 15 par défaut : à reporter ici si elle change).
-- Colonnes :
--   apercu : URL de la mosaïque d'images du spot (à afficher en image), vide quand elle n'est pas encore faite ;
--   marque, entreprise, secteur, categorie : classement du spot ;
--   transcription : texte du spot, transcrit automatiquement ;
--   duree_sec : durée du spot ;
--   nb_diffusions, pub_secondes : diffusions du spot et leur durée cumulée sur la période ;
--   part_pct : part du spot dans les secondes de toutes les lignes du tableau (annonceur, secteur et catégorie
--     choisis) ;
--   premier_jour, dernier_jour : première et dernière diffusion sur la période ;
--   chaines : chaînes de diffusion, par nombre de diffusions décroissant ;
--   lien_visualiseur, lien_mediatree : dernière diffusion du spot dans le visualiseur de publicités et dans le
--     lecteur Mediatree (vide après un an) ; lien_sauvegarde : audio ou vidéo sauvegardés du spot ;
--   spot_id : identifiant du spot.
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
-- diffusions de l'annonceur choisi rattachées à une émission ; table sans alias : les filtres de champ de
-- Metabase la nomment en entier
diffusions AS (
    SELECT
        curated.publicite.spot_id,
        curated.publicite.marque,
        curated.publicite.entreprise,
        COALESCE(curated.publicite.secteur, 'Non classé') AS secteur,
        COALESCE(curated.publicite.categorie_produit, 'Non classé') AS categorie,
        curated.publicite.duree_sec,
        curated.publicite.jour,
        curated.publicite.debut_paris,
        curated.publicite.chaine_nom,
        curated.publicite.lien_visualiseur,
        curated.publicite.lien_mediatree,
        curated.publicite.lien_sauvegarde
    FROM curated.publicite
    CROSS JOIN bornes b
    WHERE curated.publicite.jour BETWEEN b.debut AND b.fin
      AND (
          (curated.publicite.programme_avant IS NOT NULL
              AND (curated.publicite.est_dans_programme OR curated.publicite.ecart_programme_avant_sec <= 15 * 60))
          OR (NOT COALESCE(curated.publicite.est_dans_programme, FALSE)
              AND curated.publicite.programme_apres IS NOT NULL
              AND curated.publicite.ecart_programme_apres_sec <= 15 * 60)
      )
      AND {{marque}}
      AND {{entreprise}}
      AND {{groupe}}
),
choisies AS (
    SELECT d.*
    FROM diffusions d
    WHERE TRUE
      [[AND d.secteur = CAST({{secteur}} AS text)]]
      [[AND d.categorie = CAST({{categorie}} AS text)]]
),
par_chaine AS (
    SELECT spot_id, chaine_nom, COUNT(*) AS nb
    FROM choisies
    GROUP BY 1, 2
),
par_spot AS (
    SELECT
        c.spot_id,
        MAX(c.marque) AS marque,
        MAX(c.entreprise) AS entreprise,
        MAX(c.secteur) AS secteur,
        MAX(c.categorie) AS categorie,
        MAX(c.duree_sec) AS duree_sec,
        COUNT(*) AS nb_diffusions,
        SUM(c.duree_sec) AS pub_secondes,
        MIN(c.jour) AS premier_jour,
        MAX(c.jour) AS dernier_jour,
        -- liens de la dernière diffusion
        (ARRAY_AGG(c.lien_visualiseur ORDER BY c.debut_paris DESC))[1] AS lien_visualiseur,
        (ARRAY_AGG(c.lien_mediatree ORDER BY c.debut_paris DESC))[1] AS lien_mediatree,
        MAX(c.lien_sauvegarde) AS lien_sauvegarde
    FROM choisies c
    GROUP BY c.spot_id
)
SELECT
    s.image_url AS apercu,
    ps.marque,
    ps.entreprise,
    ps.secteur,
    ps.categorie,
    a.transcript AS transcription,
    ROUND(ps.duree_sec::numeric, 0) AS duree_sec,
    ps.nb_diffusions,
    ROUND(ps.pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((100 * ps.pub_secondes / NULLIF(SUM(ps.pub_secondes) OVER (), 0))::numeric, 2) AS part_pct,
    ps.premier_jour,
    ps.dernier_jour,
    (SELECT STRING_AGG(pc.chaine_nom, ', ' ORDER BY pc.nb DESC, pc.chaine_nom)
     FROM par_chaine pc WHERE pc.spot_id = ps.spot_id) AS chaines,
    ps.lien_visualiseur,
    ps.lien_mediatree,
    ps.lien_sauvegarde,
    ps.spot_id
FROM par_spot ps
LEFT JOIN advertising.ad a ON a.id = ps.spot_id
LEFT JOIN advertising.ad_screenshot s
    ON s.target_type = 'ad' AND s.target_id = ps.spot_id AND s.status = 'done'
-- spots les plus diffusés (en temps) d'abord
ORDER BY ps.pub_secondes DESC, ps.spot_id
