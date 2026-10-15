-- Publicité autour des émissions, par annonceur, croisée avec les métriques de mésinformation des
-- émissions (chaînes françaises) : une ligne par couple annonceur / émission.
-- Pour un nuage de points : abscisse une métrique de l'émission (ex. cas_par_heure_env), ordonnée
-- part_annonceur_pct, taille pub_secondes, couleur annonceur.
-- Variables Metabase :
--   debut  type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin    type Date, dernier jour inclus, ex. 2026-09-30
--   niveau type Texte, échelle de l'annonceur : marque, entreprise ou groupe (société mère ultime) ; à régler en
--          liste déroulante de ces trois valeurs, valeur par défaut entreprise (une autre valeur ne renvoie rien)
--   env_heures_min        type Nombre, facultatif (0.5 si vide) : émissions gardées, au moins ce temps
--                         consacré à l'environnement sur la période (heures)
--   cas_par_heure_env_min type Nombre, facultatif (0.5 si vide) : émissions gardées, au moins ce nombre de cas
--                         par heure d'environnement sur la période (0 garde aussi les émissions sans cas)
-- Lit les tables précalculées par dbt (voir leur documentation dans my_dbt_project/models/schema.yml) :
--   analytics.pub_annonceur_emission_jour : secondes de pub de chaque annonceur autour de chaque émission, par
--     jour (date de la pub en heure de Paris). Une pub compte pour l'émission qui contient tout son écran (100 %
--     de sa durée), sinon pour l'émission avant et l'émission après l'écran à moins de 15 min (50 % chacune,
--     100 % à la seule des deux assez proche) ; les parts d'un annonceur font ainsi 100 %. La fenêtre de 15 min
--     est la variable dbt ad_emission_window_minutes : la changer demande de reconstruire la table.
--     Une ligne par marque (orthographe de ad_brands), avec son entreprise et sa société mère ultime (la
--     marque elle-même quand elle n'est pas dans ad_brands ou que la valeur est 'à vérifier') : l'annonceur
--     est l'un des trois selon niveau, ses secondes la somme de celles de ses marques. Pubs à partir de
--     ad_analysis_start_date (2025-09-29 par défaut).
--   analytics.emission_jour : par émission et par jour de programme, volume de grille, temps consacré à
--     l'environnement et cas de mésinformation, mêmes calculs que metabase_taux_mesinformation_par_emission.sql
--     (programme d'ingestion des segments, marges d'ingestion de 6 min). Jour de programme : le jour où
--     l'émission commence, une émission qui finit après minuit compte pour la veille.
-- La période retient les jours de programme de debut à fin pour les métriques des émissions, les jours des pubs
-- de debut à fin pour la publicité.
-- part_annonceur_pct : secondes de pub de l'annonceur autour de l'émission / toutes ses secondes rattachées
-- à une émission sur la période.
-- Annonceurs gardés : pour chaque émission, les nb_annonceurs_par_emission avec la plus forte
-- part_annonceur_pct (rang_dans_emission), parmi ceux qui ont au moins annonceur_secondes_min de pub rattachée.
-- Métriques des émissions sur toute la période, puis (colonnes _campagne) sur la seule campagne de
-- l'annonceur : du premier au dernier jour (heure de Paris) de ses pubs rattachées à une émission, le même pour
-- toutes ses lignes ; colonnes ecart_ : campagne - période entière. Plus la campagne est courte, plus ces taux
-- reposent sur peu de cas : voir nb_cas_campagne.
-- NOT MATERIALIZED : les bornes sont recopiées dans chaque lecture comme des constantes, Postgres choisit
-- l'index sur jour dès la planification
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
-- réglages, appliqués à la lecture (la fenêtre autour des émissions est fixée dans dbt, voir plus haut)
parametres AS (
    SELECT
        15 AS nb_annonceurs_par_emission, -- par émission, annonceurs gardés par part de leur pub décroissante
        600 AS annonceur_secondes_min, -- annonceurs classés : au moins ce volume de pub rattachée sur la période
        -- émissions gardées : variables Metabase, valeur par défaut quand elles sont vides
        COALESCE([[{{env_heures_min}},]] 0.5) AS env_heures_min,
        COALESCE([[{{cas_par_heure_env_min}},]] 0.5) AS cas_par_heure_env_min,
        0.5 AS seuil_biais           -- biais_de_saisonnalite : neutre quand |ecart_cas_par_heure_env| <= ce seuil
),
-- métriques de chaque émission sur la période ; MATERIALIZED : calculées une seule fois
emissions AS MATERIALIZED (
    SELECT
        emission_cle,
        channel_name,
        chaine,
        type,
        nom,
        rediffusion,
        nb_diffusions,
        -- valeurs exactes pour les écarts, arrondies pour l'affichage
        volume_heures AS p_volume_heures,
        env_heures AS p_env_heures,
        nb_cas AS p_nb_cas,
        ROUND(volume_heures::numeric, 1) AS volume_heures,
        ROUND(env_heures::numeric, 2) AS env_heures,
        ROUND((100 * env_heures / volume_heures)::numeric, 2) AS part_env_pct,
        nb_cas,
        ROUND((nb_cas / volume_heures)::numeric, 3) AS cas_par_heure,
        ROUND((10 * nb_cas / volume_heures)::numeric, 2) AS cas_pour_10h,
        -- NULL quand l'émission n'a pas parlé d'environnement sur la période
        ROUND((nb_cas / NULLIF(env_heures, 0))::numeric, 2) AS cas_par_heure_env
    FROM (
        SELECT
            ej.emission_cle,
            MAX(ej.channel_name) AS channel_name,
            MAX(ej.channel_title) AS chaine,
            MAX(ej.type) AS type,
            MAX(ej.nom) AS nom,
            BOOL_OR(ej.rediffusion) AS rediffusion,
            SUM(ej.nb_diffusions) AS nb_diffusions,
            SUM(ej.volume_heures) AS volume_heures,
            SUM(ej.env_heures) AS env_heures,
            SUM(ej.nb_cas) AS nb_cas
        FROM analytics.emission_jour ej
        CROSS JOIN bornes b
        WHERE ej.jour BETWEEN b.debut AND b.fin
        GROUP BY ej.emission_cle
    ) t
    -- comme dans la requête par émission : seulement les émissions diffusées pendant la période
    WHERE nb_diffusions > 0
),
-- secondes de pub par annonceur (au niveau choisi) et par émission sur la période, avec les jours de la
-- première et de la dernière pub
pub_par_emission AS (
    SELECT
        CASE CAST({{niveau}} AS text)
            WHEN 'marque' THEN p.marque
            WHEN 'entreprise' THEN p.entreprise
            WHEN 'groupe' THEN p.groupe
        END AS annonceur,
        p.emission_cle,
        SUM(p.pub_secondes) AS pub_secondes,
        MIN(p.jour) AS premier_jour,
        MAX(p.jour) AS dernier_jour
    FROM analytics.pub_annonceur_emission_jour p
    CROSS JOIN bornes b
    WHERE p.jour BETWEEN b.debut AND b.fin
      AND CAST({{niveau}} AS text) IN ('marque', 'entreprise', 'groupe')
    GROUP BY 1, p.emission_cle
),
-- volume total de chaque annonceur sur la période et sa campagne : du premier au dernier jour de ses pubs
-- rattachées à une émission
annonceurs AS MATERIALIZED (
    SELECT
        annonceur,
        SUM(pub_secondes) AS pub_secondes_total,
        MIN(premier_jour) AS campagne_debut,
        MAX(dernier_jour) AS campagne_fin
    FROM pub_par_emission
    GROUP BY annonceur
),
-- pour chaque émission, les annonceurs pour qui elle pèse le plus dans leur pub totale ; le volume minimum
-- écarte les annonceurs presque absents, pour qui une seule pub peut faire 100 %
pub_retenue AS MATERIALIZED (
    SELECT *
    FROM (
        SELECT
            pe.*,
            a.pub_secondes_total,
            a.campagne_debut,
            a.campagne_fin,
            RANK() OVER (
                PARTITION BY pe.emission_cle
                ORDER BY pe.pub_secondes / a.pub_secondes_total DESC
            ) AS rang_dans_emission
        FROM pub_par_emission pe
        JOIN annonceurs a ON a.annonceur = pe.annonceur
        CROSS JOIN parametres pa
        WHERE a.pub_secondes_total >= pa.annonceur_secondes_min
    ) classes
    CROSS JOIN parametres pa
    WHERE classes.rang_dans_emission <= pa.nb_annonceurs_par_emission
),
-- métriques de l'émission sur la campagne de l'annonceur, lues dans analytics.emission_jour (index sur
-- emission_cle, jour) ; MATERIALIZED : calculées une seule fois, avant la jointure finale
campagne AS MATERIALIZED (
    SELECT
        pr.*,
        c.c_volume_heures,
        c.c_env_heures,
        c.c_nb_cas
    FROM pub_retenue pr
    CROSS JOIN LATERAL (
        SELECT
            COALESCE(SUM(ej.volume_heures), 0) AS c_volume_heures,
            COALESCE(SUM(ej.env_heures), 0) AS c_env_heures,
            COALESCE(SUM(ej.nb_cas), 0) AS c_nb_cas
        FROM analytics.emission_jour ej
        WHERE ej.emission_cle = pr.emission_cle
          AND ej.jour BETWEEN pr.campagne_debut AND pr.campagne_fin
    ) c
)
SELECT
    CAST({{niveau}} AS text) AS niveau,
    pe.annonceur,
    e.chaine,
    e.type,
    e.nom AS emission,
    e.rediffusion,
    pe.annonceur || ' / ' || e.chaine || ' / ' || e.nom AS point,
    ROUND(pe.pub_secondes::numeric, 0) AS pub_secondes,
    ROUND(pe.pub_secondes_total::numeric, 0) AS annonceur_pub_secondes_total,
    ROUND((100 * pe.pub_secondes / pe.pub_secondes_total)::numeric, 2) AS part_annonceur_pct,
    pe.rang_dans_emission,
    e.volume_heures,
    e.env_heures,
    e.part_env_pct,
    e.nb_cas,
    e.cas_par_heure,
    e.cas_pour_10h,
    e.cas_par_heure_env,
    -- mêmes métriques sur la campagne de l'annonceur (premier au dernier jour de ses pubs rattachées)
    pe.campagne_debut,
    pe.campagne_fin,
    pe.campagne_fin - pe.campagne_debut + 1 AS campagne_jours,
    ROUND(pe.c_volume_heures::numeric, 1) AS volume_heures_campagne,
    ROUND(pe.c_env_heures::numeric, 2) AS env_heures_campagne,
    ROUND((100 * pe.c_env_heures / NULLIF(pe.c_volume_heures, 0))::numeric, 2) AS part_env_pct_campagne,
    pe.c_nb_cas AS nb_cas_campagne,
    ROUND((pe.c_nb_cas / NULLIF(pe.c_volume_heures, 0))::numeric, 3) AS cas_par_heure_campagne,
    ROUND((10 * pe.c_nb_cas / NULLIF(pe.c_volume_heures, 0))::numeric, 2) AS cas_pour_10h_campagne,
    ROUND((pe.c_nb_cas / NULLIF(pe.c_env_heures, 0))::numeric, 2) AS cas_par_heure_env_campagne,
    -- écarts campagne - période entière (positif : l'émission a fait pire pendant la campagne)
    ROUND((100 * pe.c_env_heures / NULLIF(pe.c_volume_heures, 0)
        - 100 * e.p_env_heures / NULLIF(e.p_volume_heures, 0))::numeric, 2) AS ecart_part_env_pct,
    ROUND((pe.c_nb_cas / NULLIF(pe.c_volume_heures, 0)
        - e.p_nb_cas / NULLIF(e.p_volume_heures, 0))::numeric, 3) AS ecart_cas_par_heure,
    ecart.cas_par_heure_env AS ecart_cas_par_heure_env,
    -- le même écart en trois classes : la campagne tombe-t-elle sur une période où l'émission fait mieux ou pire
    -- que d'habitude ? Classée sur l'écart affiché (arrondi à 0,01), bornes incluses dans neutre ; NULL sans
    -- environnement pendant la campagne
    CASE
        WHEN ecart.cas_par_heure_env IS NULL THEN NULL
        WHEN ecart.cas_par_heure_env > pa.seuil_biais THEN 'négatif'
        WHEN ecart.cas_par_heure_env < -pa.seuil_biais THEN 'positif'
        ELSE 'neutre'
    END AS biais_de_saisonnalite
FROM campagne pe
JOIN emissions e ON e.emission_cle = pe.emission_cle
CROSS JOIN parametres pa
CROSS JOIN LATERAL (
    SELECT ROUND((pe.c_nb_cas / NULLIF(pe.c_env_heures, 0)
        - e.p_nb_cas / NULLIF(e.p_env_heures, 0))::numeric, 2) AS cas_par_heure_env
) ecart
WHERE e.env_heures >= pa.env_heures_min
  AND COALESCE(e.cas_par_heure_env, 0) >= pa.cas_par_heure_env_min
ORDER BY e.chaine, e.nom, pe.rang_dans_emission
