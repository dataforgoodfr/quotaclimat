-- Publicité autour des émissions, par annonceur, croisée avec les métriques de mésinformation des
-- émissions (chaînes françaises) : une ligne par couple annonceur / émission.
-- Pour un nuage de points : abscisse une métrique de l'émission (ex. cas_par_heure_env), ordonnée
-- part_annonceur_pct, taille pub_secondes, couleur annonceur.
-- Variables Metabase, type Date (jours en heure de Paris) :
--   debut premier jour inclus, ex. 2026-09-01
--   fin   dernier jour inclus, ex. 2026-09-30
-- Publicités : analytics.publicites (à partir de ad_analysis_start_date, 2025-09-29 par défaut), avec les
-- lignes de analytics.program avant / après l'écran publicitaire (ad_tunnel_programs). Une pub compte pour :
--   - l'émission qui contient tout son écran (inside_program) : 100 % de sa durée ;
--   - sinon l'émission avant et l'émission après l'écran, si elles sont à moins de fenetre_minutes :
--     50 % chacune, 100 % à la seule des deux qui est assez proche.
--   Les parts d'un annonceur font ainsi 100 % ; les pubs loin de toute émission (et les fragments hors
--   écran, sans tunnel) sont laissées de côté.
-- Annonceur : brand_company (société de la marque, la marque elle-même hors ad_brands) ; brand_ultimate_parent
-- pour regrouper par groupe. Pubs sans marque prédite laissées de côté.
-- part_annonceur_pct : secondes de pub de l'annonceur autour de l'émission / toutes ses secondes rattachées
-- à une émission sur la période.
-- Annonceurs gardés : pour chaque émission, les nb_annonceurs_par_emission avec la plus forte
-- part_annonceur_pct (rang_dans_emission), parmi ceux qui ont au moins annonceur_secondes_min de pub rattachée.
-- Métriques des émissions : voir metabase_taux_mesinformation_par_emission.sql (même calcul), sur toute la
-- période, puis (colonnes _campagne) sur la seule campagne de l'annonceur : du premier au dernier jour (heure de
-- Paris) de ses pubs rattachées à une émission, le même pour toutes ses lignes ; colonnes ecart_ : campagne -
-- période entière. Plus la campagne est courte, plus ces taux reposent sur peu de cas : voir nb_cas_campagne.
-- bornes de la période en heure de Paris : de debut 00:00 à fin + 1 jour 00:00 (fin incluse).
-- Variables de type Date : la conversion date -> timestamp ne dépend pas de la session, Postgres calcule les
-- bornes une seule fois dès la planification (une variable Texte convertie avec CAST serait recalculée pour
-- chaque ligne lue). NOT MATERIALIZED : la CTE est recopiée à chaque usage, les bornes y sont des constantes,
-- ce qui permet aussi d'écarter les partitions de keywords hors période dès la planification (sinon :
-- estimation sur toutes les partitions et compilation JIT de plusieurs dizaines de secondes)
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS timestamp) AS debut,
        CAST({{fin}} AS timestamp) + INTERVAL '1 day' AS fin
),
grille AS (
    SELECT
        p.id,
        p.program_metadata_id,
        p.channel_name,
        COALESCE(p.channel_title, p.channel_name) AS chaine,
        CASE WHEN p.emission IS NOT NULL THEN 'émission' ELSE 'programme' END AS type,
        COALESCE(p.emission, p.channel_program, p.channel_title, p.channel_name) AS nom,
        COALESCE(p.rediffusion, FALSE) AS rediffusion,
        p.weekday,
        p.start_minute,
        p.end_minute,
        p.grid_start,
        p.grid_end
    FROM analytics.program p
    WHERE p.country = 'france'
),
-- volume : une ligne par diffusion (ligne de grille x jour), en heure de Paris ; on part de la veille du début
-- pour les programmes qui finissent après minuit
diffusions AS (
    SELECT
        g.*,
        j.jour::date AS jour_programme,
        GREATEST(j.jour + g.start_minute * INTERVAL '1 minute', b.debut) AS diffusion_debut,
        LEAST(j.jour + g.end_minute * INTERVAL '1 minute', b.fin) AS diffusion_fin
    FROM bornes b
    CROSS JOIN LATERAL generate_series(
        date_trunc('day', b.debut) - INTERVAL '1 day', b.fin, INTERVAL '1 day'
    ) AS j(jour)
    JOIN grille g
      ON g.weekday = EXTRACT(ISODOW FROM j.jour)
     AND j.jour::date BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
),
-- segments de 2 min de public.keywords de la période avec leur programme d'ingestion ; start est un
-- timestamp with time zone en production (bornes de la période converties en instants pour le filtre) ;
-- sans program_metadata_id, un segment est hors des dashboards et laissé de côté
segments AS MATERIALIZED (
    SELECT
        k.id,
        k.start,
        k.program_metadata_id,
        k.number_of_keywords,
        -- jour, jour de semaine et minute dans la journée (heure de Paris) calculés une fois par segment
        p.paris_time::date AS jour_paris,
        EXTRACT(ISODOW FROM p.paris_time)::int AS jour_semaine,
        EXTRACT(EPOCH FROM p.paris_time::time)::float8 / 60 AS minute_jour
    FROM bornes b
    JOIN public.keywords k
      ON k.start >= b.debut AT TIME ZONE 'Europe/Paris'
     AND k.start < b.fin AT TIME ZONE 'Europe/Paris'
    CROSS JOIN LATERAL (SELECT k.start AT TIME ZONE 'Europe/Paris' AS paris_time) p
    WHERE k.country = 'france'
      AND k.number_of_keywords > 0
      AND k.program_metadata_id IS NOT NULL
),
-- ligne de grille du programme d'ingestion : jour du programme d'après son jour de semaine (le jour même,
-- la veille pour un programme qui finit après minuit, le lendemain pour la marge avant un programme qui
-- commence à minuit), puis l'émission qui contient le segment, sinon la plus proche
env_segments AS (
    SELECT DISTINCT ON (s.id, s.start)
        g.channel_name,
        g.type,
        g.nom,
        g.rediffusion,
        m.jour AS jour_programme,
        s.number_of_keywords
    FROM segments s
    JOIN grille g ON g.program_metadata_id = s.program_metadata_id
    -- décalage entre le jour du segment et le jour du programme : 0, 1 (programme de la veille qui finit après
    -- minuit) ou -1 (marge avant un programme du lendemain qui commence à minuit)
    CROSS JOIN LATERAL (
        SELECT CASE (s.jour_semaine - g.weekday + 7) % 7 WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 6 THEN -1 END AS decalage
    ) d
    CROSS JOIN LATERAL (
        SELECT s.jour_paris - d.decalage AS jour, s.minute_jour + 1440 * d.decalage AS minute
    ) m
    WHERE m.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
      -- seulement les lignes à moins de 6 min (marges d'ingestion) : moins de lignes à trier ; les émissions
      -- d'un programme découpé le couvrent sans trou, chaque segment en garde au moins une
      AND m.minute >= g.start_minute - 6
      AND m.minute < g.end_minute + 6
    ORDER BY s.id, s.start,
        m.minute >= g.start_minute AND m.minute < g.end_minute DESC,
        GREATEST(g.start_minute - m.minute, m.minute - g.end_minute),
        g.grid_start DESC NULLS LAST, g.start_minute DESC
),
-- cas de la période ; data_item_start (Label Studio) est en UTC, comme dans la macro dbt program_at
cas_periode AS (
    SELECT
        c.task_aggregate_id,
        c.data_item_channel_name AS channel_name,
        (c.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' AS paris_time
    FROM analytics.cas_de_mesinformation c
    CROSS JOIN bornes b
    -- bornes converties en UTC et comparées à la colonne telle quelle : un index sur data_item_start reste
    -- utilisable
    WHERE c.country = 'france'
      AND c.data_item_start >= (b.debut AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC'
      AND c.data_item_start < (b.fin AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC'
),
-- jour de grille et minute dans ce jour : le jour même, la veille pour un programme de la veille qui finit
-- après minuit, le lendemain pour la marge avant un programme qui commence à minuit
cas_jour AS (
    SELECT
        c.task_aggregate_id,
        c.channel_name,
        o.day_offset,
        c.paris_time::date - o.day_offset AS jour,
        EXTRACT(ISODOW FROM c.paris_time::date - o.day_offset)::int AS weekday,
        EXTRACT(EPOCH FROM c.paris_time - (c.paris_time::date - o.day_offset)) / 60 AS minute
    FROM cas_periode c
    CROSS JOIN (VALUES (-1), (0), (1)) AS o(day_offset)
),
-- créneaux élargis comme à l'ingestion, pour suivre la même règle que les segments : API de début - 5 min
-- à fin + 5 min incluse (api_to_s3.py, epoch_margin de la France), réattribution à 6 min
-- (channel_program.get_matching_program_hour), soit [début - 6 min, fin + 6 min[ ; priorité à la ligne qui
-- contient vraiment le cas, puis à la plus proche, puis mêmes priorités que la macro program_at
cas_lignes AS (
    SELECT DISTINCT ON (c.task_aggregate_id)
        g.channel_name,
        g.type,
        g.nom,
        g.rediffusion,
        c.jour AS jour_programme
    FROM cas_jour c
    JOIN grille g
      ON g.channel_name = c.channel_name
     AND g.weekday = c.weekday
     AND c.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
     AND c.minute >= g.start_minute - 6
     AND c.minute < g.end_minute + 6
    ORDER BY c.task_aggregate_id,
        c.minute >= g.start_minute AND c.minute < g.end_minute DESC,
        GREATEST(g.start_minute - c.minute, c.minute - g.end_minute),
        g.type = 'émission' DESC, g.grid_start DESC NULLS LAST, c.day_offset, g.start_minute DESC
),
-- volume, temps d'environnement et cas par émission et par jour de programme, pour les métriques sur la
-- campagne de chaque annonceur (mêmes rattachements que les métriques sur toute la période)
metriques_jour AS MATERIALIZED (
    SELECT channel_name, type, nom, rediffusion, jour_programme,
        MAX(chaine) AS chaine,
        SUM(nb_diffusions) AS nb_diffusions,
        SUM(volume_heures) AS volume_heures,
        SUM(env_heures) AS env_heures,
        SUM(nb_cas) AS nb_cas
    FROM (
        SELECT channel_name, type, nom, rediffusion, jour_programme, chaine, 1 AS nb_diffusions,
            EXTRACT(EPOCH FROM diffusion_fin - diffusion_debut) / 3600.0 AS volume_heures, 0 AS env_heures, 0 AS nb_cas
        FROM diffusions
        WHERE diffusion_fin > diffusion_debut
        UNION ALL
        SELECT channel_name, type, nom, rediffusion, jour_programme, NULL, 0, 0, number_of_keywords * 20 / 3600.0, 0
        FROM env_segments
        UNION ALL
        SELECT channel_name, type, nom, rediffusion, jour_programme, NULL, 0, 0, 0, 1
        FROM cas_lignes
    ) m
    GROUP BY 1, 2, 3, 4, 5
),
-- cumuls jour après jour par émission, sur un calendrier sans trou (de la veille du début au lendemain de la
-- fin) : la somme sur une campagne est le cumul de son dernier jour moins celui de la veille de son premier
metriques_cumul AS MATERIALIZED (
    SELECT
        -- clé d'émission en une colonne : Postgres estime mal les jointures sur 4 colonnes de CTE
        concat_ws('|', k.channel_name, k.type, k.nom, k.rediffusion) AS cle,
        j.jour,
        SUM(COALESCE(m.volume_heures, 0)) OVER w AS volume_heures,
        SUM(COALESCE(m.env_heures, 0)) OVER w AS env_heures,
        SUM(COALESCE(m.nb_cas, 0)) OVER w AS nb_cas
    FROM (SELECT DISTINCT channel_name, type, nom, rediffusion FROM metriques_jour) k
    CROSS JOIN bornes b
    CROSS JOIN LATERAL generate_series(b.debut - INTERVAL '1 day', b.fin, INTERVAL '1 day') AS c(jour_ts)
    CROSS JOIN LATERAL (SELECT c.jour_ts::date AS jour) j
    LEFT JOIN metriques_jour m
      ON m.channel_name = k.channel_name
     AND m.type = k.type
     AND m.nom = k.nom
     AND m.rediffusion = k.rediffusion
     AND m.jour_programme = j.jour
    WINDOW w AS (PARTITION BY k.channel_name, k.type, k.nom, k.rediffusion ORDER BY j.jour)
),
-- métriques de chaque émission, comme dans metabase_taux_mesinformation_par_emission.sql ; MATERIALIZED :
-- calculées une seule fois, sans quoi Postgres, qui sous-estime les jointures sur les CTE, peut les
-- recalculer pour chaque ligne de publicité
emissions AS MATERIALIZED (
    SELECT
        channel_name,
        chaine,
        type,
        nom,
        rediffusion,
        nb_diffusions,
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
            channel_name, type, nom, rediffusion,
            MAX(chaine) AS chaine,
            SUM(nb_diffusions) AS nb_diffusions,
            SUM(volume_heures) AS volume_heures,
            SUM(env_heures) AS env_heures,
            SUM(nb_cas) AS nb_cas
        FROM metriques_jour
        GROUP BY 1, 2, 3, 4
    ) t
    -- comme dans la requête par émission : seulement les émissions diffusées pendant la période
    WHERE nb_diffusions > 0
),
-- réglages
parametres AS (
    SELECT
        10 AS fenetre_minutes,       -- distance maximale entre l'écran publicitaire et l'émission
        5 AS nb_annonceurs_par_emission, -- par émission, annonceurs gardés par part de leur pub décroissante
        600 AS annonceur_secondes_min, -- annonceurs classés : au moins ce volume de pub rattachée sur la période
        1.0 AS env_heures_min,       -- émissions gardées : au moins ce temps consacré à l'environnement
        0 AS cas_par_heure_env_min,  -- émissions gardées : au moins ce nombre de cas par heure d'environnement
                                     -- (sur toute la période ; 0 garde tout, y compris les émissions sans cas)
        2 AS pas_ecart_arrondi       -- pas de ecart_cas_par_heure_env_arrondi : multiple de ce pas le plus proche
),
-- pubs de la période, avec les émissions avant / après l'écran retenues ; NOT MATERIALIZED : recopiée dans
-- les deux lectures ci-dessous plutôt qu'écrite sur disque ; occurrence_date est en UTC (timestamp sans fuseau)
pubs AS NOT MATERIALIZED (
    SELECT
        p.brand_company AS annonceur,
        ((p.occurrence_date AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::date AS jour_pub,
        p.duration_sec,
        p.program_before_id,
        p.program_after_id,
        p.program_before_id IS NOT NULL
            AND (COALESCE(p.inside_program, FALSE) OR p.program_before_gap_sec <= pa.fenetre_minutes * 60) AS avant_proche,
        NOT COALESCE(p.inside_program, FALSE)
            AND p.program_after_id IS NOT NULL
            AND p.program_after_gap_sec <= pa.fenetre_minutes * 60 AS apres_proche
    FROM analytics.publicites p
    CROSS JOIN bornes b
    CROSS JOIN parametres pa
    -- bornes converties en UTC et comparées à la colonne telle quelle : un index sur occurrence_date reste
    -- utilisable
    WHERE p.brand_company IS NOT NULL
      AND p.occurrence_date >= (b.debut AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC'
      AND p.occurrence_date < (b.fin AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC'
),
-- secondes par annonceur et par ligne de grille, agrégées avant de rejoindre la grille (clé étroite)
pub_par_ligne AS (
    SELECT annonceur, program_id, SUM(secondes) AS secondes,
        MIN(jour_pub) AS premier_jour, MAX(jour_pub) AS dernier_jour
    FROM (
        SELECT annonceur, program_before_id AS program_id, jour_pub,
            duration_sec * CASE WHEN apres_proche THEN 0.5 ELSE 1 END AS secondes
        FROM pubs
        WHERE avant_proche
        UNION ALL
        SELECT annonceur, program_after_id, jour_pub,
            duration_sec * CASE WHEN avant_proche THEN 0.5 ELSE 1 END
        FROM pubs
        WHERE apres_proche
    ) pe
    GROUP BY 1, 2
),
-- même regroupement que les métriques : émission, ou programme quand l'émission n'est pas connue
pub_par_emission AS (
    SELECT
        pl.annonceur,
        g.channel_name,
        g.type,
        g.nom,
        g.rediffusion,
        SUM(pl.secondes) AS pub_secondes,
        MIN(pl.premier_jour) AS premier_jour,
        MAX(pl.dernier_jour) AS dernier_jour
    FROM pub_par_ligne pl
    JOIN grille g ON g.id = pl.program_id
    GROUP BY 1, 2, 3, 4, 5
),
-- volume total de chaque annonceur sur la période et sa campagne : du premier au dernier jour (heure de Paris)
-- de ses pubs rattachées à une émission ; calculés une seule fois (MATERIALIZED)
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
                PARTITION BY pe.channel_name, pe.type, pe.nom, pe.rediffusion
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
-- métriques de l'émission sur la campagne de l'annonceur, et sur toute la période (non arrondies, pour les
-- écarts), lues dans les cumuls ; MATERIALIZED : calculées une seule fois, avant la jointure finale
campagne AS MATERIALIZED (
    SELECT
        pe.*,
        t.volume_heures AS p_volume_heures,
        t.env_heures AS p_env_heures,
        t.nb_cas AS p_nb_cas,
        f.volume_heures - COALESCE(d.volume_heures, 0) AS c_volume_heures,
        f.env_heures - COALESCE(d.env_heures, 0) AS c_env_heures,
        f.nb_cas - COALESCE(d.nb_cas, 0) AS c_nb_cas
    FROM pub_retenue pe
    CROSS JOIN bornes b
    CROSS JOIN LATERAL (SELECT concat_ws('|', pe.channel_name, pe.type, pe.nom, pe.rediffusion) AS cle) k
    -- cumul au dernier jour du calendrier : toute la période
    JOIN metriques_cumul t ON t.cle = k.cle AND t.jour = b.fin::date
    -- cumul au dernier jour de la campagne
    JOIN metriques_cumul f ON f.cle = k.cle AND f.jour = pe.campagne_fin
    -- cumul la veille du premier jour de la campagne
    LEFT JOIN metriques_cumul d ON d.cle = k.cle AND d.jour = pe.campagne_debut - 1
)
SELECT
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
    ROUND(COALESCE(pe.c_env_heures, 0)::numeric, 2) AS env_heures_campagne,
    ROUND((100 * COALESCE(pe.c_env_heures, 0) / NULLIF(pe.c_volume_heures, 0))::numeric, 2) AS part_env_pct_campagne,
    COALESCE(pe.c_nb_cas, 0) AS nb_cas_campagne,
    ROUND((COALESCE(pe.c_nb_cas, 0) / NULLIF(pe.c_volume_heures, 0))::numeric, 3) AS cas_par_heure_campagne,
    ROUND((10 * COALESCE(pe.c_nb_cas, 0) / NULLIF(pe.c_volume_heures, 0))::numeric, 2) AS cas_pour_10h_campagne,
    ROUND((COALESCE(pe.c_nb_cas, 0) / NULLIF(pe.c_env_heures, 0))::numeric, 2) AS cas_par_heure_env_campagne,
    -- écarts campagne - période entière (positif : l'émission a fait pire pendant la campagne)
    ROUND((100 * COALESCE(pe.c_env_heures, 0) / NULLIF(pe.c_volume_heures, 0)
        - 100 * pe.p_env_heures / NULLIF(pe.p_volume_heures, 0))::numeric, 2) AS ecart_part_env_pct,
    ROUND((COALESCE(pe.c_nb_cas, 0) / NULLIF(pe.c_volume_heures, 0)
        - pe.p_nb_cas / NULLIF(pe.p_volume_heures, 0))::numeric, 3) AS ecart_cas_par_heure,
    ROUND((COALESCE(pe.c_nb_cas, 0) / NULLIF(pe.c_env_heures, 0)
        - pe.p_nb_cas / NULLIF(pe.p_env_heures, 0))::numeric, 2) AS ecart_cas_par_heure_env,
    -- même écart arrondi au multiple de pas_ecart_arrondi le plus proche (pas de 2 : 0 de -1 à 1, 2 de 1 à 3…),
    -- une valeur à mi-chemin va au multiple le plus éloigné de 0 (1 donne 2, -1 donne -2)
    pa.pas_ecart_arrondi * ROUND(((COALESCE(pe.c_nb_cas, 0) / NULLIF(pe.c_env_heures, 0)
        - pe.p_nb_cas / NULLIF(pe.p_env_heures, 0)) / pa.pas_ecart_arrondi)::numeric) AS ecart_cas_par_heure_env_arrondi
FROM campagne pe
JOIN emissions e
  ON e.channel_name = pe.channel_name
 AND e.type = pe.type
 AND e.nom = pe.nom
 AND e.rediffusion = pe.rediffusion
CROSS JOIN parametres pa
WHERE e.env_heures >= pa.env_heures_min
  AND COALESCE(e.cas_par_heure_env, 0) >= pa.cas_par_heure_env_min
ORDER BY e.chaine, e.nom, pe.rang_dans_emission
