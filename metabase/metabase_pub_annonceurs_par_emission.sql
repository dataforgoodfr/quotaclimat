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
-- Métriques des émissions : voir metabase_taux_mesinformation_par_emission.sql (même calcul).
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
volume AS (
    SELECT
        channel_name,
        chaine,
        type,
        nom,
        rediffusion,
        COUNT(*) AS nb_diffusions,
        SUM(EXTRACT(EPOCH FROM diffusion_fin - diffusion_debut)) / 3600.0 AS volume_heures
    FROM diffusions
    WHERE diffusion_fin > diffusion_debut
    GROUP BY 1, 2, 3, 4, 5
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
env AS (
    SELECT
        channel_name,
        type,
        nom,
        rediffusion,
        SUM(number_of_keywords) * 20 / 3600.0 AS env_heures
    FROM env_segments
    GROUP BY 1, 2, 3, 4
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
        g.rediffusion
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
cas AS (
    SELECT
        channel_name,
        type,
        nom,
        rediffusion,
        COUNT(*) AS nb_cas
    FROM cas_lignes
    GROUP BY 1, 2, 3, 4
),
-- métriques de chaque émission, comme dans metabase_taux_mesinformation_par_emission.sql ; MATERIALIZED :
-- calculées une seule fois, sans quoi Postgres, qui sous-estime les jointures sur les CTE, peut les
-- recalculer pour chaque ligne de publicité
emissions AS MATERIALIZED (
    SELECT
        v.channel_name,
        v.chaine,
        v.type,
        v.nom,
        v.rediffusion,
        v.nb_diffusions,
        ROUND(v.volume_heures::numeric, 1) AS volume_heures,
        ROUND(COALESCE(e.env_heures, 0)::numeric, 2) AS env_heures,
        ROUND((100 * COALESCE(e.env_heures, 0) / v.volume_heures)::numeric, 2) AS part_env_pct,
        COALESCE(c.nb_cas, 0) AS nb_cas,
        ROUND((COALESCE(c.nb_cas, 0) / v.volume_heures)::numeric, 3) AS cas_par_heure,
        ROUND((10 * COALESCE(c.nb_cas, 0) / v.volume_heures)::numeric, 2) AS cas_pour_10h,
        -- NULL quand l'émission n'a pas parlé d'environnement sur la période
        ROUND((COALESCE(c.nb_cas, 0) / NULLIF(e.env_heures, 0))::numeric, 2) AS cas_par_heure_env
    FROM volume v
    LEFT JOIN env e
      ON e.channel_name = v.channel_name
     AND e.type = v.type
     AND e.nom = v.nom
     AND e.rediffusion = v.rediffusion
    LEFT JOIN cas c
      ON c.channel_name = v.channel_name
     AND c.type = v.type
     AND c.nom = v.nom
     AND c.rediffusion = v.rediffusion
),
-- réglages
parametres AS (
    SELECT
        10 AS fenetre_minutes,       -- distance maximale entre l'écran publicitaire et l'émission
        15 AS nb_annonceurs,         -- annonceurs gardés, par secondes de pub rattachées décroissantes
        1.0 AS env_heures_min        -- émissions gardées : au moins ce temps consacré à l'environnement
),
-- pubs de la période, avec les émissions avant / après l'écran retenues ; NOT MATERIALIZED : recopiée dans
-- les deux lectures ci-dessous plutôt qu'écrite sur disque ; occurrence_date est en UTC (timestamp sans fuseau)
pubs AS NOT MATERIALIZED (
    SELECT
        p.brand_company AS annonceur,
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
    SELECT annonceur, program_id, SUM(secondes) AS secondes
    FROM (
        SELECT annonceur, program_before_id AS program_id,
            duration_sec * CASE WHEN apres_proche THEN 0.5 ELSE 1 END AS secondes
        FROM pubs
        WHERE avant_proche
        UNION ALL
        SELECT annonceur, program_after_id,
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
        SUM(pl.secondes) AS pub_secondes
    FROM pub_par_ligne pl
    JOIN grille g ON g.id = pl.program_id
    GROUP BY 1, 2, 3, 4, 5
),
-- classement des annonceurs, puis seulement les annonceurs retenus : calculés une seule fois (MATERIALIZED)
-- et réduits avant la jointure avec les émissions
annonceurs AS MATERIALIZED (
    SELECT
        annonceur,
        SUM(pub_secondes) AS pub_secondes_total,
        RANK() OVER (ORDER BY SUM(pub_secondes) DESC) AS rang
    FROM pub_par_emission
    GROUP BY annonceur
),
pub_retenue AS MATERIALIZED (
    SELECT
        pe.*,
        a.pub_secondes_total,
        a.rang
    FROM pub_par_emission pe
    JOIN annonceurs a ON a.annonceur = pe.annonceur
    CROSS JOIN parametres pa
    WHERE a.rang <= pa.nb_annonceurs
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
    e.volume_heures,
    e.env_heures,
    e.part_env_pct,
    e.nb_cas,
    e.cas_par_heure,
    e.cas_pour_10h,
    e.cas_par_heure_env
FROM pub_retenue pe
JOIN emissions e
  ON e.channel_name = pe.channel_name
 AND e.type = pe.type
 AND e.nom = pe.nom
 AND e.rediffusion = pe.rediffusion
CROSS JOIN parametres pa
WHERE e.env_heures >= pa.env_heures_min
ORDER BY pe.rang, pe.pub_secondes DESC
