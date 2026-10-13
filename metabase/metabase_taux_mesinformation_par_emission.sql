-- Taux de mésinformation et part d'environnement par émission, chaînes françaises
-- Unité d'analyse : l'émission quand analytics.program la connaît (Google Sheet Programmes, chaînes d'info
-- en continue), sinon le programme suivi de program_metadata (colonne type)
-- Variables Metabase, type Texte, heure de Paris :
--   {{debut}} inclus, ex. 2026-09-01 ou 2026-09-01 06:00
--   {{fin}}   exclu,  ex. 2026-10-01
-- volume_heures : durée de diffusion théorique sur la période d'après la grille (analytics.program),
--   coupée aux bornes de la période
-- env_heures : temps consacré à l'environnement, même calcul que core_query_environmental_shares
--   (public.keywords.number_of_keywords fenêtres de 20 s) ; chaque segment va au programme posé à
--   l'ingestion (keywords.program_metadata_id), comme dans les dashboards, puis à l'émission de ce
--   programme à l'antenne (la plus proche pour les segments des marges d'ingestion)
-- nb_cas : cas validés (analytics.cas_de_mesinformation, 1re annotation 'Correct'), sans id de programme :
--   rattachés par leur heure, créneaux élargis comme à l'ingestion
WITH bornes AS (
    SELECT
        CAST({{debut}} AS timestamp) AS debut,
        CAST({{fin}} AS timestamp) AS fin
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
-- volume : une ligne par diffusion (ligne de grille x jour), en heure de Paris ; on part de la veille de {{debut}}
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
        k.start AT TIME ZONE 'Europe/Paris' AS paris_time
    FROM public.keywords k
    -- bornes écrites ici plutôt que lues dans bornes : keywords est partitionnée par mois, Postgres n'écarte
    -- les partitions hors période à la planification que si elles sont connues à ce moment ; sinon il estime
    -- la lecture de toutes les partitions et déclenche une compilation JIT de plusieurs dizaines de secondes
    WHERE k.start >= CAST({{debut}} AS timestamp) AT TIME ZONE 'Europe/Paris'
      AND k.start < CAST({{fin}} AS timestamp) AT TIME ZONE 'Europe/Paris'
      AND k.country = 'france'
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
    CROSS JOIN LATERAL (
        SELECT s.paris_time::date - CASE (EXTRACT(ISODOW FROM s.paris_time)::int - g.weekday + 7) % 7
            WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 6 THEN -1
        END AS jour
    ) j
    CROSS JOIN LATERAL (SELECT EXTRACT(EPOCH FROM s.paris_time - j.jour) / 60 AS minute) m
    WHERE j.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
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
    -- bornes converties en UTC et comparées à la colonne telle quelle : un index sur data_item_start reste
    -- utilisable, et les bornes sont connues dès la planification
    WHERE c.country = 'france'
      AND c.data_item_start >= (CAST({{debut}} AS timestamp) AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC'
      AND c.data_item_start < (CAST({{fin}} AS timestamp) AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC'
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
)
SELECT
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
ORDER BY cas_par_heure DESC, volume_heures DESC
