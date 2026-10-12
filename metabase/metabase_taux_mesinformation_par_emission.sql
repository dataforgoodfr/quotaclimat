-- Taux de mésinformation et part d'environnement par émission, chaînes françaises
-- Unité d'analyse : l'émission quand analytics.program la connaît (Google Sheet Programmes, chaînes d'info
-- en continue), sinon le programme suivi de program_metadata (colonne type)
-- Variables Metabase, type Texte, heure de Paris :
--   {{debut}} inclus, ex. 2026-09-01 ou 2026-09-01 06:00
--   {{fin}}   exclu,  ex. 2026-10-01
-- volume_heures : durée de diffusion théorique sur la période d'après la grille (analytics.program),
--   coupée aux bornes de la période
-- env_heures : temps consacré à l'environnement, même calcul que core_query_environmental_shares
--   (public.keywords.number_of_keywords fenêtres de 20 s), segments rattachés à la ligne de grille à l'antenne
-- nb_cas : cas validés (analytics.cas_de_mesinformation, 1re annotation 'Correct'), via leur program_id
WITH bornes AS (
    SELECT
        CAST({{debut}} AS timestamp) AS debut,
        CAST({{fin}} AS timestamp) AS fin
),
grille AS (
    SELECT
        p.id,
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
-- segments de 2 min de public.keywords (start en UTC) de la période, en heure de Paris ; la marge de 3 h
-- du pré-filtre couvre le décalage UTC / Paris
keywords_periode AS MATERIALIZED (
    SELECT
        k.id,
        k.start,
        k.channel_name,
        k.number_of_keywords,
        (k.start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' AS paris_time
    FROM bornes b
    JOIN public.keywords k
      ON k.start >= b.debut - INTERVAL '3 hours'
     AND k.start < b.fin + INTERVAL '3 hours'
    WHERE k.country = 'france'
      AND k.number_of_keywords > 0
),
-- jour de grille et minute dans ce jour de chaque segment : le jour même, et aussi la veille quand le
-- segment est assez tôt pour tomber dans un programme de la veille qui finit après minuit
keywords_jour AS MATERIALIZED (
    SELECT
        k.id,
        k.start,
        k.channel_name,
        k.number_of_keywords,
        o.day_offset,
        k.paris_time::date - o.day_offset AS jour,
        EXTRACT(ISODOW FROM k.paris_time::date - o.day_offset)::int AS weekday,
        EXTRACT(EPOCH FROM k.paris_time - (k.paris_time::date - o.day_offset)) / 60 AS minute
    FROM keywords_periode k
    CROSS JOIN bornes b
    CROSS JOIN (VALUES (0), (1)) AS o(day_offset)
    WHERE k.paris_time >= b.debut
      AND k.paris_time < b.fin
      AND (o.day_offset = 0
           OR EXTRACT(EPOCH FROM k.paris_time - k.paris_time::date) / 60 + 1440
              < (SELECT MAX(end_minute) FROM grille))
),
-- chaque segment rattaché à la ligne de grille à l'antenne à son début ; en cas de chevauchement, mêmes
-- priorités que la macro program_at
env_segments AS (
    SELECT DISTINCT ON (k.id, k.start)
        g.channel_name,
        g.type,
        g.nom,
        g.rediffusion,
        k.number_of_keywords
    FROM keywords_jour k
    JOIN grille g
      ON g.channel_name = k.channel_name
     AND g.weekday = k.weekday
     AND k.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
     AND k.minute >= g.start_minute
     AND k.minute < g.end_minute
    ORDER BY k.id, k.start, g.type = 'émission' DESC, g.grid_start DESC NULLS LAST, k.day_offset, g.start_minute DESC
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
-- data_item_start est en UTC, converti en heure de Paris comme dans la macro program_at
cas AS (
    SELECT
        g.channel_name,
        g.type,
        g.nom,
        g.rediffusion,
        COUNT(*) AS nb_cas
    FROM analytics.cas_de_mesinformation c
    CROSS JOIN bornes b
    JOIN grille g ON g.id = c.program_id
    WHERE (c.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' >= b.debut
      AND (c.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' < b.fin
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
