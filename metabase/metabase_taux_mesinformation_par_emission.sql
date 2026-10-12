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
-- une ligne par diffusion (ligne de grille x jour), en heure de Paris ; on part de la veille de {{debut}}
-- pour les programmes qui finissent après minuit
diffusions AS (
    SELECT
        g.*,
        j.jour::date AS jour,
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
-- segments de 2 min de public.keywords (start en UTC) ; la marge de 3 h couvre le décalage UTC / Paris
keywords_periode AS (
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
-- chaque segment rattaché à la ligne de grille à l'antenne à son début (jour même, ou veille pour un
-- programme qui finit après minuit) ; en cas de chevauchement, mêmes priorités que la macro program_at
env_segments AS (
    SELECT DISTINCT ON (k.id, k.start)
        d.channel_name,
        d.type,
        d.nom,
        d.rediffusion,
        k.number_of_keywords
    FROM keywords_periode k
    CROSS JOIN (VALUES (0), (1)) AS o(day_offset)
    JOIN diffusions d
      ON d.channel_name = k.channel_name
     AND d.jour = k.paris_time::date - o.day_offset
     AND k.paris_time >= d.diffusion_debut
     AND k.paris_time < d.diffusion_fin
    ORDER BY k.id, k.start, d.type = 'émission' DESC, d.grid_start DESC NULLS LAST, o.day_offset, d.start_minute DESC
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
