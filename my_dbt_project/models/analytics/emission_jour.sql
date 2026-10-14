{{
    config(
        materialized='table',
        schema='analytics',
        indexes=[
            {'columns': ['jour']},
            {'columns': ['emission_cle', 'jour'], 'unique': True},
        ],
    )
}}

{#- analytics.emission_jour: one row per emission and per program day (French channels): the airtime of the
    emission in the grid, the time spent on environmental subjects and the validated misinformation cases.
    Emission: a row group of analytics.program, the emission when known (Programmes Google Sheet), else the
    monitored program of program_metadata (type 'émission' / 'programme'), keyed by emission_cle. Program day:
    the day the emission starts, an emission ending after midnight counting for the day before. Precomputed for
    the Metabase questions on a period (sums of days), see metabase/ in the misinformation / advertising work.
    - volume_heures, nb_diffusions: the slots of analytics.program broadcast that day.
    - env_heures: public.keywords segments (French, with keywords), number_of_keywords windows of 20 s, like
      core_query_environmental_shares. Each segment goes to the program set at ingestion
      (keywords.program_metadata_id, like the dashboards), then to the row of that program on the air, the
      closest one within the ingestion margins: the API is queried from start - 5 min to end + 5 min
      (api_to_s3.py, epoch_margin of France) and the program reassignment accepts 6 min
      (channel_program.get_matching_program_hour), hence [start - 6 min, end + 6 min[. A segment further than
      6 min from every row of its program (inside a program split into emissions that do not cover it) is left
      out.
    - nb_cas: analytics.cas_de_mesinformation (data_item_start in UTC), matched on time to the row on the air
      with the same margins (no program id for the cases).
    Days from emission_jour_start_date (dbt var) to today (Paris). -#}

{% set start_date = var('emission_jour_start_date', '2023-01-01') %}

WITH calendrier AS (
    SELECT jour::date AS jour
    FROM generate_series(
        '{{ start_date }}'::date,
        (now() AT TIME ZONE 'Europe/Paris')::date,
        INTERVAL '1 day'
    ) AS jour
),
grille AS (
    SELECT
        p.id,
        p.program_metadata_id,
        p.channel_name,
        COALESCE(p.channel_title, p.channel_name) AS channel_title,
        p.public,
        p.infocontinue,
        p.radio,
        CASE WHEN p.emission IS NOT NULL THEN 'émission' ELSE 'programme' END AS type,
        COALESCE(p.emission, p.channel_program, p.channel_title, p.channel_name) AS nom,
        COALESCE(p.rediffusion, FALSE) AS rediffusion,
        p.weekday,
        p.start_minute,
        p.end_minute,
        p.grid_start,
        p.grid_end
    FROM {{ ref('program') }} p
    WHERE p.country = 'france'
),
grille_cle AS (
    SELECT
        g.*,
        concat_ws('|', g.channel_name, g.type, g.nom, g.rediffusion::text) AS emission_cle
    FROM grille g
),
-- slots broadcast each program day
diffusions AS (
    SELECT
        g.emission_cle,
        c.jour,
        (g.end_minute - g.start_minute) / 60.0 AS volume_heures
    FROM calendrier c
    JOIN grille_cle g
      ON g.weekday = EXTRACT(ISODOW FROM c.jour)
     AND c.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
),
-- keyword segments with their ingestion program; Paris day, weekday and minute of the day computed once
segments AS MATERIALIZED (
    SELECT
        k.id,
        k.start,
        k.program_metadata_id,
        k.number_of_keywords,
        p.paris_time::date AS jour_paris,
        EXTRACT(ISODOW FROM p.paris_time)::int AS jour_semaine,
        EXTRACT(EPOCH FROM p.paris_time::time)::float8 / 60 AS minute_jour
    FROM {{ source('public', 'keywords') }} k
    CROSS JOIN LATERAL (SELECT {{ keywords_paris_time('k') }} AS paris_time) p
    WHERE k.country = 'france'
      AND k.number_of_keywords > 0
      AND k.program_metadata_id IS NOT NULL
      AND k.start >= '{{ start_date }}'::date - 1
),
-- row of the ingestion program on the air, program day from the weekday of the program row: the same day,
-- the day before (program ending after midnight) or the day after (margin before a program starting at midnight)
segments_lignes AS (
    SELECT DISTINCT ON (s.id, s.start)
        g.emission_cle,
        m.jour,
        s.number_of_keywords
    FROM segments s
    JOIN grille_cle g ON g.program_metadata_id = s.program_metadata_id
    CROSS JOIN LATERAL (
        SELECT CASE (s.jour_semaine - g.weekday + 7) % 7 WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 6 THEN -1 END AS decalage
    ) d
    CROSS JOIN LATERAL (
        SELECT s.jour_paris - d.decalage AS jour, s.minute_jour + 1440 * d.decalage AS minute
    ) m
    WHERE m.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
      AND m.minute >= g.start_minute - 6
      AND m.minute < g.end_minute + 6
    ORDER BY s.id, s.start,
        m.minute >= g.start_minute AND m.minute < g.end_minute DESC,
        GREATEST(g.start_minute - m.minute, m.minute - g.end_minute),
        g.grid_start DESC NULLS LAST, g.start_minute DESC
),
-- validated misinformation cases, Paris time; candidate program days: the same day, the day before, the day after
cas_jour AS (
    SELECT
        c.task_aggregate_id,
        c.data_item_channel_name AS channel_name,
        o.day_offset,
        t.paris_time::date - o.day_offset AS jour,
        EXTRACT(ISODOW FROM t.paris_time::date - o.day_offset)::int AS weekday,
        EXTRACT(EPOCH FROM t.paris_time - (t.paris_time::date - o.day_offset)) / 60 AS minute
    FROM {{ ref('cas_de_mesinformation') }} c
    CROSS JOIN LATERAL (SELECT (c.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' AS paris_time) t
    CROSS JOIN (VALUES (-1), (0), (1)) AS o(day_offset)
    WHERE c.country = 'france'
),
cas_lignes AS (
    SELECT DISTINCT ON (c.task_aggregate_id)
        g.emission_cle,
        c.jour
    FROM cas_jour c
    JOIN grille_cle g
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
metriques AS (
    SELECT
        emission_cle,
        jour,
        SUM(nb_diffusions) AS nb_diffusions,
        SUM(volume_heures) AS volume_heures,
        SUM(env_heures) AS env_heures,
        SUM(nb_cas) AS nb_cas
    FROM (
        SELECT emission_cle, jour, 1 AS nb_diffusions, volume_heures, 0 AS env_heures, 0 AS nb_cas
        FROM diffusions
        UNION ALL
        SELECT emission_cle, jour, 0, 0, number_of_keywords * 20 / 3600.0, 0
        FROM segments_lignes
        UNION ALL
        SELECT emission_cle, jour, 0, 0, 0, 1
        FROM cas_lignes
    ) m
    WHERE jour >= '{{ start_date }}'::date
    GROUP BY 1, 2
),
-- descriptive columns of each emission
emissions AS (
    SELECT DISTINCT ON (emission_cle)
        emission_cle, channel_name, channel_title, public, infocontinue, radio, type, nom, rediffusion
    FROM grille_cle
    ORDER BY emission_cle, grid_end DESC
)
SELECT
    m.emission_cle,
    m.jour,
    e.channel_name,
    e.channel_title,
    e.public,
    e.infocontinue,
    e.radio,
    e.type,
    e.nom,
    e.rediffusion,
    m.nb_diffusions,
    m.volume_heures,
    m.env_heures,
    m.nb_cas::int AS nb_cas
FROM metriques m
JOIN emissions e ON e.emission_cle = m.emission_cle
