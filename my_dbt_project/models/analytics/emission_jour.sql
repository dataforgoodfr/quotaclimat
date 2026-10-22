{{
    config(
        materialized='table',
        schema='public',
        indexes=[
            {'columns': ['jour']},
            {'columns': ['emission_cle', 'jour'], 'unique': True},
        ],
    )
}}

{#- public.emission_jour: one row per emission and per program day (French channels): the airtime of the
    emission in the grid, the time spent on environmental subjects and the validated misinformation cases.
    Emission: a row group of analytics.program, the emission when known (Programmes Google Sheet), else the
    monitored program of program_metadata (type 'émission' / 'programme'), keyed by emission_cle. Program day:
    the day the emission starts, an emission ending after midnight counting for the day before. Precomputed for
    the Metabase questions on a period (sums of days), see metabase/ in the misinformation / advertising work.
    - volume_heures, nb_diffusions: the slots of analytics.program broadcast that day.
    - env_heures: public.keywords segments (French, with keywords), number_of_keywords windows of 20 s, like
      core_query_environmental_shares; climat_heures... ressources_solutions_heures: the same with the windows of
      each subject and theme (number_of_keywords_climat, number_of_changement_climatique_constat...). A window
      can hold several subjects: the subjects do not add up to env_heures. Each segment goes to the program set
      at ingestion (keywords.program_metadata_id, like the dashboards; before 2025 the segments have no
      program_metadata_id: the program of the same channel and channel_program, like the dashboards), then to
      the row of that program on the air, the
      closest one within the ingestion margins: the API is queried from start - 5 min to end + 5 min
      (api_to_s3.py, epoch_margin of France) and the program reassignment accepts 6 min
      (channel_program.get_matching_program_hour), hence [start - 6 min, end + 6 min[. A segment further than
      6 min from every row of its program (inside a program split into emissions that do not cover it) is left
      out.
    - nb_cas: analytics.cas_de_mesinformation, matched on time to the row on the air with the same margins (no
      program id for the cases), see analytics.cas_de_mesinformation.
    - chaine_captee: the channel was captured that day (public.time_monitored minutes, a keyword segment
      before time_monitored exists, or a validated case): false for an outage or a channel no longer broadcast but still in the grid,
      whose airtime would count as 0 % environment.
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
        p.channel_program,
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
        k.channel_name,
        k.channel_program,
        k.number_of_keywords,
        k.number_of_keywords_climat,
        k.number_of_keywords_biodiversite,
        k.number_of_keywords_ressources,
        k.number_of_changement_climatique_constat,
        k.number_of_changement_climatique_causes_directes,
        k.number_of_changement_climatique_consequences,
        k.number_of_attenuation_climatique_solutions_directes,
        k.number_of_adaptation_climatique_solutions_directes,
        k.number_of_biodiversite_concepts_generaux,
        k.number_of_biodiversite_causes_directes,
        k.number_of_biodiversite_consequences,
        k.number_of_biodiversite_solutions_directes,
        k.number_of_ressources,
        k.number_of_ressources_solutions,
        p.paris_time::date AS jour_paris,
        EXTRACT(ISODOW FROM p.paris_time)::int AS jour_semaine,
        EXTRACT(EPOCH FROM p.paris_time::time)::float8 / 60 AS minute_jour
    FROM {{ source('public', 'keywords') }} k
    CROSS JOIN LATERAL (SELECT {{ keywords_paris_time('k') }} AS paris_time) p
    WHERE k.country = 'france'
      AND k.number_of_keywords > 0
      AND k.start >= '{{ start_date }}'::date - 1
),
-- candidate rows of the ingestion program: by program_metadata_id, else (segments before 2025) by channel and
-- channel_program, as core_query_environmental_shares
segments_programmes AS (
    SELECT s.*, g.emission_cle, g.weekday, g.start_minute, g.end_minute, g.grid_start, g.grid_end
    FROM segments s
    JOIN grille_cle g ON g.program_metadata_id = s.program_metadata_id
    UNION ALL
    SELECT s.*, g.emission_cle, g.weekday, g.start_minute, g.end_minute, g.grid_start, g.grid_end
    FROM segments s
    JOIN grille_cle g ON g.channel_name = s.channel_name AND g.channel_program = s.channel_program
    WHERE s.program_metadata_id IS NULL
),
-- row of the ingestion program on the air, program day from the weekday of the program row: the same day,
-- the day before (program ending after midnight) or the day after (margin before a program starting at midnight)
segments_lignes AS (
    SELECT DISTINCT ON (s.id, s.start)
        g.emission_cle,
        m.jour,
        s.number_of_keywords_climat,
        s.number_of_keywords_biodiversite,
        s.number_of_keywords_ressources,
        s.number_of_changement_climatique_constat,
        s.number_of_changement_climatique_causes_directes,
        s.number_of_changement_climatique_consequences,
        s.number_of_attenuation_climatique_solutions_directes,
        s.number_of_adaptation_climatique_solutions_directes,
        s.number_of_biodiversite_concepts_generaux,
        s.number_of_biodiversite_causes_directes,
        s.number_of_biodiversite_consequences,
        s.number_of_biodiversite_solutions_directes,
        s.number_of_ressources,
        s.number_of_ressources_solutions,
        s.number_of_keywords
    FROM segments_programmes s
    -- g: the candidate program row of the segment, named as in grille_cle
    CROSS JOIN LATERAL (
        SELECT s.emission_cle, s.weekday, s.start_minute, s.end_minute, s.grid_start, s.grid_end
    ) g
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
-- validated misinformation cases, attributed to their emission and program day in analytics.cas_de_mesinformation
cas_lignes AS (
    SELECT emission_cle, jour_programme AS jour
    FROM {{ ref('cas_de_mesinformation') }}
    WHERE emission_cle IS NOT NULL
),
metriques AS (
    SELECT
        emission_cle,
        jour,
        SUM(nb_diffusions) AS nb_diffusions,
        SUM(volume_heures) AS volume_heures,
        SUM(env_heures) AS env_heures,
        SUM(climat_heures) AS climat_heures,
        SUM(biodiversite_heures) AS biodiversite_heures,
        SUM(ressources_heures) AS ressources_heures,
        SUM(climat_constat_heures) AS climat_constat_heures,
        SUM(climat_causes_heures) AS climat_causes_heures,
        SUM(climat_consequences_heures) AS climat_consequences_heures,
        SUM(climat_attenuation_heures) AS climat_attenuation_heures,
        SUM(climat_adaptation_heures) AS climat_adaptation_heures,
        SUM(biodiversite_constat_heures) AS biodiversite_constat_heures,
        SUM(biodiversite_causes_heures) AS biodiversite_causes_heures,
        SUM(biodiversite_consequences_heures) AS biodiversite_consequences_heures,
        SUM(biodiversite_solutions_heures) AS biodiversite_solutions_heures,
        SUM(ressources_constat_heures) AS ressources_constat_heures,
        SUM(ressources_solutions_heures) AS ressources_solutions_heures,
        SUM(nb_cas) AS nb_cas
    FROM (
        SELECT emission_cle, jour, 1 AS nb_diffusions, volume_heures, 0 AS env_heures,
            0 AS climat_heures, 0 AS biodiversite_heures, 0 AS ressources_heures, 0 AS climat_constat_heures, 0 AS climat_causes_heures, 0 AS climat_consequences_heures, 0 AS climat_attenuation_heures, 0 AS climat_adaptation_heures, 0 AS biodiversite_constat_heures, 0 AS biodiversite_causes_heures, 0 AS biodiversite_consequences_heures, 0 AS biodiversite_solutions_heures, 0 AS ressources_constat_heures, 0 AS ressources_solutions_heures,
            0 AS nb_cas
        FROM diffusions
        UNION ALL
        SELECT emission_cle, jour, 0, 0, number_of_keywords * 20 / 3600.0,
            COALESCE(number_of_keywords_climat, 0) * 20 / 3600.0, COALESCE(number_of_keywords_biodiversite, 0) * 20 / 3600.0, COALESCE(number_of_keywords_ressources, 0) * 20 / 3600.0, COALESCE(number_of_changement_climatique_constat, 0) * 20 / 3600.0, COALESCE(number_of_changement_climatique_causes_directes, 0) * 20 / 3600.0, COALESCE(number_of_changement_climatique_consequences, 0) * 20 / 3600.0, COALESCE(number_of_attenuation_climatique_solutions_directes, 0) * 20 / 3600.0, COALESCE(number_of_adaptation_climatique_solutions_directes, 0) * 20 / 3600.0, COALESCE(number_of_biodiversite_concepts_generaux, 0) * 20 / 3600.0, COALESCE(number_of_biodiversite_causes_directes, 0) * 20 / 3600.0, COALESCE(number_of_biodiversite_consequences, 0) * 20 / 3600.0, COALESCE(number_of_biodiversite_solutions_directes, 0) * 20 / 3600.0, COALESCE(number_of_ressources, 0) * 20 / 3600.0, COALESCE(number_of_ressources_solutions, 0) * 20 / 3600.0,
            0
        FROM segments_lignes
        UNION ALL
        SELECT emission_cle, jour, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
        FROM cas_lignes
    ) m
    WHERE jour >= '{{ start_date }}'::date
    GROUP BY 1, 2
),
-- channels captured each day: minutes in public.time_monitored, a keyword segment (time_monitored starts at the
-- end of 2024) or a validated case
captation AS (
    SELECT DISTINCT channel_name, start::date AS jour
    FROM {{ source('public', 'time_monitored') }}
    WHERE country = 'france' AND duration_minutes > 0
    UNION
    SELECT DISTINCT channel_name, jour_paris
    FROM segments
    UNION
    -- a validated case was captured
    SELECT DISTINCT data_item_channel_name, jour_programme
    FROM {{ ref('cas_de_mesinformation') }}
    WHERE jour_programme IS NOT NULL
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
    m.nb_cas::int AS nb_cas,
    m.climat_heures,
    m.biodiversite_heures,
    m.ressources_heures,
    m.climat_constat_heures,
    m.climat_causes_heures,
    m.climat_consequences_heures,
    m.climat_attenuation_heures,
    m.climat_adaptation_heures,
    m.biodiversite_constat_heures,
    m.biodiversite_causes_heures,
    m.biodiversite_consequences_heures,
    m.biodiversite_solutions_heures,
    m.ressources_constat_heures,
    m.ressources_solutions_heures,
    c.channel_name IS NOT NULL AS chaine_captee
FROM metriques m
JOIN emissions e ON e.emission_cle = m.emission_cle
LEFT JOIN captation c ON c.channel_name = e.channel_name AND c.jour = m.jour
