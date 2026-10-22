{{
    config(
        materialized='table',
        schema='analytics',
    )
}}

{#- One row per misinformation case validated in Label Studio: the rows of task_global_completion with
    the first annotation ("Annotation Version" = 1) answering 'Correct' alone ('Correct,Incorrect', when
    annotators disagree, is left out), same definition as ad_occurrence_mesinfo, all countries.
    With the channel attributes of program_metadata and, for the French channels (grids in Paris time), the row
    of analytics.program on the air at the start of the segment (data_item_start, UTC): the monitored program,
    split into its emission when known, with its emission key and program day as in public.emission_jour,
    which counts the cases from here. The cases have no program id: they are matched on time, with the ingestion
    margins of the keyword segments: the API is queried from start - 5 min to end + 5 min (api_to_s3.py,
    epoch_margin of France) and the program reassignment accepts 6 min
    (channel_program.get_matching_program_hour), hence [start - 6 min, end + 6 min[. Priority to the row
    containing the case, then the closest one, then a row with an emission, the most recent grid, the program
    of the same day, the latest started. Program day: the day the emission starts, an emission ending after
    midnight counting for the day before. A case further than 6 min from every row of its channel has no
    program. Built in the analytics schema after task_global_completion (see entrypoints/dbt.sh). -#}
WITH mesinfo AS (
    SELECT *
    FROM {{ source('analytics', 'task_global_completion') }}
    WHERE mesinfo_choice = 'Correct'
      AND "Annotation Version" = 1
),
-- channel attributes of the most recent grid, per country (the same channel_name may exist in several countries)
channels AS (
    SELECT DISTINCT ON (channel_name, country)
        channel_name,
        country,
        channel_title,
        public,
        infocontinue,
        radio
    FROM {{ source('public', 'program_metadata') }}
    ORDER BY channel_name, country, program_grid_end DESC NULLS FIRST, program_grid_start DESC NULLS LAST
),
-- candidate program days of each French case: the same day, the day before (program ending after midnight),
-- the day after (margin before a program starting at midnight)
mesinfo_jours AS (
    SELECT
        m.task_aggregate_id,
        m.data_item_channel_name AS channel_name,
        o.day_offset,
        t.paris_time::date - o.day_offset AS jour,
        EXTRACT(ISODOW FROM t.paris_time::date - o.day_offset)::int AS weekday,
        EXTRACT(EPOCH FROM t.paris_time - (t.paris_time::date - o.day_offset)) / 60 AS minute
    FROM mesinfo m
    CROSS JOIN LATERAL (SELECT (m.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' AS paris_time) t
    CROSS JOIN (VALUES (-1), (0), (1)) AS o(day_offset)
    WHERE m.country = 'france'
),
mesinfo_programs AS (
    SELECT DISTINCT ON (c.task_aggregate_id)
        c.task_aggregate_id,
        p.id AS program_id,
        p.label AS program,
        concat_ws('|', p.channel_name,
            CASE WHEN p.emission IS NOT NULL THEN 'émission' ELSE 'programme' END,
            COALESCE(p.emission, p.channel_program, p.channel_title, p.channel_name),
            COALESCE(p.rediffusion, FALSE)::text) AS emission_cle,
        c.jour AS jour_programme
    FROM mesinfo_jours c
    JOIN {{ ref('program') }} p
      ON p.channel_name = c.channel_name
     AND p.country = 'france'
     AND p.weekday = c.weekday
     AND c.jour BETWEEN COALESCE(p.grid_start, '-infinity'::date) AND p.grid_end
     AND c.minute >= p.start_minute - 6
     AND c.minute < p.end_minute + 6
    ORDER BY c.task_aggregate_id,
        c.minute >= p.start_minute AND c.minute < p.end_minute DESC,
        GREATEST(p.start_minute - c.minute, c.minute - p.end_minute),
        p.emission IS NOT NULL DESC, p.grid_start DESC NULLS LAST, c.day_offset, p.start_minute DESC
)
SELECT
    m.*,
    c.channel_title,
    c.public,
    c.infocontinue,
    c.radio,
    e.program_id,
    e.program,
    e.emission_cle,
    e.jour_programme
FROM mesinfo m
LEFT JOIN channels c ON c.channel_name = m.data_item_channel_name AND c.country = m.country
LEFT JOIN mesinfo_programs e ON e.task_aggregate_id = m.task_aggregate_id
