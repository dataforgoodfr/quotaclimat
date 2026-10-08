{{
    config(
        materialized='table',
    )
}}

{#- One row per emission and per day, from the Programmes Google Sheet (tab emissions-infos-en-continue,
    24h news channels), with its channel and the monitored program of program_metadata it is broadcast in.
    Columns of the tab:
    - weekday: '*' (every day), 'weekday' (Monday to Friday), 'weekend' (Saturday and Sunday) or days
      1 (Monday) to 7 (Sunday) separated by '|', e.g. '1|2|3|4'. Numbered like program_metadata.weekday,
      not like the Python grids (channel_program.py, 0 for Monday).
    - start / end: HH:MM, end '24:00' for midnight, an end before the start for an emission ending after
      midnight. An invalid time gives NULL duration and no program.
    - rediffusion: 'oui' for a rerun.
    - grid_start / grid_end: validity of the row (YYYY-MM-DD or DD/MM/YYYY), as program_grid_start /
      program_grid_end: an empty grid_end is 2100-01-01 (like transform_program.py), an empty grid_start
      has no lower bound (NULL).
    Rows with an invalid weekday give no row: the seed tests (seeds/ref/_ref_seeds.yml) report them, like
    invalid times or dates.
    Program: among the program_metadata rows of the channel valid during the emission grid, the one of
    the most recent grid whose time slot overlaps the emission the most (programs of the day before or
    after crossing midnight included). NULL when the emission is outside the monitored programs, e.g.
    23:00-24:00 for the 6:00-23:00 slots of the news channels. -#}

{% set no_grid_end = "'2100-01-01'::date" %}

-- cast to text: the seed columns are text (column_types), unless dbt inferred their type (e.g. a date
-- for grid_end when dbt partial parsing missed the seed config)
WITH sheet AS (
    SELECT
        NULLIF(TRIM(channel_name::text), '') AS channel_name,
        NULLIF(TRIM(emission::text), '') AS emission,
        NULLIF(TRIM(presentation::text), '') AS presentation,
        LOWER(REPLACE(weekday::text, ' ', '')) AS weekday_spec,
        TRIM(start::text) AS start,
        TRIM("end"::text) AS "end",
        COALESCE(LOWER(TRIM(rediffusion::text)) = 'oui', FALSE) AS rediffusion,
        TRIM(grid_start::text) AS grid_start,
        TRIM(grid_end::text) AS grid_end
    FROM ({{ source_or_empty('programs', 'news_channel_emissions', [
        'channel_name', 'emission', 'presentation', 'weekday', 'start', '"end"', 'rediffusion', 'grid_start', 'grid_end'
    ]) }}) emissions
),
parsed AS (
    SELECT
        channel_name,
        emission,
        presentation,
        weekday_spec,
        start,
        "end",
        rediffusion,
        CASE WHEN start ~ '^([01]?[0-9]|2[0-3]):[0-5][0-9]$'
            THEN SPLIT_PART(start, ':', 1)::int * 60 + SPLIT_PART(start, ':', 2)::int
        END AS start_minute,
        CASE WHEN "end" ~ '^(([01]?[0-9]|2[0-3]):[0-5][0-9]|24:00)$'
            THEN SPLIT_PART("end", ':', 1)::int * 60 + SPLIT_PART("end", ':', 2)::int
        END AS end_minute_of_day,
        CASE
            WHEN grid_start ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TO_DATE(grid_start, 'YYYY-MM-DD')
            WHEN grid_start ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN TO_DATE(grid_start, 'DD/MM/YYYY')
        END AS grid_start,
        COALESCE(CASE
            WHEN grid_end ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TO_DATE(grid_end, 'YYYY-MM-DD')
            WHEN grid_end ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN TO_DATE(grid_end, 'DD/MM/YYYY')
        END, {{ no_grid_end }}) AS grid_end
    FROM sheet
),
-- one row per day of weekday_spec
emission_days AS (
    SELECT
        MD5(CONCAT_WS('|', p.channel_name, d.weekday, p.start, p.grid_start)) AS id,
        p.channel_name,
        d.weekday,
        p.emission,
        p.presentation,
        p.rediffusion,
        p.start,
        p."end",
        p.start_minute,
        -- minutes since the start of the day, beyond 1440 when ending after midnight
        p.end_minute_of_day
            + CASE WHEN p.end_minute_of_day <= p.start_minute THEN 1440 ELSE 0 END AS end_minute,
        p.grid_start,
        p.grid_end
    FROM parsed p
    CROSS JOIN LATERAL (
        SELECT DISTINCT day::int AS weekday
        FROM UNNEST(CASE
            WHEN p.weekday_spec = '*' THEN ARRAY['1', '2', '3', '4', '5', '6', '7']
            WHEN p.weekday_spec = 'weekday' THEN ARRAY['1', '2', '3', '4', '5']
            WHEN p.weekday_spec = 'weekend' THEN ARRAY['6', '7']
            WHEN p.weekday_spec ~ '^[1-7](\|[1-7])*$' THEN STRING_TO_ARRAY(p.weekday_spec, '|')
        END) AS day
    ) d
),
programs AS (
    SELECT
        id,
        channel_name,
        weekday,
        channel_program,
        channel_program_type,
        start::text AS start,
        "end"::text AS "end",
        program_grid_start::date AS program_grid_start,
        COALESCE(program_grid_end::date, {{ no_grid_end }}) AS program_grid_end,
        -- start / end are text in production ('6:00'), maybe inferred as intervals by dbt seed ('06:00:00')
        SPLIT_PART(start::text, ':', 1)::int * 60 + SPLIT_PART(start::text, ':', 2)::int AS start_minute,
        SPLIT_PART("end"::text, ':', 1)::int * 60 + SPLIT_PART("end"::text, ':', 2)::int AS end_minute_of_day
    FROM {{ source('public', 'program_metadata') }}
),
-- channel attributes of the most recent grid
channels AS (
    SELECT DISTINCT ON (channel_name)
        channel_name,
        channel_title,
        country,
        public,
        infocontinue,
        radio
    FROM {{ source('public', 'program_metadata') }}
    ORDER BY channel_name, COALESCE(program_grid_end::date, {{ no_grid_end }}) DESC, program_grid_start DESC
),
program_overlaps AS (
    SELECT
        e.id,
        p.id AS program_metadata_id,
        p.channel_program,
        p.channel_program_type,
        p.start AS program_start,
        p."end" AS program_end,
        p.program_grid_start,
        p.program_grid_end,
        -- program slot shifted to the emission day: -1440 for the day before, +1440 for the day after
        LEAST(
            e.end_minute,
            p.end_minute_of_day + CASE WHEN p.end_minute_of_day <= p.start_minute THEN 1440 ELSE 0 END
                + o.day_offset * 1440
        ) - GREATEST(e.start_minute, p.start_minute + o.day_offset * 1440) AS overlap_minutes
    FROM emission_days e
    CROSS JOIN (VALUES (-1), (0), (1)) AS o(day_offset)
    JOIN programs p
      ON p.channel_name = e.channel_name
     AND p.weekday = (e.weekday - 1 + o.day_offset + 7) % 7 + 1
     AND p.program_grid_start <= e.grid_end
     AND (e.grid_start IS NULL OR p.program_grid_end >= e.grid_start)
),
emission_programs AS (
    SELECT DISTINCT ON (id)
        id,
        program_metadata_id,
        channel_program,
        channel_program_type,
        program_start,
        program_end,
        overlap_minutes
    FROM program_overlaps
    WHERE overlap_minutes > 0
    ORDER BY id, program_grid_end DESC, overlap_minutes DESC, program_grid_start DESC, program_metadata_id
)
SELECT
    e.id,
    e.channel_name,
    c.channel_title,
    c.country,
    c.public,
    c.infocontinue,
    c.radio,
    e.weekday,
    e.emission,
    e.presentation,
    e.rediffusion,
    e.start,
    e."end",
    e.end_minute - e.start_minute AS duration_minutes,
    e.grid_start,
    e.grid_end,
    ep.program_metadata_id,
    ep.channel_program,
    ep.channel_program_type,
    ep.program_start,
    ep.program_end,
    COALESCE(ep.overlap_minutes, 0) AS program_overlap_minutes
FROM emission_days e
LEFT JOIN channels c ON c.channel_name = e.channel_name
LEFT JOIN emission_programs ep ON ep.id = e.id
