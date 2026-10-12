{{
    config(
        materialized='table',
        schema='analytics',
    )
}}

{#- analytics.program: the monitored programs of program_metadata (one row per program and per day),
    split into the emissions broadcast in them when they are known (Programmes Google Sheet, tab
    emissions-infos-en-continue, 24h news channels). Every row has the program; a split row also has the
    emission, with the time slot of the emission cut to the program (start, end). What is outside the
    monitored programs is left out (e.g. 23:00-24:00 for the 6:00-23:00 programs of the news channels).
    An emission over two programs gives one row in each. Before / after the validity of its emissions
    (grid_start / grid_end), a program is not split.
    label: program / emission (presenters) [rediffusion], the channel when the program has no name.
    Columns of the tab:
    - weekday: '*' (every day), 'weekday' (Monday to Friday), 'weekend' (Saturday and Sunday) or days
      1 (Monday) to 7 (Sunday) separated by '|', e.g. '1|2|3|4'. Numbered like program_metadata.weekday,
      not like the Python grids (channel_program.py, 0 for Monday).
    - start / end: HH:MM, end '24:00' for midnight, an end before the start for an emission ending after
      midnight. An emission with an invalid time is left out.
    - rediffusion: 'oui' for a rerun.
    - grid_start / grid_end: validity of the row (YYYY-MM-DD or DD/MM/YYYY): an empty grid_end is
      2100-01-01 (like transform_program.py), an empty grid_start has no lower bound.
    Invalid weekdays, times or dates are reported by the seed tests (seeds/ref/_ref_seeds.yml). -#}

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
-- one row per emission and per day of weekday_spec
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
        channel_title,
        country,
        public,
        infocontinue,
        radio,
        weekday,
        channel_program,
        channel_program_type,
        start::text AS start,
        "end"::text AS "end",
        program_grid_start::date AS grid_start,
        COALESCE(program_grid_end::date, {{ no_grid_end }}) AS grid_end,
        -- start / end are text in production ('6:00'), maybe inferred as intervals by dbt seed ('06:00:00')
        start_minute,
        -- minutes since the start of the day, beyond 1440 when ending after midnight
        end_minute_of_day + CASE WHEN end_minute_of_day <= start_minute THEN 1440 ELSE 0 END AS end_minute
    FROM (
        SELECT
            *,
            SPLIT_PART(start::text, ':', 1)::int * 60 + SPLIT_PART(start::text, ':', 2)::int AS start_minute,
            SPLIT_PART("end"::text, ':', 1)::int * 60 + SPLIT_PART("end"::text, ':', 2)::int AS end_minute_of_day
        FROM {{ source('public', 'program_metadata') }}
    ) pm
),
-- each emission cut to each program it overlaps, in the minutes of the program day: the emission of the
-- day before (day_offset -1, its minutes shifted by -1440) or after (+1) for the programs or emissions
-- crossing midnight
splits AS (
    SELECT
        p.id AS program_metadata_id,
        e.id AS emission_id,
        e.emission,
        e.presentation,
        e.rediffusion,
        e.start AS emission_start,
        e."end" AS emission_end,
        GREATEST(p.start_minute, e.start_minute + o.day_offset * 1440) AS start_minute,
        LEAST(p.end_minute, e.end_minute + o.day_offset * 1440) AS end_minute,
        GREATEST(p.grid_start, e.grid_start) AS grid_start,
        LEAST(p.grid_end, e.grid_end) AS grid_end
    FROM programs p
    CROSS JOIN (VALUES (-1), (0), (1)) AS o(day_offset)
    JOIN emission_days e
      ON e.channel_name = p.channel_name
     AND e.weekday = (p.weekday - 1 + o.day_offset + 7) % 7 + 1
    -- LEAST / GREATEST ignore NULLs: emissions with an invalid time left out
    WHERE e.start_minute IS NOT NULL
      AND e.end_minute IS NOT NULL
      AND GREATEST(p.start_minute, e.start_minute + o.day_offset * 1440)
          < LEAST(p.end_minute, e.end_minute + o.day_offset * 1440)
      AND GREATEST(p.grid_start, e.grid_start) <= LEAST(p.grid_end, e.grid_end)
),
-- days of a split program without emissions: before the first ones, after the last ones
unsplit AS (
    SELECT
        p.id AS program_metadata_id,
        p.grid_start,
        LEAST(p.grid_end, s.first_day - 1) AS grid_end
    FROM programs p
    LEFT JOIN (
        SELECT program_metadata_id, MIN(grid_start) AS first_day, MAX(grid_end) AS last_day
        FROM splits
        GROUP BY program_metadata_id
    ) s ON s.program_metadata_id = p.id
    WHERE s.program_metadata_id IS NULL OR s.first_day > p.grid_start
    UNION ALL
    SELECT p.id, s.last_day + 1, p.grid_end
    FROM programs p
    JOIN (
        SELECT program_metadata_id, MAX(grid_end) AS last_day
        FROM splits
        GROUP BY program_metadata_id
    ) s ON s.program_metadata_id = p.id
    WHERE s.last_day < p.grid_end
),
grid AS (
    SELECT * FROM splits
    UNION ALL
    SELECT
        u.program_metadata_id,
        NULL::text AS emission_id,
        NULL::text AS emission,
        NULL::text AS presentation,
        NULL::boolean AS rediffusion,
        NULL::text AS emission_start,
        NULL::text AS emission_end,
        p.start_minute,
        p.end_minute,
        u.grid_start,
        u.grid_end
    FROM unsplit u
    JOIN programs p ON p.id = u.program_metadata_id
)
SELECT
    MD5(CONCAT_WS('|', g.program_metadata_id, g.emission_id, g.grid_start)) AS id,
    p.channel_name,
    p.channel_title,
    p.country,
    p.public,
    p.infocontinue,
    p.radio,
    p.weekday,
    -- the channel when the program has no name
    COALESCE(NULLIF(CONCAT_WS(' / ',
        p.channel_program,
        g.emission || COALESCE(' (' || g.presentation || ')', '')
            || CASE WHEN g.rediffusion THEN ' [rediffusion]' ELSE '' END
    ), ''), p.channel_title) AS label,
    -- time slot of the row (the emission cut to the program), HH:MM
    {{ hhmm('g.start_minute') }} AS start,
    {{ hhmm('g.end_minute') }} AS "end",
    -- minutes since the start of the program day (Paris time), end beyond 1440 when ending after midnight
    g.start_minute,
    g.end_minute,
    g.end_minute - g.start_minute AS duration_minutes,
    g.grid_start,
    g.grid_end,
    g.program_metadata_id,
    p.channel_program,
    p.channel_program_type,
    p.start AS program_start,
    p."end" AS program_end,
    g.emission_id,
    g.emission,
    g.presentation,
    g.rediffusion,
    g.emission_start,
    g.emission_end
FROM grid g
JOIN programs p ON p.id = g.program_metadata_id
