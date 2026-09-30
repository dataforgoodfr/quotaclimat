{{
    config(
        materialized='table',
    )
}}

{% set window_minutes = var('ad_program_window_minutes', 60) %}
{#- time zone of advertising.ad_occurrence.occurrence_date (timestamp without time zone) -#}
{% set occurrence_tz = var('ad_occurrence_timezone', 'UTC') %}

-- One row per ad tunnel with the monitored program (program_metadata, French channels) just before it
-- and the one just after it when the tunnel is not inside the program before, each at most
-- {{ window_minutes }} minutes away. Program grids are in Paris local time.
WITH tunnels AS (
    SELECT
        tunnel_id,
        channel_name,
        start_date,
        end_date
    FROM {{ ref('ad_tunnels') }}
),
-- Paris days around each tunnel: the day before for the programs ending after midnight,
-- the day after for the programs starting after midnight
tunnel_days AS (
    SELECT
        t.tunnel_id,
        t.channel_name,
        d::date AS day
    FROM tunnels t
    CROSS JOIN LATERAL generate_series(
        ((t.start_date AT TIME ZONE '{{ occurrence_tz }}') AT TIME ZONE 'Europe/Paris')::date - 1,
        ((t.end_date AT TIME ZONE '{{ occurrence_tz }}') AT TIME ZONE 'Europe/Paris')::date + 1,
        interval '1 day'
    ) d
),
-- programs broadcast on these days, in the time zone of the occurrences
programs AS (
    SELECT DISTINCT
        pm.channel_name,
        days.day,
        pm.channel_program,
        pm.channel_program_type,
        ((days.day + pm.start::time) AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS start_date,
        ((days.day + pm."end"::time
            -- programs ending at or after midnight
            + CASE WHEN pm."end"::time <= pm.start::time THEN interval '1 day' ELSE interval '0' END
        ) AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS end_date
    FROM (SELECT DISTINCT channel_name, day FROM tunnel_days) days
    JOIN {{ source('public', 'program_metadata') }} pm
      ON pm.channel_name = days.channel_name
     AND pm.weekday = EXTRACT(ISODOW FROM days.day)
     AND days.day BETWEEN pm.program_grid_start AND pm.program_grid_end
    WHERE pm.country = 'france'
),
candidates AS (
    SELECT
        t.tunnel_id,
        t.start_date AS tunnel_start,
        t.end_date AS tunnel_end,
        p.channel_program,
        p.channel_program_type,
        p.start_date,
        p.end_date
    FROM tunnel_days td
    JOIN tunnels t ON t.tunnel_id = td.tunnel_id
    JOIN programs p ON p.channel_name = td.channel_name AND p.day = td.day
),
-- last program started before the tunnel, in progress or ended less than the window before
program_before AS (
    SELECT DISTINCT ON (tunnel_id)
        tunnel_id,
        channel_program,
        channel_program_type,
        GREATEST(EXTRACT(EPOCH FROM (tunnel_start - end_date)), 0) AS gap_sec,
        end_date >= tunnel_end AS inside_program
    FROM candidates
    WHERE start_date <= tunnel_start
      AND end_date >= tunnel_start - interval '{{ window_minutes }} minutes'
    ORDER BY tunnel_id, start_date DESC, end_date DESC, channel_program
),
-- first program started after the tunnel start, less than the window after its end
program_after AS (
    SELECT DISTINCT ON (tunnel_id)
        tunnel_id,
        channel_program,
        channel_program_type,
        GREATEST(EXTRACT(EPOCH FROM (start_date - tunnel_end)), 0) AS gap_sec
    FROM candidates
    WHERE start_date > tunnel_start
      AND start_date <= tunnel_end + interval '{{ window_minutes }} minutes'
    ORDER BY tunnel_id, start_date, end_date, channel_program
)
SELECT
    t.tunnel_id,
    COALESCE(b.inside_program, FALSE) AS inside_program,
    b.channel_program AS program_before,
    b.channel_program_type AS program_before_type,
    b.gap_sec AS program_before_gap_sec,
    -- inside a program, the program after is the same one: left empty
    CASE WHEN NOT COALESCE(b.inside_program, FALSE) THEN a.channel_program END AS program_after,
    CASE WHEN NOT COALESCE(b.inside_program, FALSE) THEN a.channel_program_type END AS program_after_type,
    CASE WHEN NOT COALESCE(b.inside_program, FALSE) THEN a.gap_sec END AS program_after_gap_sec
FROM tunnels t
LEFT JOIN program_before b ON b.tunnel_id = t.tunnel_id
LEFT JOIN program_after a ON a.tunnel_id = t.tunnel_id
