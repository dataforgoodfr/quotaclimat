{{
    config(
        materialized='table',
    )
}}

{% set window_minutes = var('ad_program_window_minutes', 60) %}
{#- time zone of advertising.ad_occurrence.occurrence_date (timestamp without time zone) -#}
{% set occurrence_tz = var('ad_occurrence_timezone', 'UTC') %}

-- One row per ad tunnel with, at most {{ window_minutes }} minutes away, the program of analytics.program
-- (French channels: the emissions of the Programmes Google Sheet, else the monitored programs of
-- program_metadata) just before it, and the one just after it when the tunnel is not inside the one before:
-- a tunnel between two programs has both, with the gaps in seconds. Grids are in Paris local time.
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
channel_days AS (
    SELECT DISTINCT channel_name, day FROM tunnel_days
),
-- programs broadcast on these days, in the time zone of the occurrences
programs AS (
    SELECT
        p.channel_name,
        days.day,
        p.id,
        p.label,
        ((days.day + p.start_minute * interval '1 minute') AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS start_date,
        -- end_minute is beyond 1440 for the programs ending after midnight
        ((days.day + p.end_minute * interval '1 minute') AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS end_date
    FROM channel_days days
    JOIN {{ ref('program') }} p
      ON p.channel_name = days.channel_name
     AND p.weekday = EXTRACT(ISODOW FROM days.day)
     AND days.day BETWEEN COALESCE(p.grid_start, '-infinity'::date) AND p.grid_end
    WHERE p.country = 'france'
      -- emissions with an invalid time in the sheet are left out
      AND p.start_minute IS NOT NULL
      AND p.end_minute IS NOT NULL
),
candidates AS (
    SELECT
        t.tunnel_id,
        t.start_date AS tunnel_start,
        t.end_date AS tunnel_end,
        p.id,
        p.label,
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
        id,
        label,
        GREATEST(EXTRACT(EPOCH FROM (tunnel_start - end_date)), 0) AS gap_sec,
        end_date >= tunnel_end AS inside_program
    FROM candidates
    WHERE start_date <= tunnel_start
      AND end_date >= tunnel_start - interval '{{ window_minutes }} minutes'
    ORDER BY tunnel_id, start_date DESC, end_date DESC, label
),
-- first program started after the tunnel start, less than the window after its end
program_after AS (
    SELECT DISTINCT ON (tunnel_id)
        tunnel_id,
        id,
        label,
        GREATEST(EXTRACT(EPOCH FROM (start_date - tunnel_end)), 0) AS gap_sec
    FROM candidates
    WHERE start_date > tunnel_start
      AND start_date <= tunnel_end + interval '{{ window_minutes }} minutes'
    ORDER BY tunnel_id, start_date, end_date, label
)
SELECT
    t.tunnel_id,
    COALESCE(b.inside_program, FALSE) AS inside_program,
    b.id AS program_before_id,
    b.label AS program_before,
    b.gap_sec AS program_before_gap_sec,
    -- inside a program, the program after is the same one: left empty
    CASE WHEN NOT COALESCE(b.inside_program, FALSE) THEN a.id END AS program_after_id,
    CASE WHEN NOT COALESCE(b.inside_program, FALSE) THEN a.label END AS program_after,
    CASE WHEN NOT COALESCE(b.inside_program, FALSE) THEN a.gap_sec END AS program_after_gap_sec
FROM tunnels t
LEFT JOIN program_before b ON b.tunnel_id = t.tunnel_id
LEFT JOIN program_after a ON a.tunnel_id = t.tunnel_id
