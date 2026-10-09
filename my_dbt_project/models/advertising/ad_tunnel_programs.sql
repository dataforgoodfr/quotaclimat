{{
    config(
        materialized='table',
    )
}}

{% set window_minutes = var('ad_program_window_minutes', 60) %}
{#- time zone of advertising.ad_occurrence.occurrence_date (timestamp without time zone) -#}
{% set occurrence_tz = var('ad_occurrence_timezone', 'UTC') %}

-- One row per ad tunnel with, at most {{ window_minutes }} minutes away, the monitored program (program_metadata,
-- French channels) just before it and the one just after it when the tunnel is not inside the program before,
-- and the same for the emissions (analytics.program, from the Programmes Google Sheet): a tunnel between two
-- programs or emissions has both, with the gaps in seconds. Program and emission grids are in Paris local time.
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
-- programs and emissions broadcast on these days, in the time zone of the occurrences
slots AS (
    SELECT DISTINCT
        'program' AS kind,
        pm.channel_name,
        days.day,
        NULL::text AS slot_id,
        pm.channel_program AS name,
        pm.channel_program_type AS type,
        ((days.day + pm.start::time) AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS start_date,
        ((days.day + pm."end"::time
            -- programs ending at or after midnight
            + CASE WHEN pm."end"::time <= pm.start::time THEN interval '1 day' ELSE interval '0' END
        ) AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS end_date
    FROM channel_days days
    JOIN {{ source('public', 'program_metadata') }} pm
      ON pm.channel_name = days.channel_name
     AND pm.weekday = EXTRACT(ISODOW FROM days.day)
     AND days.day BETWEEN pm.program_grid_start AND pm.program_grid_end
    WHERE pm.country = 'france'
    UNION ALL
    SELECT
        'emission' AS kind,
        e.channel_name,
        days.day,
        e.id AS slot_id,
        e.emission AS name,
        NULL::text AS type,
        ((days.day + e.start_minute * interval '1 minute') AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS start_date,
        -- end_minute is beyond 1440 for the emissions ending after midnight
        ((days.day + e.end_minute * interval '1 minute') AT TIME ZONE 'Europe/Paris') AT TIME ZONE '{{ occurrence_tz }}' AS end_date
    FROM channel_days days
    JOIN {{ ref('program') }} e
      ON e.channel_name = days.channel_name
     AND e.weekday = EXTRACT(ISODOW FROM days.day)
     AND days.day BETWEEN COALESCE(e.grid_start, '-infinity'::date) AND e.grid_end
    -- emissions with an invalid time in the sheet are left out
    WHERE e.start_minute IS NOT NULL
      AND e.end_minute IS NOT NULL
),
candidates AS (
    SELECT
        t.tunnel_id,
        t.start_date AS tunnel_start,
        t.end_date AS tunnel_end,
        s.kind,
        s.slot_id,
        s.name,
        s.type,
        s.start_date,
        s.end_date
    FROM tunnel_days td
    JOIN tunnels t ON t.tunnel_id = td.tunnel_id
    JOIN slots s ON s.channel_name = td.channel_name AND s.day = td.day
),
-- last program / emission started before the tunnel, in progress or ended less than the window before
slot_before AS (
    SELECT DISTINCT ON (tunnel_id, kind)
        tunnel_id,
        kind,
        slot_id,
        name,
        type,
        GREATEST(EXTRACT(EPOCH FROM (tunnel_start - end_date)), 0) AS gap_sec,
        end_date >= tunnel_end AS inside
    FROM candidates
    WHERE start_date <= tunnel_start
      AND end_date >= tunnel_start - interval '{{ window_minutes }} minutes'
    ORDER BY tunnel_id, kind, start_date DESC, end_date DESC, name
),
-- first program / emission started after the tunnel start, less than the window after its end
slot_after AS (
    SELECT DISTINCT ON (tunnel_id, kind)
        tunnel_id,
        kind,
        slot_id,
        name,
        type,
        GREATEST(EXTRACT(EPOCH FROM (start_date - tunnel_end)), 0) AS gap_sec
    FROM candidates
    WHERE start_date > tunnel_start
      AND start_date <= tunnel_end + interval '{{ window_minutes }} minutes'
    ORDER BY tunnel_id, kind, start_date, end_date, name
)
SELECT
    t.tunnel_id,
    COALESCE(pb.inside, FALSE) AS inside_program,
    pb.name AS program_before,
    pb.type AS program_before_type,
    pb.gap_sec AS program_before_gap_sec,
    -- inside a program, the program after is the same one: left empty
    CASE WHEN NOT COALESCE(pb.inside, FALSE) THEN pa.name END AS program_after,
    CASE WHEN NOT COALESCE(pb.inside, FALSE) THEN pa.type END AS program_after_type,
    CASE WHEN NOT COALESCE(pb.inside, FALSE) THEN pa.gap_sec END AS program_after_gap_sec,
    COALESCE(eb.inside, FALSE) AS inside_emission,
    eb.slot_id AS emission_before_id,
    eb.name AS emission_before,
    eb.gap_sec AS emission_before_gap_sec,
    -- inside an emission, the emission after is the same one: left empty
    CASE WHEN NOT COALESCE(eb.inside, FALSE) THEN ea.slot_id END AS emission_after_id,
    CASE WHEN NOT COALESCE(eb.inside, FALSE) THEN ea.name END AS emission_after,
    CASE WHEN NOT COALESCE(eb.inside, FALSE) THEN ea.gap_sec END AS emission_after_gap_sec
FROM tunnels t
LEFT JOIN slot_before pb ON pb.tunnel_id = t.tunnel_id AND pb.kind = 'program'
LEFT JOIN slot_after pa ON pa.tunnel_id = t.tunnel_id AND pa.kind = 'program'
LEFT JOIN slot_before eb ON eb.tunnel_id = t.tunnel_id AND eb.kind = 'emission'
LEFT JOIN slot_after ea ON ea.tunnel_id = t.tunnel_id AND ea.kind = 'emission'
