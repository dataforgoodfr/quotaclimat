{#- Program of analytics.program (an emission of the Programmes Google Sheet, else a program of
    program_metadata) on the air at a given time: one row per row of `relation` (a CTE or a table) on the
    air during a program, keyed by `id_column`, with the program id and label. `time_column` is a timestamp
    without time zone in `timezone`; the grids are in Paris time: French channels only. A program ending after midnight also
    covers the first hours of the next day. When several programs match (overlapping rows), an emission
    wins over a program, then the most recent grid, then the latest started. -#}
{% macro program_at(relation, id_column, channel_column, time_column, timezone='UTC') %}
SELECT DISTINCT ON (r.row_id)
    r.row_id AS {{ id_column }},
    p.id AS program_id,
    p.label AS program
FROM (
    SELECT
        {{ id_column }} AS row_id,
        {{ channel_column }} AS channel_name,
        ({{ time_column }} AT TIME ZONE '{{ timezone }}') AT TIME ZONE 'Europe/Paris' AS paris_time
    FROM {{ relation }}
) r
-- 0: program of the same day, 1: program of the day before ending after midnight
CROSS JOIN (VALUES (0), (1)) AS o(day_offset)
JOIN {{ ref('program') }} p
  ON p.channel_name = r.channel_name
 AND p.country = 'france'
 AND p.weekday = EXTRACT(ISODOW FROM r.paris_time::date - o.day_offset)
 AND r.paris_time::date - o.day_offset BETWEEN COALESCE(p.grid_start, '-infinity'::date) AND p.grid_end
 AND EXTRACT(EPOCH FROM r.paris_time - (r.paris_time::date - o.day_offset)) / 60 >= p.start_minute
 AND EXTRACT(EPOCH FROM r.paris_time - (r.paris_time::date - o.day_offset)) / 60 < p.end_minute
ORDER BY r.row_id, p.source = 'emission' DESC, p.grid_start DESC NULLS LAST, o.day_offset, p.start_minute DESC
{% endmacro %}
