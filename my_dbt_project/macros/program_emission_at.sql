{#- Emission of analytics.program broadcast at a given time: one row per row of `relation` (a CTE or a
    table) on the air during an emission, keyed by `id_column`, with the emission columns. `time_column`
    is a timestamp without time zone in `timezone`; the emission grids are in Paris time. An emission
    ending after midnight also covers the first hours of the next day. When several emissions match
    (overlapping rows in the sheet), the one of the most recent grid wins, then the latest started. -#}
{% macro program_emission_at(relation, id_column, channel_column, time_column, timezone='UTC') %}
SELECT DISTINCT ON (r.row_id)
    r.row_id AS {{ id_column }},
    pe.id AS emission_id,
    pe.emission,
    pe.presentation,
    pe.rediffusion
FROM (
    SELECT
        {{ id_column }} AS row_id,
        {{ channel_column }} AS channel_name,
        ({{ time_column }} AT TIME ZONE '{{ timezone }}') AT TIME ZONE 'Europe/Paris' AS paris_time
    FROM {{ relation }}
) r
-- 0: emission of the same day, 1: emission of the day before ending after midnight
CROSS JOIN (VALUES (0), (1)) AS o(day_offset)
JOIN {{ ref('program') }} pe
  ON pe.channel_name = r.channel_name
 AND pe.weekday = EXTRACT(ISODOW FROM r.paris_time::date - o.day_offset)
 AND r.paris_time::date - o.day_offset BETWEEN COALESCE(pe.grid_start, '-infinity'::date) AND pe.grid_end
 AND EXTRACT(EPOCH FROM r.paris_time - (r.paris_time::date - o.day_offset)) / 60 >= pe.start_minute
 AND EXTRACT(EPOCH FROM r.paris_time - (r.paris_time::date - o.day_offset)) / 60 < pe.end_minute
ORDER BY r.row_id, pe.grid_start DESC NULLS LAST, o.day_offset, pe.start_minute DESC
{% endmacro %}
