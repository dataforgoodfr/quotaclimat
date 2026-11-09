-- Double run of models/daily: fails (warning only) when a table of models/dashboards or models/homepage differs
-- from the same table computed from the daily layer, see models/daily/double_run_comparison.sql.
{{ config(severity='warn') }}

SELECT *
FROM {{ ref('double_run_comparison') }}
WHERE only_in_old > 0 OR only_in_new > 0 OR old_rows <> new_rows
