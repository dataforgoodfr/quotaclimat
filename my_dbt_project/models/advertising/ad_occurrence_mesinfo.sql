{{
    config(
        materialized='table',
    )
}}

{% set window_days = var('ad_mesinfo_window_days', 7) %}

-- One row per non-deleted occurrence with a validated misinformation on the same channel
-- at most {{ window_days }} days before or after it: the nearest one and the distance to it.
-- analytics.task_global_completion is built by the `--target analytics` run, which must run
-- before the advertising models (see entrypoints/dbt.sh).
WITH occ AS (
    SELECT
        o.id AS occurrence_id,
        o.channel_name,
        o.occurrence_date
    FROM {{ source('advertising', 'ad_occurrence') }} o
    WHERE o.deleted_at IS NULL
      AND o.occurrence_date >= '{{ var("ad_analysis_start_date", "2025-09-29") }}'::timestamp
),
mesinfo AS (
    -- to be validated by the misinformation team: first annotation only, 'Correct' alone
    -- ('Correct,Incorrect' when annotators disagree is left out), France only
    SELECT
        task_aggregate_id,
        data_item_channel_name AS channel_name,
        data_item_start
    FROM {{ source('analytics', 'task_global_completion') }}
    WHERE mesinfo_choice = 'Correct'
      AND "Annotation Version" = 1
      AND country = 'france'
)
SELECT DISTINCT ON (occ.occurrence_id)
    occ.occurrence_id,
    m.task_aggregate_id AS nearest_mesinfo_task_aggregate_id,
    ABS(EXTRACT(EPOCH FROM (occ.occurrence_date - m.data_item_start))) AS mesinfo_distance_sec
FROM occ
JOIN mesinfo m
  ON m.channel_name = occ.channel_name
 AND m.data_item_start BETWEEN occ.occurrence_date - interval '{{ window_days }} days'
                           AND occ.occurrence_date + interval '{{ window_days }} days'
ORDER BY occ.occurrence_id, mesinfo_distance_sec, m.task_aggregate_id
