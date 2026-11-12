{# Same table as homepage_environment_by_media_by_month, computed from the days of
   core_query_environmental_shares_daily: share of the week by channel (in %), mean of the weeks of each month (a week
   counts in the month of its Monday), and a row 'Aucun' with the mean of the channels. #}
{{ config(materialized='table') }}

WITH weekly AS (
  SELECT
    DATE_TRUNC('week', CAST("day" AS timestamp)) AS week,
    channel_title,
    CAST(100 * SUM(crise_env_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS pct
  FROM {{ ref('core_query_environmental_shares_daily') }}
  GROUP BY 1, 2
),

grouped_data AS (
  SELECT
    DATE_TRUNC('month', week) AS "start",
    channel_title AS "channel_title",
    AVG(pct) AS "avg"
  FROM weekly
  GROUP BY 1, 2
)

SELECT "start", "channel_title", "avg"
FROM grouped_data

UNION ALL

SELECT "start", 'Aucun' AS "channel_title", AVG("avg") AS "avg_per_channel"
FROM grouped_data
GROUP BY "start"
