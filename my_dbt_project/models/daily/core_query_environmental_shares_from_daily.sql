{# Same table as core_query_environmental_shares (same columns, same order), computed from the days of core_query_environmental_shares_daily: the
   ratios of the week are the sums of the week divided by the duration of the week. Built next to core_query_environmental_shares during the
   double run, compared with it in double_run_comparison. #}
{{ config(materialized='table') }}

SELECT
  DATE_TRUNC('week', CAST("day" AS timestamp)) AS "start",
  channel_title AS "Program Metadata - Channel Name__channel_title",
  public AS "Program Metadata - Channel Name__public",
  infocontinue AS "Program Metadata - Channel Name__infocontinue",
  radio AS "Program Metadata - Channel Name__radio",
  CAST(SUM(sum_duration_minutes) AS bigint) AS "sum_duration_minutes",
  CAST(SUM(crise_env_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% environnement total",
  CAST(SUM(climat_total_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% climat",
  CAST(SUM(climat_cause_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% climat cause",
  CAST(SUM(climat_adaptation_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% climat solutions adaptation ",
  CAST(SUM(climat_consequence_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% climat consequences",
  CAST(SUM(climat_attenuation_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% climat solutions attenuation",
  CAST(SUM(climat_constat_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% climat constat",
  CAST(SUM(biodiversite_total_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% biodiversite",
  CAST(SUM(biodiversite_concept_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% biodiversité constat",
  CAST(SUM(biodiversite_solution_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% biodiversité solutions",
  CAST(SUM(biodiversite_consequence_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% biodiversité conséquences",
  CAST(SUM(biodiversite_cause_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% biodiversité causes",
  CAST(SUM(ressource_total_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% ressources",
  CAST(SUM(ressource_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% ressources constat",
  CAST(SUM(ressource_solution_minutes) AS float) / NULLIF(SUM(sum_duration_minutes), 0) AS "% ressources solutions"
FROM {{ ref('core_query_environmental_shares_daily') }}
GROUP BY 1, 2, 3, 4, 5
