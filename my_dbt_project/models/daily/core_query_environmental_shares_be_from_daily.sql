{# Same table as core_query_environmental_shares_be (same columns, same order), computed from the days of
   core_query_environmental_shares_be_daily. Built next to it during the double run, compared with it in
   double_run_comparison. #}
{{ config(materialized='table') }}


SELECT
    DATE_TRUNC('week', CAST(day AS timestamp))                          AS "start",
    channel_name,
    COALESCE(channel_title, channel_name)                               AS channel_title,
    country,
    bool_or(has_grid)                                                   AS has_grid,

    SUM(denominator_min)                                                AS sum_duration_minutes,
    SUM(theorique_min)                                                  AS sum_theorique_min,
    SUM(time_monitored_min)                                             AS sum_time_monitored,
    SUM(monitored_in_perimeter_min)                                     AS sum_monitored_in_perimeter_min,
    ROUND(100.0 * SUM(monitored_in_perimeter_min)
                / NULLIF(SUM(theorique_min),0), 1)                      AS couverture_dictionnaire_pct,

    CAST(SUM(env_min)                AS float)/NULLIF(SUM(denominator_min),0) AS "% environnement total",
    CAST(SUM(climat_min)             AS float)/NULLIF(SUM(denominator_min),0) AS "% climat",
    CAST(SUM(climat_cause_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% climat cause",
    CAST(SUM(climat_adapt_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% climat solutions adaptation",
    CAST(SUM(climat_conseq_min)      AS float)/NULLIF(SUM(denominator_min),0) AS "% climat consequences",
    CAST(SUM(climat_atten_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% climat solutions attenuation",
    CAST(SUM(climat_constat_min)     AS float)/NULLIF(SUM(denominator_min),0) AS "% climat constat",
    CAST(SUM(biodiv_min)             AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversite",
    CAST(SUM(biodiv_constat_min)     AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité constat",
    CAST(SUM(biodiv_solution_min)    AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité solutions",
    CAST(SUM(biodiv_conseq_min)      AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité conséquences",
    CAST(SUM(biodiv_cause_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité causes",
    CAST(SUM(ressource_min)          AS float)/NULLIF(SUM(denominator_min),0) AS "% ressources",
    CAST(SUM(ressource_constat_min)  AS float)/NULLIF(SUM(denominator_min),0) AS "% ressources constat",
    CAST(SUM(ressource_solution_min) AS float)/NULLIF(SUM(denominator_min),0) AS "% ressources solutions"
FROM {{ ref('core_query_environmental_shares_be_daily') }}
GROUP BY "start", channel_name, channel_title, country
ORDER BY "start" ASC, channel_title ASC