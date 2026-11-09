{# Same table as core_query_thematics_keywords_be (same columns, same order), computed from the days of
   core_query_thematics_keywords_be_daily (occurrences) and core_query_environmental_shares_be_daily (denominators,
   same daily computation as in core_query_thematics_keywords_be). Built next to it during the double run, compared
   with it in double_run_comparison. #}
{{ config(materialized='table') }}

WITH denom_weekly AS (
    SELECT
        DATE_TRUNC('week', day::timestamp)::date AS week,
        channel_name,
        bool_or(has_grid)                   AS has_grid,
        SUM(denominator_min)                AS weekly_denominator_min,
        SUM(theorique_min)                  AS weekly_theorique_min,
        SUM(time_monitored_min)             AS weekly_time_monitored_min,
        SUM(monitored_in_perimeter_min)     AS weekly_in_perimeter_min
    FROM {{ ref('core_query_environmental_shares_be_daily') }}
    GROUP BY 1, 2
),

weekly AS (
    SELECT
        channel_title,
        channel_name,
        country,
        DATE_TRUNC('week', day::timestamp)::date AS week,
        category,
        high_risk_of_false_positive,
        is_solution,
        is_consequence,
        is_cause,
        is_general_concepts,
        is_statement,
        crise_type,
        theme,
        keyword,
        general,
        agriculture,
        transport,
        batiments,
        energie,
        industrie,
        eau,
        ecosysteme,
        economie_ressources,
        SUM(count) AS count
    FROM {{ ref('core_query_thematics_keywords_be_daily') }}
    GROUP BY channel_title, channel_name, country, 4, category, high_risk_of_false_positive, is_solution, is_consequence, is_cause, is_general_concepts, is_statement, crise_type, theme, keyword, general, agriculture, transport, batiments, energie, industrie, eau, ecosysteme, economie_ressources
)

SELECT
    w.channel_title,
    w.channel_name,
    w.country,
    w.week,
    dw.has_grid,
    w.category,
    w.high_risk_of_false_positive,
    w.is_solution,
    w.is_consequence,
    w.is_cause,
    w.is_general_concepts,
    w.is_statement,
    w.crise_type,
    w.theme,
    w.keyword,
    w.general,
    w.agriculture,
    w.transport,
    w.batiments,
    w.energie,
    w.industrie,
    w.eau,
    w.ecosysteme,
    w.economie_ressources,
    CAST(SUM(w.count) AS bigint) AS count,
    COALESCE(dw.weekly_denominator_min,    0) AS sum_duration_minutes,
    COALESCE(dw.weekly_theorique_min,      0) AS sum_theorique_min,
    COALESCE(dw.weekly_time_monitored_min, 0) AS sum_time_monitored,
    COALESCE(dw.weekly_in_perimeter_min,   0) AS sum_monitored_in_perimeter_min
FROM weekly w
LEFT JOIN denom_weekly dw
       ON dw.channel_name = w.channel_name
      AND dw.week         = w.week
GROUP BY
    w.channel_title, w.channel_name, w.country, w.week, dw.has_grid,
    w.category, w.high_risk_of_false_positive, w.is_solution, w.is_consequence, w.is_cause, w.is_general_concepts, w.is_statement, w.crise_type, w.theme, w.keyword, w.general, w.agriculture, w.transport, w.batiments, w.energie, w.industrie, w.eau, w.ecosysteme, w.economie_ressources,
    dw.weekly_denominator_min, dw.weekly_theorique_min,
    dw.weekly_time_monitored_min, dw.weekly_in_perimeter_min
