{# One row per day, channel and keyword: the occurrences counted by core_query_thematics_keywords_be, which sums
   them by week (core_query_thematics_keywords_be_from_daily, with the weekly denominators of
   core_query_environmental_shares_be_daily). Incremental by day, see macros/daily_window.sql. #}
{{ config(
    materialized='incremental'
    ,incremental_strategy='delete+insert'
    ,unique_key=['day']
  )
}}

WITH grid_channels AS (
    SELECT unnest(ARRAY[
        'vtm','stu-bru','la-premiere','canvas','qmusic','radio-1',
        'ln-radio','radio-2','play','vivacite','bel-rtl','vrt1'
    ]) AS channel_name
),

channel_map AS (
    SELECT channel_name, MAX(channel_title) AS channel_title
    FROM "public"."keywords"
    WHERE country = 'belgium'
    GROUP BY channel_name
),

kw_scoped AS (
    SELECT k.*
    FROM "public"."keywords" k
    WHERE k.country = 'belgium'
      AND {{ daily_window('k.start') }}
      AND k.channel_name IN (SELECT channel_name FROM grid_channels)
      AND EXISTS (
          SELECT 1 FROM "public"."program_metadata" pm
          WHERE pm.country       = 'belgium'
            AND pm.channel_name  = k.channel_name
            AND pm.channel_program = k.channel_program
            AND COALESCE(pm.channel_program_type,'') = COALESCE(k.channel_program_type,'')
            AND COALESCE(NULLIF(((EXTRACT(DOW FROM k.start)::int + 1 + 6) % 7), 0), 7) = pm.weekday
            AND CAST(k.start AS date) BETWEEN CAST(pm.program_grid_start AS date)
                                          AND CAST(pm.program_grid_end   AS date)
      )
    UNION ALL
    SELECT k.*
    FROM "public"."keywords" k
    WHERE k.country = 'belgium'
      AND {{ daily_window('k.start') }}
      AND k.channel_name NOT IN (SELECT channel_name FROM grid_channels)
),

keyword_occurrences AS (
    SELECT DISTINCT
        ks.channel_name,
        CAST(ks.start AS date) AS day,
        ks.start AS occurrence_time,
        ks.country,
        CASE WHEN LOWER(kw ->> 'theme') LIKE '%solution%'          THEN TRUE ELSE FALSE END AS is_solution,
        CASE WHEN LOWER(kw ->> 'theme') LIKE '%consequence%'       THEN TRUE ELSE FALSE END AS is_consequence,
        CASE WHEN LOWER(kw ->> 'theme') LIKE '%cause%'             THEN TRUE ELSE FALSE END AS is_cause,
        CASE WHEN LOWER(kw ->> 'theme') LIKE '%concepts_generaux%' THEN TRUE ELSE FALSE END AS is_general_concepts,
        CASE WHEN LOWER(kw ->> 'theme') LIKE '%constat%'           THEN TRUE ELSE FALSE END AS is_statement,
        CASE
          WHEN LOWER(kw ->> 'theme') LIKE '%climat%'       THEN 'Crise climatique'
          WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
          WHEN LOWER(kw ->> 'theme') LIKE '%ressource%'    THEN 'Crise des ressources'
          ELSE 'Autre'
        END AS crise_type,
        kw ->> 'theme'   AS theme,
        kw ->> 'keyword' AS keyword
    FROM kw_scoped ks,
         json_array_elements(ks.keywords_with_timestamp::json) AS kw
    WHERE LOWER(kw ->> 'theme') NOT LIKE '%indirect%'
)
SELECT
    COALESCE(cm.channel_title, ko.channel_name) AS channel_title,
    ko.channel_name,
    ko.country,
    ko.day,
    COALESCE(NULLIF(d.category, ''), 'Transversal') AS category,
    d.high_risk_of_false_positive,
    ko.is_solution,
    ko.is_consequence,
    ko.is_cause,
    ko.is_general_concepts,
    ko.is_statement,
    ko.crise_type,
    ko.theme,
    ko.keyword,
    kmc.general,
    kmc.agriculture,
    kmc.transport,
    kmc.batiments,
    kmc.energie,
    kmc.industrie,
    kmc.eau,
    kmc.ecosysteme,
    kmc.economie_ressources,
    COUNT(*) AS count
FROM keyword_occurrences ko
LEFT JOIN "public"."dictionary" d
       ON d.keyword = ko.keyword
      AND d.theme LIKE ko.theme || '%'
LEFT JOIN "public"."keyword_macro_category" kmc
       ON kmc.keyword = ko.keyword
LEFT JOIN channel_map cm
       ON cm.channel_name = ko.channel_name
WHERE COALESCE(cm.channel_title, ko.channel_name) NOT IN ('LN24','LATROIS','CANALZ')
GROUP BY
    COALESCE(cm.channel_title, ko.channel_name),
    ko.channel_name, ko.country, ko.day,
    COALESCE(NULLIF(d.category, ''), 'Transversal'),
    d.high_risk_of_false_positive,
    ko.is_solution, ko.is_consequence, ko.is_cause,
    ko.is_general_concepts, ko.is_statement,
    ko.crise_type, ko.theme, ko.keyword,
    kmc.general, kmc.agriculture, kmc.transport, kmc.batiments,
    kmc.energie, kmc.industrie, kmc.eau, kmc.ecosysteme, kmc.economie_ressources
