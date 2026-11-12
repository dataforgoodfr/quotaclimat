{# One row per day, channel and keyword: the occurrences counted by core_query_thematics_keywords_i8n, which sums them by
   week and adds the weekly duration of the program grid (core_query_thematics_keywords_i8n_from_daily). Incremental by day, see
   macros/daily_window.sql. #}
{{ config(
    materialized='incremental'
    ,incremental_strategy='delete+insert'
    ,unique_key=['day']
  )
}}

WITH keyword_occurrences AS (
  SELECT DISTINCT
    COALESCE(pm.channel_title, k.channel_title) AS channel_title,
    CAST(k.start AS date) AS day,
    k.start AS occurrence_time,
    k.country AS country,
    -- Semantic tags
    CASE WHEN LOWER(kw ->> 'theme') LIKE '%solution%' THEN TRUE ELSE FALSE END AS is_solution,
    CASE WHEN LOWER(kw ->> 'theme') LIKE '%consequence%' THEN TRUE ELSE FALSE END AS is_consequence,
    CASE WHEN LOWER(kw ->> 'theme') LIKE '%cause%' THEN TRUE ELSE FALSE END AS is_cause,
    CASE WHEN LOWER(kw ->> 'theme') LIKE '%concepts_generaux%' THEN TRUE ELSE FALSE END AS is_general_concepts,
    CASE WHEN LOWER(kw ->> 'theme') LIKE '%constat%' THEN TRUE ELSE FALSE END AS is_statement,
    -- Crisis type
    CASE
      WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
      WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
      WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
      ELSE 'Autre'
    END AS crise_type,
    kw ->> 'theme' AS theme,
    kw ->> 'keyword' AS keyword
  FROM public.keywords k
  LEFT JOIN public.program_metadata pm
    ON k.channel_program = pm.channel_program
   AND k.channel_name = pm.channel_name
   AND (
      (
        CASE
          WHEN ((EXTRACT(DOW FROM k.start)::int + 1 + 6) % 7) = 0 THEN 7
          ELSE ((EXTRACT(DOW FROM k.start)::int + 1 + 6) % 7)
        END = pm.weekday
      )
    )
   -- AND k.country = pm.country
   AND CAST(k.start AS date) BETWEEN CAST(pm.program_grid_start AS date)
   AND CAST(pm.program_grid_end AS date)
  , json_array_elements(k.keywords_with_timestamp::json) AS kw
  WHERE
    LOWER(kw ->> 'theme') NOT LIKE '%indirect%'
    AND {{ daily_window('k.start') }}
)
SELECT
  ko.channel_title,
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
LEFT JOIN public.dictionary d
  ON d.keyword = ko.keyword AND d.theme LIKE ko.theme || '%' -- ensure matc with indirect theme inside the dictionary table
LEFT JOIN public.keyword_macro_category kmc
  ON kmc.keyword = ko.keyword
GROUP BY
  ko.channel_title,
  ko.country,
  ko.day,
  COALESCE(NULLIF(d.category, ''), 'Transversal'),
  d.high_risk_of_false_positive,
  ko.is_solution, ko.is_consequence, ko.is_cause, ko.is_general_concepts, ko.is_statement,
  ko.crise_type, ko.theme, ko.keyword,
  kmc.general, kmc.agriculture, kmc.transport, kmc.batiments, kmc.energie,
  kmc.industrie, kmc.eau, kmc.ecosysteme, kmc.economie_ressources
