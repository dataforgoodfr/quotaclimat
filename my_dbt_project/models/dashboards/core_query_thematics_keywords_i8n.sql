{{ config(
    materialized='incremental'
    ,unique_key=['week','channel_title']
  )
}}
{{ config(
    materialized='incremental'
    ,unique_key=['week','channel_title']
  )
}}

WITH keyword_occurrences AS (
  SELECT DISTINCT
    COALESCE(pm.channel_title, k.channel_title) AS channel_title,
    DATE_TRUNC('week', k.start)::date AS week,
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
)

SELECT
  ko.channel_title,
  ko.country,
  ko.week,
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
  COUNT(*) AS count
FROM keyword_occurrences ko
LEFT JOIN public.dictionary d
  ON d.keyword = ko.keyword AND d.theme LIKE ko.theme || '%' -- ensure matc with indirect theme inside the dictionary table
GROUP BY
  ko.country,
  ko.channel_title,
  ko.week,
  d.high_risk_of_false_positive,
  COALESCE(NULLIF(d.category, ''), 'Transversal'),
  ko.is_solution,
  ko.is_consequence,
  ko.is_cause,
  ko.is_general_concepts,
  ko.is_statement,
  ko.crise_type,
  ko.theme,
  ko.keyword
ORDER BY
  ko.channel_title, ko.week, ko.crise_type