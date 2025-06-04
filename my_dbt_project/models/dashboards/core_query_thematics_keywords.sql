{{ config(
    materialized='incremental'
    ,unique_key=['week','channel_title']
  )
}}

-- Core Query Thematics Keywords makes only sense when looking for keywords,theme, and category together (otherwise duplicates
-- as a keyword inside keyword_with_timestamp is present 4 times, if the keyword has 4 themes)

WITH program_durations AS (
  SELECT
    pm.channel_title,
    pm.channel_program,
    CAST(pm.program_grid_start AS date) AS program_start,
    CAST(pm.program_grid_end AS date) AS program_end,
    pm.duration_minutes
  FROM public.program_metadata pm
  WHERE pm.country = 'france'
),

program_weeks AS (
  SELECT
    pd.channel_title,
    pd.channel_program,
    pd.duration_minutes,
    generate_series(
      date_trunc('week', pd.program_start),
      date_trunc('week', pd.program_end),
      interval '1 week'
    )::date AS week
  FROM program_durations pd
),

weekly_program_durations AS (
  SELECT
    channel_title,
    week,
    SUM(DISTINCT duration_minutes) AS weekly_duration_minutes
  FROM program_weeks
  GROUP BY channel_title, week
),

keyword_occurrences AS (
  SELECT DISTINCT
    COALESCE(pm.channel_title, k.channel_title) AS channel_title,
    DATE_TRUNC('week', k.start)::date AS week,
    k.start AS occurrence_time,
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
   AND CAST(k.start AS date) BETWEEN CAST(pm.program_grid_start AS date)
   AND CAST(pm.program_grid_end AS date)
  , json_array_elements(k.keywords_with_timestamp::json) AS kw
  WHERE
    LOWER(kw ->> 'theme') NOT LIKE '%indirect%'
    AND k.country = 'france'
)


SELECT
  ko.channel_title,
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
  COUNT(*) AS count,
  COALESCE(wpd.weekly_duration_minutes, 0) AS sum_duration_minutes
FROM keyword_occurrences ko
LEFT JOIN public.dictionary d
  ON d.keyword = ko.keyword AND d.theme LIKE ko.theme || '%' -- ensure matc with indirect theme inside the dictionary table

LEFT JOIN weekly_program_durations wpd
  ON wpd.channel_title = ko.channel_title AND wpd.week = ko.week
GROUP BY
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
  ko.keyword,
  wpd.weekly_duration_minutes
ORDER BY
  ko.channel_title, ko.week, ko.crise_type