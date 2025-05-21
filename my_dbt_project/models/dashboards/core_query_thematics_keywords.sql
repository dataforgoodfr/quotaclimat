{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
  )
}}

with RECURSIVE weekly_expansion AS (
-- Base case: get initial week_start for each row
SELECT
  pm.channel_title,
  pm.country,
  pm.channel_program,
  pm.public,
  pm.infocontinue,
  pm.radio,
  CAST(pm.program_grid_start AS TIMESTAMP) AS start_ts,
  CAST(pm.program_grid_end AS TIMESTAMP) AS end_ts,
  pm.duration_minutes,
  DATE_TRUNC('week', CAST(pm.program_grid_start AS TIMESTAMP)) AS week_start
FROM public.program_metadata pm
UNION ALL
-- Recursive case: add 1 week until week_start exceeds end_ts
SELECT
  we.channel_title,
  we.country,
  we.channel_program,
  we.public,
  we.infocontinue,
  we.radio,
  we.start_ts,
  we.end_ts,
  we.duration_minutes,
  we.week_start + INTERVAL '7 days'
FROM weekly_expansion we
WHERE we.week_start + INTERVAL '7 days' <= we.end_ts
),
program_durations as (
  SELECT
    weekly_expansion.channel_title,
    weekly_expansion.public,
    weekly_expansion.infocontinue,
    weekly_expansion.radio,
    weekly_expansion.week_start,
    SUM(weekly_expansion.duration_minutes) AS sum_duration_minutes_week
  FROM weekly_expansion
  where weekly_expansion.country = 'france'
  GROUP BY
    weekly_expansion.channel_title,
    weekly_expansion.public,
    weekly_expansion.infocontinue,
    weekly_expansion.radio,
    weekly_expansion.week_start
),
keywords_with_durations as (
	select 
	      k.*,
	      DATE_TRUNC('week', k.start) :: date AS week_start,
	      json_array_elements(k.keywords_with_timestamp :: json) AS kw,
	      program_durations.sum_duration_minutes_week
	from keywords k  
	left join program_durations on k.channel_title=program_durations.channel_title and week_start=program_durations.week_start
)
select
  pm.channel_title channel_title,
  kd.week_start,
  kd.sum_duration_minutes_week,
  kd.channel_program,
  kd.channel_name,
  d.high_risk_of_false_positive,
  d.solution,
  d.consequence,
  d.cause,
  d.general_concepts,
  d.statement,
  d.crisis_climate,
  d.crisis_biodiversity,
  d.crisis_resource,
  d.categories,
  d.themes,
  d.language,
  CASE
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%solution%' THEN TRUE
	ELSE FALSE
  END AS is_solution,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%consequence%' THEN TRUE
	  ELSE FALSE
  END AS is_consequence,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%cause%' THEN TRUE
	  ELSE FALSE
  END AS is_cause,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%concepts_generaux%' THEN TRUE
	  ELSE FALSE
  END AS is_general_concepts,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%constat%' THEN TRUE
	  ELSE FALSE
  END AS is_statement,
  category, --from dictionary table
  -- Crise type selon le thème
  CASE
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
	ELSE 'Autre'
  END AS crise_type,
  kd.kw ->> 'theme' AS theme,
 -- COALESCE(NULLIF(TRIM(kd.kw ->> 'category'), ''), 'Transversal') AS category, --legacy category would can be not up to date.
  kd.kw ->> 'keyword' AS keyword,
  COUNT(*) AS count
from
  keywords_with_durations kd
left join public.program_metadata pm ON kd.channel_program = pm.channel_program
  AND kd.channel_name = pm.channel_name
  AND (
    (
      CASE
        WHEN (
          (
            EXTRACT(
              DOW
              FROM
                kd.start
            ) :: int + 1 + 6
          ) % 7
        ) = 0 THEN 7
        ELSE (
          (
            EXTRACT(
              DOW
              FROM
                kd.start
            ) :: int + 1 + 6
          ) % 7
        )
      END = pm.weekday
    )
  )
  AND CAST(kd.start AS date) BETWEEN CAST(pm.program_grid_start AS date) AND CAST(pm.program_grid_end AS date) -- Expand keywords
LEFT JOIN public.dictionary d ON d."keyword" = kd.kw ->> 'keyword'
-- Unnest categories array into one line per category
LEFT JOIN LATERAL UNNEST(
	COALESCE(
		d.categories,
		ARRAY['Transversal']
	)
) AS category ON TRUE
GROUP BY
  pm.channel_title,
  kd.week_start,
  kd.sum_duration_minutes_week,
  kd.channel_program,
  kd.channel_name,
  d.high_risk_of_false_positive,
  d.solution,
  d.consequence,
  d.cause,
  d.general_concepts,
  d.statement,
  d.crisis_climate,
  d.crisis_biodiversity,
  d.crisis_resource,
  d.categories,
  d.themes,
  d.language,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%solution%' THEN TRUE
	  ELSE FALSE
  END,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%consequence%' THEN TRUE
	  ELSE FALSE
  END,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%cause%' THEN TRUE
	  ELSE FALSE
  END,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%concepts_generaux%' THEN TRUE
	  ELSE FALSE
  END,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%constat%' THEN TRUE
	  ELSE FALSE
  END,
  CASE
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
	  WHEN LOWER(kd.kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
	  ELSE 'Autre'
  END,
  CASE
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
	WHEN LOWER(kd.kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
	ELSE 'Autre'
  END,
  kd.kw ->> 'theme',
  COALESCE(NULLIF(TRIM(kd.kw ->> 'category'), ''), 'Transversal'),
  kd.kw ->> 'keyword',
  category
ORDER BY
  pm.channel_title,
  week_start,
  crise_type
