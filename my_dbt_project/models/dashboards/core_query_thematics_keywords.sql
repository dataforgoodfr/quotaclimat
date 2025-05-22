{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
  )
}}

with RECURSIVE weekly_expansion AS (
-- Base case: get initial week_start for each row
SELECT
  pm.channel_title,
  pm.channel_name,
  pm.country,
  pm.channel_program,
  pm.public,
  pm.infocontinue,
  pm.radio,
  pm.weekday,
  CAST(pm.program_grid_start AS TIMESTAMP) AS start_ts,
  CAST(pm.program_grid_end AS TIMESTAMP) AS end_ts,
  pm.duration_minutes,
  DATE_TRUNC('week', CAST(pm.program_grid_start AS TIMESTAMP)) AS week_start
FROM public.program_metadata pm
UNION ALL
-- Recursive case: add 1 week until week_start exceeds end_ts
SELECT
  we.channel_title,
  we.channel_name,
  we.country,
  we.channel_program,
  we.public,
  we.infocontinue,
  we.radio,
  we.weekday,
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
	weekly_expansion.channel_name,
	weekly_expansion.channel_program,
    weekly_expansion.public,
    weekly_expansion.infocontinue,
    weekly_expansion.radio,
    weekly_expansion.week_start,
    SUM(weekly_expansion.duration_minutes) AS sum_duration_minutes
  FROM weekly_expansion
  where weekly_expansion.country = 'france'
  GROUP BY
    weekly_expansion.channel_title,
	weekly_expansion.channel_name,
	weekly_expansion.channel_program,
    weekly_expansion.public,
    weekly_expansion.infocontinue,
    weekly_expansion.radio,
    weekly_expansion.week_start
),
keywords_query as (
	SELECT
	  pm.channel_title,
	  DATE_TRUNC('week', k.start) :: date AS week,
	  -- Dictionary metadata
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
	    WHEN LOWER(kw ->> 'theme') LIKE '%solution%' THEN TRUE
	    ELSE FALSE
	  END AS is_solution,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%consequence%' THEN TRUE
	      ELSE FALSE
	  END AS is_consequence,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%cause%' THEN TRUE
	      ELSE FALSE
	  END AS is_cause,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%concepts_generaux%' THEN TRUE
	      ELSE FALSE
	  END AS is_general_concepts,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%constat%' THEN TRUE
	      ELSE FALSE
	  END AS is_statement,
	    category, --from dictionary table
	  -- Crise type selon le thème
	  CASE
	    WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
	    WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
	    WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
	    ELSE 'Autre'
	  END AS crise_type,
	  kw ->> 'theme' AS theme,
	 -- COALESCE(NULLIF(TRIM(kw ->> 'category'), ''), 'Transversal') AS category, --legacy category would can be not up to date.
	  kw ->> 'keyword' AS keyword,
	  pm.weekday as weekday,
	      COUNT(*) AS count
	    FROM
	      public.keywords k    
	LEFT JOIN public.program_metadata pm ON k.channel_program = pm.channel_program   
	   AND k.channel_name = pm.channel_name
	      AND (
	        (
	          CASE
	            WHEN (
	              (
	                EXTRACT(
	                  DOW
	                  FROM
	                    k.start
	                ) :: int + 1 + 6
	              ) % 7
	            ) = 0 THEN 7
	            ELSE (
	              (
	                EXTRACT(
	                  DOW
	                  FROM
	                    k.start
	                ) :: int + 1 + 6
	              ) % 7
	            )
	          END = pm.weekday
	        )
	      )
	      AND CAST(k.start AS date) BETWEEN CAST(pm.program_grid_start AS date)
	      AND CAST(pm.program_grid_end AS date) -- Expand keywords
	,
	      json_array_elements(k.keywords_with_timestamp :: json) AS kw -- Join dictionary on keyword
	  LEFT JOIN public.dictionary d ON d."keyword" = kw ->> 'keyword' -- Exclude indirect themes
	   -- Unnest categories array into one line per category
	LEFT JOIN LATERAL UNNEST(
	    COALESCE(
	        d.categories,
	        ARRAY['Transversal']
	        )
	    ) AS category ON TRUE
	WHERE
	      LOWER(kw ->> 'theme') NOT LIKE '%indirect%'
	  AND k."country" = 'france'
	GROUP BY
	      pm.channel_title,
	      DATE_TRUNC('week', k.start) :: date,
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
	      WHEN LOWER(kw ->> 'theme') LIKE '%solution%' THEN TRUE
	      ELSE FALSE
	  END,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%consequence%' THEN TRUE
	      ELSE FALSE
	  END,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%cause%' THEN TRUE
	      ELSE FALSE
	  END,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%concepts_generaux%' THEN TRUE
	      ELSE FALSE
	  END,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%constat%' THEN TRUE
	      ELSE FALSE
	  END,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
	      WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
	      WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
	      ELSE 'Autre'
	  END,
	  CASE
	      WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
	      WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
	      WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
	      ELSE 'Autre'
	  END,
	  kw ->> 'theme',
	  COALESCE(NULLIF(TRIM(kw ->> 'category'), ''), 'Transversal'),
	  kw ->> 'keyword',
		    category,
		    pm.weekday
	ORDER BY
	      pm.channel_title,
	      week,
	      crise_type
),
keyword_counts as (
select 
  kq.channel_title,
  kq.week,
  kq.high_risk_of_false_positive,
  kq.solution,
  kq.consequence,
  kq.cause,
  kq.general_concepts,
  kq.statement,
  kq.crisis_climate,
  kq.crisis_biodiversity,
  kq.crisis_resource,
  kq.categories,
  kq.themes,
  kq.language,
  kq.is_solution,
  kq.is_consequence,
  kq.is_cause,
  kq.is_general_concepts,
  kq.is_statement,
  kq.category,
  kq.crise_type,
  kq.theme,
  kq.keyword,
  sum(kq.count) as count
from keywords_query kq
group by 
  kq.channel_title,
  kq.week,
  kq.high_risk_of_false_positive,
  kq.solution,
  kq.consequence,
  kq.cause,
  kq.general_concepts,
  kq.statement,
  kq.crisis_climate,
  kq.crisis_biodiversity,
  kq.crisis_resource,
  kq.categories,
  kq.themes,
  kq.language,
  kq.is_solution,
  kq.is_consequence,
  kq.is_cause,
  kq.is_general_concepts,
  kq.is_statement,
  kq.category,
  kq.crise_type,
  kq.theme,
  kq.keyword
ORDER by
  kq.channel_title,
  kq.week,
  kq.crise_type
)
select 
  kc.channel_title,
  kc.week,
  kc.high_risk_of_false_positive,
  kc.solution,
  kc.consequence,
  kc.cause,
  kc.general_concepts,
  kc.statement,
  kc.crisis_climate,
  kc.crisis_biodiversity,
  kc.crisis_resource,
  kc.categories,
  kc.themes,
  kc.language,
  kc.is_solution,
  kc.is_consequence,
  kc.is_cause,
  kc.is_general_concepts,
  kc.is_statement,
  kc.category,
  kc.crise_type,
  kc.theme,
  kc.keyword,
  kc.count,
  sum(pd.sum_duration_minutes) as sum_duration_minutes
from keyword_counts kc
left join program_durations pd
on
	kc.channel_title=pd.channel_title
and
	kc.week=pd.week_start::date
group by 
kc.channel_title,
  kc.week,
  kc.high_risk_of_false_positive,
  kc.solution,
  kc.consequence,
  kc.cause,
  kc.general_concepts,
  kc.statement,
  kc.crisis_climate,
  kc.crisis_biodiversity,
  kc.crisis_resource,
  kc.categories,
  kc.themes,
  kc.language,
  kc.is_solution,
  kc.is_consequence,
  kc.is_cause,
  kc.is_general_concepts,
  kc.is_statement,
  kc.category,
  kc.crise_type,
  kc.theme,
  kc.keyword,
  kc.count
ORDER by
  kc.channel_title,
  kc.week,
  kc.crise_type