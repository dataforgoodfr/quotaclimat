{{ config(
    materialized='incremental'
    ,unique_key=['week','channel_title']
  )
}}

    SELECT
      COALESCE(pm.channel_title, k.channel_title) AS channel_title,
      DATE_TRUNC('week', k.start) :: date AS week,
      -- Dictionary metadata
      d.high_risk_of_false_positive,
      COALESCE(NULLIF(d.category, ''), 'Transversal') AS category,
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
      -- Crise type selon le thème
      CASE
        WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
        WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
        WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
        ELSE 'Autre'
      END AS crise_type,
      kw ->> 'theme' AS theme,
      kw ->> 'keyword' AS keyword,
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
      LEFT JOIN public.dictionary d ON d."keyword" = kw ->> 'keyword'AND d."theme" = kw ->> 'theme'

WHERE
      LOWER(kw ->> 'theme') NOT LIKE '%indirect%'
      AND k."country" = 'france'
      
   
GROUP BY
      COALESCE(pm.channel_title, k.channel_title),
      DATE_TRUNC('week', k.start) :: date,
      -- Dictionary metadata
      d.high_risk_of_false_positive,
      COALESCE(NULLIF(d.category, ''), 'Transversal'),
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
      kw ->> 'keyword',
	    category
   
ORDER BY
      channel_title,
      week,
      crise_type
