{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
  )
}}

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
	    category
   
ORDER BY
      pm.channel_title,
      week,
      crise_type
