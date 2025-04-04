{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
  )
}}

SELECT
    pm.channel_title,
    DATE_TRUNC('week', k.start)::date AS week,

    -- Crise type selon le thème (calculé dans SELECT)
    CASE
        WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
        WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
        WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
        ELSE 'Autre'
    END AS crise_type,

    kw ->> 'theme' AS theme,
    COALESCE(NULLIF(TRIM(kw ->> 'category'), ''), 'Transversal') AS category,
    kw ->> 'keyword' AS keyword,

    COUNT(*) AS ligne_count

FROM public.keywords k
LEFT JOIN public.program_metadata pm 
    ON k.channel_program = pm.channel_program 
    AND k.channel_name = pm.channel_name
    AND (
        (CASE
            WHEN ((EXTRACT(DOW FROM k.start)::int + 1 + 6) % 7) = 0 THEN 7
            ELSE ((EXTRACT(DOW FROM k.start)::int + 1 + 6) % 7)
        END = pm.weekday)
    )
    AND CAST(k.start AS date) BETWEEN CAST(pm.program_grid_start AS date) AND CAST(pm.program_grid_end AS date)

-- On déplie les mots-clés
, json_array_elements(k.keywords_with_timestamp) AS kw

-- Exclusion des thèmes indirects
WHERE LOWER(kw ->> 'theme') NOT LIKE '%indirect%'

GROUP BY
    pm.channel_title,
    DATE_TRUNC('week', k.start)::date,
    CASE
        WHEN LOWER(kw ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
        WHEN LOWER(kw ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
        WHEN LOWER(kw ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
        ELSE 'Autre'
    END,
    kw ->> 'theme',
    COALESCE(NULLIF(TRIM(kw ->> 'category'), ''), 'Transversal'),
    kw ->> 'keyword'

ORDER BY
    pm.channel_title,
    week,
    crise_type;
