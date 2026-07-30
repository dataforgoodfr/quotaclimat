{{ config(
    materialized='incremental',
    unique_key=['start','channel_title']
  )
}}

WITH grid_channels AS (
    SELECT unnest(ARRAY[
        'vtm','stu-bru','la-premiere','canvas','qmusic','radio-1',
        'ln-radio','radio-2','play','vivacite','bel-rtl','vrt1'
    ]) AS channel_name
),

channel_map AS (   -- une seule ligne par channel_name, pour éviter tout fan-out
    SELECT channel_name, MAX(channel_title) AS channel_title
    FROM "public"."keywords"
    WHERE country = 'belgium'
    GROUP BY channel_name
),

-- ===== PÉRIMÈTRE : chunks retenus (identique à la core query env shares) =====
kw_scoped AS (
    -- chaînes de grille : uniquement les chunks tombant dans un créneau program_metadata
    SELECT k.*
    FROM "public"."keywords" k
    WHERE k.country = 'belgium'
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
    -- chaînes hors grille : tous les chunks
    SELECT k.*
    FROM "public"."keywords" k
    WHERE k.country = 'belgium'
      AND k.channel_name NOT IN (SELECT channel_name FROM grid_channels)
),

-- ===== DÉNOMINATEUR =====
kw_chunks_daily AS (          -- minutes captées DANS le périmètre
    SELECT CAST(start AS date) AS day, channel_name, COUNT(*) * 2 AS kw_chunks_min
    FROM kw_scoped
    GROUP BY 1, 2
),
tm_daily AS (                 -- minutes captées au total
    SELECT channel_name, CAST(start AS date) AS day, SUM(duration_minutes) AS duration_minutes
    FROM "public"."time_monitored"
    WHERE country = 'belgium'
    GROUP BY 1, 2
),
spine AS (
    SELECT channel_name, day FROM tm_daily
    UNION
    SELECT channel_name, day FROM kw_chunks_daily
),
denom_daily AS (
    SELECT
        s.day,
        s.channel_name,
        (s.channel_name IN (SELECT channel_name FROM grid_channels)) AS has_grid,
        COALESCE(tm.duration_minutes, 0) AS time_monitored_min,
        CASE WHEN s.channel_name IN (SELECT channel_name FROM grid_channels)
             THEN COALESCE(kwc.kw_chunks_min, 0) END AS monitored_in_perimeter_min,
        -- >>> dénominateur effectif <
        CASE WHEN s.channel_name IN (SELECT channel_name FROM grid_channels)
             THEN COALESCE(kwc.kw_chunks_min, 0)      -- chaîne à grille -> périmètre seul
             ELSE COALESCE(tm.duration_minutes, 0)    -- chaîne sans grille -> time_monitored
        END AS denominator_min
    FROM spine s
    LEFT JOIN tm_daily        tm  ON tm.channel_name  = s.channel_name AND tm.day  = s.day
    LEFT JOIN kw_chunks_daily kwc ON kwc.channel_name = s.channel_name AND kwc.day = s.day
),
denom_weekly AS (
    SELECT
        DATE_TRUNC('week', day::timestamp)::date AS week,
        channel_name,
        bool_or(has_grid)                   AS has_grid,
        SUM(denominator_min)                AS weekly_denominator_min,
        SUM(time_monitored_min)             AS weekly_time_monitored_min,
        SUM(monitored_in_perimeter_min)     AS weekly_in_perimeter_min
    FROM denom_daily
    GROUP BY 1, 2
),

-- ===== NUMÉRATEUR : occurrences de mots-clés =====
keyword_occurrences AS (
    SELECT DISTINCT
        ks.channel_name,
        DATE_TRUNC('week', ks.start)::date AS week,
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
    ko.week,
    dw.has_grid,
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
    COUNT(*) AS count,
    COALESCE(dw.weekly_denominator_min,    0) AS sum_duration_minutes,
    COALESCE(dw.weekly_time_monitored_min, 0) AS sum_time_monitored,
    COALESCE(dw.weekly_in_perimeter_min,   0) AS sum_monitored_in_perimeter_min
FROM keyword_occurrences ko
LEFT JOIN "public"."dictionary" d
       ON d.keyword = ko.keyword
      AND d.theme LIKE ko.theme || '%'   -- matche aussi les variantes "indirect" du dictionnaire
LEFT JOIN denom_weekly dw
       ON dw.channel_name = ko.channel_name
      AND dw.week         = ko.week
LEFT JOIN "public"."keyword_macro_category" kmc
       ON kmc.keyword = ko.keyword
LEFT JOIN channel_map cm
       ON cm.channel_name = ko.channel_name
WHERE COALESCE(cm.channel_title, ko.channel_name) NOT IN ('LN24','LATROIS','CANALZ')
GROUP BY
    COALESCE(cm.channel_title, ko.channel_name),
    ko.channel_name,
    ko.country,
    ko.week,
    dw.has_grid,
    COALESCE(NULLIF(d.category, ''), 'Transversal'),
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
    dw.weekly_denominator_min,
    dw.weekly_time_monitored_min,
    dw.weekly_in_perimeter_min
ORDER BY channel_title, ko.week, ko.crise_type