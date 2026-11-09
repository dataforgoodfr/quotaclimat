{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
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
      AND k.channel_name IN (SELECT channel_name FROM grid_channels)
      AND EXISTS (
          SELECT 1 FROM "public"."program_metadata" pm
          WHERE pm.country      = 'belgium'
            AND pm.channel_name = k.channel_name
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
      AND k.channel_name NOT IN (SELECT channel_name FROM grid_channels)
),

kw_daily AS (
    SELECT
        CAST(start AS date) AS day,
        channel_name,
        COUNT(*) * 2                                                    AS kw_chunks_min,
        SUM(number_of_keywords                                  * 20)/60.0 AS env_min,
        SUM(number_of_keywords_climat                           * 20)/60.0 AS climat_min,
        SUM(number_of_keywords_biodiversite                     * 20)/60.0 AS biodiv_min,
        SUM(number_of_keywords_ressources                       * 20)/60.0 AS ressource_min,
        SUM(number_of_changement_climatique_constat             * 20)/60.0 AS climat_constat_min,
        SUM(number_of_changement_climatique_causes_directes     * 20)/60.0 AS climat_cause_min,
        SUM(number_of_changement_climatique_consequences        * 20)/60.0 AS climat_conseq_min,
        SUM(number_of_adaptation_climatique_solutions_directes  * 20)/60.0 AS climat_adapt_min,
        SUM(number_of_attenuation_climatique_solutions_directes * 20)/60.0 AS climat_atten_min,
        SUM(number_of_biodiversite_concepts_generaux            * 20)/60.0 AS biodiv_constat_min,
        SUM(number_of_biodiversite_causes_directes              * 20)/60.0 AS biodiv_cause_min,
        SUM(number_of_biodiversite_consequences                 * 20)/60.0 AS biodiv_conseq_min,
        SUM(number_of_biodiversite_solutions_directes           * 20)/60.0 AS biodiv_solution_min,
        SUM(number_of_ressources                                * 20)/60.0 AS ressource_constat_min,
        SUM(number_of_ressources_solutions                      * 20)/60.0 AS ressource_solution_min
    FROM kw_scoped
    GROUP BY 1, 2
),

tm_daily AS (
    SELECT
        channel_name,
        CAST(start AS date)      AS day,
        SUM(duration_minutes)    AS duration_minutes
    FROM "public"."time_monitored"
    WHERE country = 'belgium'
    GROUP BY 1, 2
),

-- ===== jours réellement captés (garde-fou) =====
jours_captes AS (
    SELECT channel_name, CAST(start AS date) AS day
    FROM "public"."keywords"
    WHERE country = 'belgium'
    GROUP BY 1, 2
),

-- ===== durée théorique des créneaux actifs, par jour capté =====
theorique_daily AS (
    SELECT j.channel_name,
           j.day,
           SUM(EXTRACT(EPOCH FROM (pm."end"::time - pm.start::time)) / 60.0) AS theorique_min
    FROM jours_captes j
    JOIN "public"."program_metadata" pm
      ON pm.country      = 'belgium'
     AND pm.channel_name = j.channel_name
     AND pm.weekday      = COALESCE(NULLIF(((EXTRACT(DOW FROM j.day)::int + 1 + 6) % 7), 0), 7)
     AND j.day BETWEEN CAST(pm.program_grid_start AS date) AND CAST(pm.program_grid_end AS date)
    GROUP BY 1, 2
),

spine AS (
    SELECT channel_name, day FROM tm_daily
    UNION
    SELECT channel_name, day FROM kw_daily
),

daily AS (
    SELECT
        s.day,
        s.channel_name,
        cm.channel_title,
        'belgium'::text AS country,
        (s.channel_name IN (SELECT channel_name FROM grid_channels)) AS has_grid,

        COALESCE(tm.duration_minutes, 0)  AS time_monitored_min,
        CASE WHEN s.channel_name IN (SELECT channel_name FROM grid_channels)
             THEN COALESCE(kw.kw_chunks_min, 0) END AS monitored_in_perimeter_min,
        CASE WHEN s.channel_name IN (SELECT channel_name FROM grid_channels)
             THEN COALESCE(th.theorique_min, 0) END AS theorique_min,

        -- >>> DÉNOMINATEUR : théorique pour les chaînes à grille <
        CASE WHEN s.channel_name IN (SELECT channel_name FROM grid_channels)
             THEN COALESCE(th.theorique_min, 0)
             ELSE COALESCE(tm.duration_minutes, 0)
        END AS denominator_min,

        COALESCE(kw.env_min,0) env_min, COALESCE(kw.climat_min,0) climat_min,
        COALESCE(kw.biodiv_min,0) biodiv_min, COALESCE(kw.ressource_min,0) ressource_min,
        COALESCE(kw.climat_constat_min,0) climat_constat_min,
        COALESCE(kw.climat_cause_min,0) climat_cause_min,
        COALESCE(kw.climat_conseq_min,0) climat_conseq_min,
        COALESCE(kw.climat_adapt_min,0) climat_adapt_min,
        COALESCE(kw.climat_atten_min,0) climat_atten_min,
        COALESCE(kw.biodiv_constat_min,0) biodiv_constat_min,
        COALESCE(kw.biodiv_cause_min,0) biodiv_cause_min,
        COALESCE(kw.biodiv_conseq_min,0) biodiv_conseq_min,
        COALESCE(kw.biodiv_solution_min,0) biodiv_solution_min,
        COALESCE(kw.ressource_constat_min,0) ressource_constat_min,
        COALESCE(kw.ressource_solution_min,0) ressource_solution_min
    FROM spine s
    LEFT JOIN tm_daily        tm ON tm.channel_name = s.channel_name AND tm.day = s.day
    LEFT JOIN kw_daily        kw ON kw.channel_name = s.channel_name AND kw.day = s.day
    LEFT JOIN theorique_daily th ON th.channel_name = s.channel_name AND th.day = s.day
    LEFT JOIN channel_map     cm ON cm.channel_name = s.channel_name
    WHERE COALESCE(cm.channel_title, s.channel_name) NOT IN ('LN24','LATROIS','CANALZ')
)

SELECT
    DATE_TRUNC('week', CAST(day AS timestamp))                          AS "start",
    channel_name,
    COALESCE(channel_title, channel_name)                               AS channel_title,
    country,
    bool_or(has_grid)                                                   AS has_grid,

    SUM(denominator_min)                                                AS sum_duration_minutes,
    SUM(theorique_min)                                                  AS sum_theorique_min,
    SUM(time_monitored_min)                                             AS sum_time_monitored,
    SUM(monitored_in_perimeter_min)                                     AS sum_monitored_in_perimeter_min,
    ROUND(100.0 * SUM(monitored_in_perimeter_min)
                / NULLIF(SUM(theorique_min),0), 1)                      AS couverture_dictionnaire_pct,

    CAST(SUM(env_min)                AS float)/NULLIF(SUM(denominator_min),0) AS "% environnement total",
    CAST(SUM(climat_min)             AS float)/NULLIF(SUM(denominator_min),0) AS "% climat",
    CAST(SUM(climat_cause_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% climat cause",
    CAST(SUM(climat_adapt_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% climat solutions adaptation",
    CAST(SUM(climat_conseq_min)      AS float)/NULLIF(SUM(denominator_min),0) AS "% climat consequences",
    CAST(SUM(climat_atten_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% climat solutions attenuation",
    CAST(SUM(climat_constat_min)     AS float)/NULLIF(SUM(denominator_min),0) AS "% climat constat",
    CAST(SUM(biodiv_min)             AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversite",
    CAST(SUM(biodiv_constat_min)     AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité constat",
    CAST(SUM(biodiv_solution_min)    AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité solutions",
    CAST(SUM(biodiv_conseq_min)      AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité conséquences",
    CAST(SUM(biodiv_cause_min)       AS float)/NULLIF(SUM(denominator_min),0) AS "% biodiversité causes",
    CAST(SUM(ressource_min)          AS float)/NULLIF(SUM(denominator_min),0) AS "% ressources",
    CAST(SUM(ressource_constat_min)  AS float)/NULLIF(SUM(denominator_min),0) AS "% ressources constat",
    CAST(SUM(ressource_solution_min) AS float)/NULLIF(SUM(denominator_min),0) AS "% ressources solutions"
FROM daily
GROUP BY "start", channel_name, channel_title, country
ORDER BY "start" ASC, channel_title ASC