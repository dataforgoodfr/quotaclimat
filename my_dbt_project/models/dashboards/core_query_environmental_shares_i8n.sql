{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
  )
}}

SELECT
  DATE_TRUNC('week', CAST("source"."start" AS timestamp)) AS "start",
  "source"."channel_name" AS "channel_name",
  "source"."channel_title" AS "channel_title",
  SUM("source"."time_monitored__duration_minutes") AS "sum_duration_minutes",
  CAST(SUM("source"."sum") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% environnement total",
  CAST(SUM("source"."sum_2") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% climat",
  CAST(SUM("source"."sum_8") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% climat cause",
  CAST(SUM("source"."sum_5") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% climat solutions adaptation ",
  CAST(SUM("source"."sum_7") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% climat consequences",
  CAST(SUM("source"."sum_6") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% climat solutions attenuation",
  CAST(SUM("source"."sum_9") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% climat constat",
  CAST(SUM("source"."sum_3") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% biodiversite",
  CAST(SUM("source"."sum_13") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% biodiversité constat",
  CAST(SUM("source"."sum_10") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% biodiversité solutions",
  CAST(SUM("source"."sum_11") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% biodiversité conséquences",
  CAST(SUM("source"."sum_12") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% biodiversité causes",
  CAST(SUM("source"."sum_4") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% ressources",
  CAST(SUM("source"."sum_15") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% ressources constat",
  CAST(SUM("source"."sum_14") AS float) / NULLIF(
    SUM(
      "source"."time_monitored__duration_minutes"
    ),
    0
  ) "% ressources solutions",
  "source"."country" AS "country"
FROM
  (
    SELECT
      CAST("source"."start" AS date) AS "start",
      "source"."channel_name" AS "channel_name",
      "source"."channel_title" AS "channel_title",
      "source"."time_monitored__duration_minutes" AS "time_monitored__duration_minutes",
      "source"."country" AS "country",
      SUM("source"."crise_env_minutes") AS "sum",
      SUM("source"."climat_total_minutes") AS "sum_2",
      SUM("source"."biodiversite_total_minutes") AS "sum_3",
      SUM("source"."ressource_total_minutes") AS "sum_4",
      SUM("source"."climat_adaptation_minutes") AS "sum_5",
      SUM("source"."climat_attenuation_minutes") AS "sum_6",
      SUM("source"."climat_consequence_minutes") AS "sum_7",
      SUM("source"."climat_cause_minutes") AS "sum_8",
      SUM("source"."climat_constat_minutes") AS "sum_9",
      SUM("source"."biodiversite_solution_minutes") AS "sum_10",
      SUM("source"."biodiversite_consequence_minutes") AS "sum_11",
      SUM("source"."biodiversite_cause_minutes") AS "sum_12",
      SUM("source"."biodiversite_concept_minutes") AS "sum_13",
      SUM("source"."ressource_solution_minute") AS "sum_14",
      SUM("source"."ressource_minutes") AS "sum_15"
    FROM
      (
        SELECT
          "public"."keywords"."id" AS "id",
          "public"."keywords"."start" AS "start",
          "public"."keywords"."plaintext" AS "plaintext",
          "public"."keywords"."theme" AS "theme",
          "public"."keywords"."keywords_with_timestamp" AS "keywords_with_timestamp",
          "public"."keywords"."number_of_keywords" AS "number_of_keywords",
          "public"."keywords"."number_of_changement_climatique_constat" AS "number_of_changement_climatique_constat",
          "public"."keywords"."number_of_changement_climatique_causes_directes" AS "number_of_changement_climatique_causes_directes",
          "public"."keywords"."number_of_changement_climatique_consequences" AS "number_of_changement_climatique_consequences",
          "public"."keywords"."number_of_adaptation_climatique_solutions_directes" AS "number_of_adaptation_climatique_solutions_directes",
          "public"."keywords"."number_of_attenuation_climatique_solutions_directes" AS "number_of_attenuation_climatique_solutions_directes",
          "public"."keywords"."number_of_biodiversite_concepts_generaux" AS "number_of_biodiversite_concepts_generaux",
          "public"."keywords"."number_of_biodiversite_causes_directes" AS "number_of_biodiversite_causes_directes",
          "public"."keywords"."number_of_biodiversite_consequences" AS "number_of_biodiversite_consequences",
          "public"."keywords"."number_of_biodiversite_solutions_directes" AS "number_of_biodiversite_solutions_directes",
          "public"."keywords"."number_of_ressources" AS "number_of_ressources",
          "public"."keywords"."number_of_ressources_solutions" AS "number_of_ressources_solutions",
          CAST(
            "public"."keywords"."number_of_keywords" * 20 AS float
          ) / 60.0 AS "crise_env_minutes",
          CAST(
            "public"."keywords"."number_of_changement_climatique_constat" * 20 AS float
          ) / 60.0 AS "climat_constat_minutes",
          CAST(
            "public"."keywords"."number_of_changement_climatique_causes_directes" * 20 AS float
          ) / 60.0 AS "climat_cause_minutes",
          CAST(
            "public"."keywords"."number_of_changement_climatique_consequences" * 20 AS float
          ) / 60.0 AS "climat_consequence_minutes",
          CAST(
            "public"."keywords"."number_of_biodiversite_concepts_generaux" * 20 AS float
          ) / 60.0 AS "biodiversite_concept_minutes",
          CAST(
            "public"."keywords"."number_of_biodiversite_causes_directes" * 20 AS float
          ) / 60.0 AS "biodiversite_cause_minutes",
          CAST(
            "public"."keywords"."number_of_biodiversite_consequences" * 20 AS float
          ) / 60.0 AS "biodiversite_consequence_minutes",
          CAST(
            "public"."keywords"."number_of_biodiversite_solutions_directes" * 20 AS float
          ) / 60.0 AS "biodiversite_solution_minutes",
          CAST(
            "public"."keywords"."number_of_attenuation_climatique_solutions_directes" * 20 AS float
          ) / 60.0 AS "climat_attenuation_minutes",
          CAST(
            "public"."keywords"."number_of_adaptation_climatique_solutions_directes" * 20 AS float
          ) / 60.0 AS "climat_adaptation_minutes",
          CAST(
            "public"."keywords"."number_of_ressources" * 20 AS float
          ) / 60.0 AS "ressource_minutes",
          CAST(
            "public"."keywords"."number_of_ressources_solutions" * 20 AS float
          ) / 60.0 AS "ressource_solution_minute",
          CAST(
            "public"."keywords"."number_of_keywords_climat" * 20 AS float
          ) / 60.0 AS "climat_total_minutes",
          CAST(
            "public"."keywords"."number_of_keywords_biodiversite" * 20 AS float
          ) / 60.0 AS "biodiversite_total_minutes",
          CAST(
            "public"."keywords"."number_of_keywords_ressources" * 20 AS float
          ) / 60.0 AS "ressource_total_minutes",
          "public"."keywords"."channel_name" AS "channel_name",
          "time_monitored"."duration_minutes" AS "time_monitored__duration_minutes"
          ,"public"."keywords"."channel_title" AS "channel_title"
          ,"public"."keywords"."country" AS "country"
        FROM
          "public"."keywords"
        INNER JOIN 
            "public"."time_monitored" AS "time_monitored"
            ON "public"."keywords"."channel_name" = "time_monitored"."channel_name"
            AND "public"."keywords"."country" = "time_monitored"."country"
            AND DATE("public"."keywords"."start") = DATE("time_monitored"."start")
      ) AS "source"
   
GROUP BY
      CAST("source"."start" AS date),
      "source"."channel_name",
      "source"."channel_title",
      "source"."time_monitored__duration_minutes",
      "source"."country"
  ) AS "source"
GROUP BY
  DATE_TRUNC('week', CAST("source"."start" AS timestamp)),
  "source"."channel_name",
  "source"."channel_title",
  "source"."country"
ORDER BY
  DATE_TRUNC('week', CAST("source"."start" AS timestamp)) ASC,
  "source"."channel_name" ASC