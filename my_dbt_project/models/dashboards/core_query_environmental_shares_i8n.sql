{{ config(
    materialized='incremental'
    ,unique_key=['start','channel_title']
  )
}}

SELECT
  DATE_TRUNC('week', CAST("source"."start" AS timestamp)) AS "start",
  "source"."Program Metadata - Channel Name__channel_title" AS "Program Metadata - Channel Name__channel_title",
  "source"."Program Metadata - Channel Name__public" AS "Program Metadata - Channel Name__public",
  "source"."Program Metadata - Channel Name__infocontinue" AS "Program Metadata - Channel Name__infocontinue",
  "source"."Program Metadata - Channel Name__radio" AS "Program Metadata - Channel Name__radio",
  CAST(SUM("source"."sum") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% environnement total",
  CAST(SUM("source"."sum_2") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% climat",
  CAST(SUM("source"."sum_8") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% climat cause",
  CAST(SUM("source"."sum_5") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% climat solutions adaptation ",
  CAST(SUM("source"."sum_7") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% climat consequences",
  CAST(SUM("source"."sum_6") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% climat solutions attenuation",
  CAST(SUM("source"."sum_9") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% climat constat",
  CAST(SUM("source"."sum_3") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% biodiversite",
  CAST(SUM("source"."sum_13") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% biodiversité constat",
  CAST(SUM("source"."sum_10") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% biodiversité solutions",
  CAST(SUM("source"."sum_11") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% biodiversité conséquences",
  CAST(SUM("source"."sum_12") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% biodiversité causes",
  CAST(SUM("source"."sum_4") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% ressources",
  CAST(SUM("source"."sum_4") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% ressources constat",
  CAST(SUM("source"."sum_14") AS float) / NULLIF(
    SUM(
      "source"."Program Metadata - Channel Name__duration_minutes"
    ),
    0
  ) "% ressources solutions",
  "source"."country" AS "country"
FROM
  (
    SELECT
      CAST("source"."start" AS date) AS "start",
      "source"."Program Metadata - Channel Name__channel_program" AS "Program Metadata - Channel Name__channel_program",
      "source"."Program Metadata - Channel Name__channel_title" AS "Program Metadata - Channel Name__channel_title",
      "source"."Program Metadata - Channel Name__duration_minutes" AS "Program Metadata - Channel Name__duration_minutes",
      "source"."Program Metadata - Channel Name__channel_program_type" AS "Program Metadata - Channel Name__channel_program_type",
      "source"."Program Metadata - Channel Name__public" AS "Program Metadata - Channel Name__public",
      "source"."Program Metadata - Channel Name__infocontinue" AS "Program Metadata - Channel Name__infocontinue",
      "source"."Program Metadata - Channel Name__radio" AS "Program Metadata - Channel Name__radio",
      "source"."country" AS "country",
      SUM("source"."crise_env_minutes") AS "sum",
      SUM("source"."climat_total_minutes") AS "sum_2",
      SUM("source"."biodiversite_total_minutes") AS "sum_3",
      SUM("source"."ressource_minutes") AS "sum_4",
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
          "Program Metadata - Channel Name"."duration_minutes" AS "Program Metadata - Channel Name__duration_minutes",
          "Program Metadata - Channel Name"."weekday" AS "Program Metadata - Channel Name__weekday",
          "Program Metadata - Channel Name"."public" AS "Program Metadata - Channel Name__public",
          "Program Metadata - Channel Name"."infocontinue" AS "Program Metadata - Channel Name__infocontinue",
          "Program Metadata - Channel Name"."radio" AS "Program Metadata - Channel Name__radio",
          "Program Metadata - Channel Name"."start" AS "Program Metadata - Channel Name__start",
          "Program Metadata - Channel Name"."end" AS "Program Metadata - Channel Name__end",
          "Program Metadata - Channel Name"."channel_title" AS "Program Metadata - Channel Name__channel_title",
          "Program Metadata - Channel Name"."channel_program" AS "Program Metadata - Channel Name__channel_program",
          "Program Metadata - Channel Name"."channel_program_type" AS "Program Metadata - Channel Name__channel_program_type"
          ,"public"."keywords"."country" AS "country"
        FROM
          "public"."keywords"
          INNER JOIN "public"."program_metadata" AS "Program Metadata - Channel Name" ON (
            "public"."keywords"."channel_name" = "Program Metadata - Channel Name"."channel_name"
          )
         
   AND (
            "public"."keywords"."channel_program" = "Program Metadata - Channel Name"."channel_program"
          )
          AND (
            "public"."keywords"."channel_program_type" = "Program Metadata - Channel Name"."channel_program_type"
          )
          AND (
            COALESCE(
              NULLIF(
                (
                  (
                    (
                      CAST(
                        extract(
                          dow
                          from
                            "public"."keywords"."start"
                        ) AS integer
                      ) + 1
                    ) + 6
                  ) % 7
                ),
                0
              ),
              7
            ) = "Program Metadata - Channel Name"."weekday"
          )
          AND (
            CAST(
              CAST("public"."keywords"."start" AS date) AS timestamptz
            ) >= CAST(
              CAST(
                "Program Metadata - Channel Name"."program_grid_start" AS date
              ) AS timestamp
            )
          )
          AND (
            CAST(
              CAST("public"."keywords"."start" AS date) AS timestamptz
            ) <= CAST(
              CAST(
                "Program Metadata - Channel Name"."program_grid_end" AS date
              ) AS timestamp
            )
          )
      ) AS "source"
   
GROUP BY
      CAST("source"."start" AS date),
      "source"."Program Metadata - Channel Name__channel_program",
      "source"."Program Metadata - Channel Name__channel_title",
      "source"."Program Metadata - Channel Name__duration_minutes",
      "source"."Program Metadata - Channel Name__channel_program_type",
      "source"."Program Metadata - Channel Name__public",
      "source"."Program Metadata - Channel Name__infocontinue",
      "source"."Program Metadata - Channel Name__radio",
      "source"."country"
   
ORDER BY
      CAST("source"."start" AS date) ASC,
      "source"."Program Metadata - Channel Name__channel_program" ASC,
      "source"."Program Metadata - Channel Name__channel_title" ASC,
      "source"."Program Metadata - Channel Name__duration_minutes" ASC,
      "source"."Program Metadata - Channel Name__channel_program_type" ASC,
      "source"."Program Metadata - Channel Name__public" ASC,
      "source"."Program Metadata - Channel Name__infocontinue" ASC,
      "source"."Program Metadata - Channel Name__radio" ASC
  ) AS "source"
GROUP BY
  DATE_TRUNC('week', CAST("source"."start" AS timestamp)),
  "source"."Program Metadata - Channel Name__channel_title",
  "source"."Program Metadata - Channel Name__public",
  "source"."Program Metadata - Channel Name__infocontinue",
  "source"."Program Metadata - Channel Name__radio",
  "source"."country"
ORDER BY
  DATE_TRUNC('week', CAST("source"."start" AS timestamp)) ASC,
  "source"."Program Metadata - Channel Name__channel_title" ASC,
  "source"."Program Metadata - Channel Name__public" ASC,
  "source"."Program Metadata - Channel Name__infocontinue" ASC,
  "source"."Program Metadata - Channel Name__radio" ASC