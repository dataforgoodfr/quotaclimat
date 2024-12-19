from sqlalchemy import create_engine, text
import logging
from postgres.database_connection import get_db_session
# Define your database connection

session = get_db_session()

def create_materialized_views_mv_average_part_env_per_media_mobile(session):
    mv_average_part_env_per_media = text("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS environmental_avg_yearly AS
        SELECT
    DATE_TRUNC('month', CAST("source"."start" AS timestamp)) AS "start",
    AVG("source"."% environnement total") AS "avg"
    FROM
    (
        SELECT
        DATE_TRUNC('week', CAST("source"."start" AS timestamp)) AS "start",
        "source"."Program Metadata - Channel Name__channel_title" AS "Program Metadata - Channel Name__channel_title",
        CAST(100 * SUM("source"."sum") AS float) / NULLIF(
            SUM(
            "source"."Program Metadata - Channel Name__duration_minutes"
            ),
            0
        ) AS "% environnement total"
        FROM
        (
            SELECT
            CAST("source"."start" AS date) AS "start",
            "source"."Program Metadata - Channel Name__channel_program" AS "Program Metadata - Channel Name__channel_program",
            "source"."Program Metadata - Channel Name__channel_title" AS "Program Metadata - Channel Name__channel_title",
            "source"."Program Metadata - Channel Name__duration_minutes" AS "Program Metadata - Channel Name__duration_minutes",
            SUM("source"."crise_env_minutes") AS "sum"
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
                    CAST("public"."keywords"."start" AS date) >= CAST(
                    "Program Metadata - Channel Name"."program_grid_start" AS date
                    )
                )
                AND (
                    CAST("public"."keywords"."start" AS date) <= CAST(
                    "Program Metadata - Channel Name"."program_grid_end" AS date
                    )
                )
            ) AS "source"
        
    GROUP BY
            CAST("source"."start" AS date),
            "source"."Program Metadata - Channel Name__channel_program",
            "source"."Program Metadata - Channel Name__channel_title",
            "source"."Program Metadata - Channel Name__duration_minutes"
        
    ORDER BY
            CAST("source"."start" AS date) ASC,
            "source"."Program Metadata - Channel Name__channel_program" ASC,
            "source"."Program Metadata - Channel Name__channel_title" ASC,
            "source"."Program Metadata - Channel Name__duration_minutes" ASC
        ) AS "source"
        GROUP BY
        DATE_TRUNC('week', CAST("source"."start" AS timestamp)),
        "source"."Program Metadata - Channel Name__channel_title"
        ORDER BY
        DATE_TRUNC('week', CAST("source"."start" AS timestamp)) ASC,
        "source"."Program Metadata - Channel Name__channel_title" ASC
    ) AS "source"
    GROUP BY
    DATE_TRUNC('month', CAST("source"."start" AS timestamp))
    ORDER BY
    DATE_TRUNC('month', CAST("source"."start" AS timestamp)) ASC;
    """)
    with session.connect() as connection:
        logging.info(f"Creating materialized view {create_materialized_views_mv_average_part_env_per_media_mobile}")
        connection.execute(create_materialized_views_mv_average_part_env_per_media_mobile)