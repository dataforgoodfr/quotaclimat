import pytest
import psycopg2
import os
import subprocess
import datetime
import logging
from decimal import *

@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB",""),
        user= os.getenv("POSTGRES_USER",""),
        password=os.getenv("POSTGRES_PASSWORD",""),
        host=os.getenv("POSTGRES_HOST",""),
        port=os.getenv("POSTGRES_PORT","")
    )
    yield conn
    conn.close()

def run_dbt_command(command_args):
    """Helper function to run dbt commands."""

    # , "--project-dir", "/app/my_dbt_project" used by DBT_PROJECT_DIR
    result = subprocess.run(
        ["poetry", "run", "dbt",  *command_args],
        capture_output=True, text=True
    )
    print(result.stdout)  # Print dbt logs for debugging
    assert result.returncode == 0, f"dbt command failed: {result.stderr}"

# @pytest.fixture(scope="module", autouse=True)
def seed_dbt():
    """Run dbt seed once before any test."""
    commands = ["seed", "--select", "program_metadata","--select", "time_monitored","--select", "keywords","--select", "dictionary", "--full-refresh"]
    logging.info(f"pytest running dbt seed : {commands}")
    run_dbt_command(commands)


seed_dbt()

@pytest.fixture(scope="module", autouse=True)
def run_core_query_thematics_keywords():
    """Run dbt for the thematics model once before related tests."""
    logging.info("pytest running dbt core_query_thematics_keywords")
    run_dbt_command(["run", "--models", "core_query_thematics_keywords", "--full-refresh","--debug"])

@pytest.fixture(scope="module", autouse=True)
def run_core_query_thematics_keywords_i8n():
    """Run dbt for the thematics model once before related tests."""
    logging.info("pytest running dbt core_query_thematics_keywords_i8n")
    run_dbt_command(["run", "--models", "core_query_thematics_keywords_i8n", "--full-refresh","--debug"])

@pytest.fixture(scope="module", autouse=True)
def run_core_query_environmental_shares():
    """Run dbt for the environmental shares model once before related tests."""
    commands = ["run", "--models", "core_query_environmental_shares", "--full-refresh","--debug"]
    logging.info(f"pytest running dbt core_query_environmental_shares {commands}")

    run_dbt_command(commands)


@pytest.fixture(scope="module", autouse=True)
def run_core_query_environmental_shares_i8n():
    """Run dbt for the environmental shares model once before related tests."""
    commands = ["run", "--models", "core_query_environmental_shares_i8n", "--full-refresh"]
    logging.info(f"pytest running dbt core_query_environmental_shares_i8n {commands}")

    run_dbt_command(commands)

@pytest.fixture(scope="module", autouse=True)
def run_homepage_environment_by_media_by_month():
    """Run dbt for the environmental shares model once before related tests."""
    commands = ["run", "--models", "homepage_environment_by_media_by_month", "--full-refresh","--debug"]
    logging.info(f"pytest running dbt homepage_environment_by_media_by_month {commands}")

    run_dbt_command(commands)


def test_homepage_environment_by_media_by_month(db_connection):
    """Test the materialized view using dbt and pytest."""

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.homepage_environment_by_media_by_month;")
    count = cur.fetchone()[0]
    cur.close()

    assert count == 3, "count error"

def test_core_query_environmental_shares(db_connection):
    """Test the materialized view using dbt and pytest."""

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.core_query_environmental_shares;") 
    count = cur.fetchone()[0]
    cur.close()

    assert count == 2, "count error"
 
def test_core_query_thematics_keywords_count(db_connection):
    """Test the materialized view using dbt and pytest."""

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.core_query_thematics_keywords;") 
    count = cur.fetchone()[0]
    cur.close()

    assert count == 108, "count error"

def test_core_query_thematics_keywords_values_tf1(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT channel_title, week, crise_type, theme, category, keyword, count,
            high_risk_of_false_positive, sum_duration_minutes
            FROM public.core_query_thematics_keywords
            WHERE channel_title = 'TF1' AND keyword = 'eau' AND theme = 'changement_climatique_constat'
            ORDER BY channel_title DESC
            LIMIT 1
        """)
        row = cur.fetchone()

        expected= (
        'TF1',
        datetime.date(2025, 1, 27),
        'Crise climatique',
        'changement_climatique_constat',
        'Transversal',
        'eau',
        4,
        True,
        1650)
        
        expected_trimmed = expected[:-1] 
        row_trimmed = row[:-1]
        assert row_trimmed == expected_trimmed, f"Unexpected values: {row}"

def test_core_query_thematics_keywords_values_arte(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT channel_title, week, crise_type, theme, category, keyword, count,
            high_risk_of_false_positive, sum_duration_minutes
            FROM public.core_query_thematics_keywords
            WHERE channel_title = 'Arte' AND keyword = 'eau' AND theme = 'changement_climatique_constat'
            ORDER BY channel_title DESC
            LIMIT 1
        """)
        row = cur.fetchone()

        expected= (
        'Arte',
        datetime.date(2025, 1, 27),
        'Crise climatique',
        'changement_climatique_constat',
        'Transversal',
        'eau',
        2,
        True,
        455)
        
        expected_trimmed = expected[:-1] 
        row_trimmed = row[:-1]
        assert row_trimmed == expected_trimmed, f"Unexpected values: {row}"


# zinc is indirect in the dictionary table, it's a good test to check direct join between keywords and dictionary
def test_core_query_thematics_keywords_values_arte_zinc(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT theme, category, count, sum_duration_minutes
            FROM public.core_query_thematics_keywords
            WHERE channel_title = 'Arte'
              AND keyword = 'zinc'
              AND week = '2025-01-27'
            ORDER BY theme, category
        """)
        rows = cur.fetchall()

        expected = [
            ('biodiversite_causes', 'Pollution', 1, 455),
            ('ressources', 'Air', 1, 455),
            ('ressources', 'Eau', 1, 455),
            ('ressources', 'Sols', 1, 455),
        ]

        assert rows == expected, f"Unexpected zinc rows: {rows}"

def test_core_query_thematics_keywords_values_i8n(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT channel_title, week, crise_type, theme, category, keyword, count,
            high_risk_of_false_positive,
            country
            FROM public.core_query_thematics_keywords_i8n
            WHERE channel_title = 'TF1' AND keyword = 'eau'
            ORDER BY channel_title DESC
            LIMIT 1
        """)
        row = cur.fetchone()

        expected=   (
        'TF1',
        datetime.date(2025, 1, 27),
        'Crise climatique',
        'changement_climatique_constat',
        'Transversal',
        'eau',
        4, 
        True
        ,"france")
        expected_trimmed = expected[: -2] + expected[-1:]
        row_trimmed = row[: -2] + row[-1:]

        assert row_trimmed == expected_trimmed, f"Unexpected values: {row}"

def test_core_query_environmental_shares_values(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
            "public"."core_query_environmental_shares"."start" AS "start",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__channel_title" AS "Program Metadata - Channel Name__channel_title",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__public" AS "Program Metadata - Channel Name__public",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__infocontinue" AS "Program Metadata - Channel Name__infocontinue",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__radio" AS "Program Metadata - Channel Name__radio",
            ROUND(CAST("public"."core_query_environmental_shares"."% environnement total" AS numeric),4) AS "% environnement total",
            ROUND(CAST("public"."core_query_environmental_shares"."% climat" AS numeric),4) AS "% climat",
            ROUND(CAST("public"."core_query_environmental_shares"."% climat cause" AS numeric),4) AS "% climat cause",
            ROUND(CAST("public"."core_query_environmental_shares"."% climat solutions adaptation " AS numeric),4) AS "% climat solutions adaptation ",
            ROUND(CAST("public"."core_query_environmental_shares"."% climat consequences" AS numeric),4) AS "% climat consequences",
            ROUND(CAST("public"."core_query_environmental_shares"."% climat solutions attenuation" AS numeric),4) AS "% climat solutions attenuation",
            ROUND(CAST("public"."core_query_environmental_shares"."% climat constat" AS numeric),4) AS "% climat constat",
            ROUND(CAST("public"."core_query_environmental_shares"."% biodiversite" AS numeric),4) AS "% biodiversite",
            ROUND(CAST("public"."core_query_environmental_shares"."% biodiversité constat" AS numeric),4) AS "% biodiversité constat",
            ROUND(CAST("public"."core_query_environmental_shares"."% biodiversité solutions" AS numeric),4) AS "% biodiversité solutions",
            ROUND(CAST("public"."core_query_environmental_shares"."% biodiversité conséquences" AS numeric),4) AS "% biodiversité conséquences",
            ROUND(CAST("public"."core_query_environmental_shares"."% biodiversité causes" AS numeric),4) AS "% biodiversité causes",
            ROUND(CAST("public"."core_query_environmental_shares"."% ressources" AS numeric),4) AS "% ressources",
            ROUND(CAST("public"."core_query_environmental_shares"."% ressources constat" AS numeric),4) AS "% ressources constat",
            ROUND(CAST("public"."core_query_environmental_shares"."% ressources solutions" AS numeric),4) AS "% ressources solutions"
            FROM
                "public"."core_query_environmental_shares"
            WHERE "Program Metadata - Channel Name__channel_title" = 'TF1'
            LIMIT 1
        """)
        row = cur.fetchone()

         

        expected = (datetime.datetime(2025, 1, 27, 0, 0), 'TF1', False, False, False, Decimal('0.1333'),Decimal('0.1111'),Decimal('0.0074'),Decimal('0.0000'),Decimal('0.0074'),Decimal('0.0000'),Decimal('0.0963'),Decimal('0.0667'),Decimal('0.0667'),Decimal('0.0000'),Decimal('0.0000'),Decimal('0.0000'),Decimal('0.0593'),Decimal('0.0593'),Decimal('0.0222'))
        assert row == expected, f"Unexpected values: {row}"

def test_core_query_environmental_shares_values_i8n(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
            "public"."core_query_environmental_shares_i8n"."start" AS "start",
            "public"."core_query_environmental_shares_i8n"."channel_title" AS "channel_title",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% environnement total" AS numeric),4) AS "% environnement total",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% climat" AS numeric),4) AS "% climat",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% climat cause" AS numeric),4) AS "% climat cause",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% climat solutions adaptation " AS numeric),4) AS "% climat solutions adaptation ",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% climat consequences" AS numeric),4) AS "% climat consequences",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% climat solutions attenuation" AS numeric),4) AS "% climat solutions attenuation",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% climat constat" AS numeric),4) AS "% climat constat",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% biodiversite" AS numeric),4) AS "% biodiversite",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% biodiversité constat" AS numeric),4) AS "% biodiversité constat",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% biodiversité solutions" AS numeric),4) AS "% biodiversité solutions",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% biodiversité conséquences" AS numeric),4) AS "% biodiversité conséquences",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% biodiversité causes" AS numeric),4) AS "% biodiversité causes",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% ressources" AS numeric),4) AS "% ressources",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% ressources constat" AS numeric),4) AS "% ressources constat",
            ROUND(CAST("public"."core_query_environmental_shares_i8n"."% ressources solutions" AS numeric),4) AS "% ressources solutions"
            ,"public"."core_query_environmental_shares_i8n"."country"
            FROM
                "public"."core_query_environmental_shares_i8n"
            WHERE "channel_title" = 'TF1'
            LIMIT 1
        """)
        row = cur.fetchone()
        expected = (datetime.datetime(2025, 1, 27, 0, 0), 'TF1', Decimal('0.0233'), Decimal('0.0194'), Decimal('0.0013'), Decimal('0.0000'), Decimal('0.0013'), Decimal('0.0000'), Decimal('0.0168'), Decimal('0.0116'), Decimal('0.0116'), Decimal('0.0000'), Decimal('0.0000'), Decimal('0.0000'), Decimal('0.0103'), Decimal('0.0103'), Decimal('0.0039'), 'france')
        # expected = (datetime.datetime(2025, 1, 27, 0, 0), 'TF1', Decimal('0.1333'),Decimal('0.1111'),Decimal('0.0074'),Decimal('0.0000'),Decimal('0.0074'),Decimal('0.0000'),Decimal('0.0963'),Decimal('0.0667'),Decimal('0.0667'),Decimal('0.0000'),Decimal('0.0000'),Decimal('0.0000'),Decimal('0.0593'),Decimal('0.0593'),Decimal('0.0222'), "france")
        assert row == expected, f"Unexpected values: {row}"