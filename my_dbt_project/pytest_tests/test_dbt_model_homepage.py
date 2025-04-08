import pytest
import psycopg2
import os
import subprocess
import datetime
import logging

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


def seed_dbt():
    """Run dbt seed once before any test."""
    commands = ["seed", "--select", "program_metadata","--select", "keywords", "--full-refresh"]
    logging.info(f"pytest running dbt seed : {commands}")
    run_dbt_command(commands)


seed_dbt()

@pytest.fixture(scope="module", autouse=True)
def run_core_query_thematics_keywords():
    """Run dbt for the thematics model once before related tests."""
    logging.info("pytest running dbt core_query_thematics_keywords")
    run_dbt_command(["run", "--models", "core_query_thematics_keywords", "--full-refresh"])

@pytest.fixture(scope="module", autouse=True)
def run_core_query_environmental_shares():
    """Run dbt for the environmental shares model once before related tests."""
    commands = ["run", "--models", "core_query_environmental_shares", "--full-refresh"]
    logging.info(f"pytest running dbt core_query_environmental_shares {commands}")

    run_dbt_command(commands)

@pytest.fixture(scope="module", autouse=True)
def run_homepage_environment_by_media_by_month():
    """Run dbt for the environmental shares model once before related tests."""
    commands = ["run", "--models", "homepage_environment_by_media_by_month", "--full-refresh"]
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

def test_core_query_thematics_keywords_values(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT channel_title, week, crise_type, theme, category, keyword, count
            FROM public.core_query_thematics_keywords
            WHERE channel_title = 'TF1' AND keyword = 'eau'
            ORDER BY channel_title DESC
            LIMIT 1
        """)
        row = cur.fetchone()

        expected = ('TF1', datetime.date(2025, 1, 27), 'Crise climatique', 'changement_climatique_constat', 'Transversal', 'eau', 4)
        assert row == expected, f"Unexpected values: {row}"

def test_core_query_environmental_shares_values(db_connection):

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
            "public"."core_query_environmental_shares"."start" AS "start",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__channel_title" AS "Program Metadata - Channel Name__channel_title",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__public" AS "Program Metadata - Channel Name__public",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__infocontinue" AS "Program Metadata - Channel Name__infocontinue",
            "public"."core_query_environmental_shares"."Program Metadata - Channel Name__radio" AS "Program Metadata - Channel Name__radio",
            "public"."core_query_environmental_shares"."% environnement total" AS "% environnement total",
            "public"."core_query_environmental_shares"."% climat" AS "% climat",
            "public"."core_query_environmental_shares"."% climat cause" AS "% climat cause",
            "public"."core_query_environmental_shares"."% climat solutions adaptation " AS "% climat solutions adaptation ",
            "public"."core_query_environmental_shares"."% climat consequences" AS "% climat consequences",
            "public"."core_query_environmental_shares"."% climat solutions attenuation" AS "% climat solutions attenuation",
            "public"."core_query_environmental_shares"."% climat constat" AS "% climat constat",
            "public"."core_query_environmental_shares"."% biodiversite" AS "% biodiversite",
            "public"."core_query_environmental_shares"."% biodiversité constat" AS "% biodiversité constat",
            "public"."core_query_environmental_shares"."% biodiversité solutions" AS "% biodiversité solutions",
            "public"."core_query_environmental_shares"."% biodiversité conséquences" AS "% biodiversité conséquences",
            "public"."core_query_environmental_shares"."% biodiversité causes" AS "% biodiversité causes",
            "public"."core_query_environmental_shares"."% ressources" AS "% ressources",
            "public"."core_query_environmental_shares"."% ressources constat" AS "% ressources constat",
            "public"."core_query_environmental_shares"."% ressources solutions" AS "% ressources solutions"
            FROM
                "public"."core_query_environmental_shares"
            WHERE "Program Metadata - Channel Name__channel_title" = 'TF1'
            LIMIT 1
        """)
        row = cur.fetchone()
        expected = (datetime.datetime(2025, 1, 27, 0, 0), 'TF1', False, False, False, 0.1333333333333333, 0.1111111111111111, 0.007407407407407407, 0.0, 0.007407407407407407, 0.0, 0.0962962962962963, 0.06666666666666665, 0.06666666666666665, 0.0, 0.0, 0.0, 0.059259259259259255, 0.059259259259259255, 0.022222222222222223)
        assert row == expected, f"Unexpected values: {row}"