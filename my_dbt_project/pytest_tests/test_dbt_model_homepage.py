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

@pytest.fixture(scope="module", autouse=True)
def seed_dbt():
    """Run dbt seed once before any test."""
    
    commands = ["seed", "--select", "program_metadata,", "keywords", "--full-refresh"]
    logging.info(f"pytest running dbt seed : {commands}")
    run_dbt_command(commands)


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

    assert count == 2, "count error" #TODO
 
def test_core_query_thematics_keywords_count(db_connection):
    """Test the materialized view using dbt and pytest."""

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.core_query_thematics_keywords;") 
    count = cur.fetchone()[0]
    cur.close()

    assert count == 108, "count error"

def test_core_query_thematics_keywords_values(db_connection):
    """Test row count and sample values for core_query_thematics_keywords."""

    with db_connection.cursor() as cur:
        # Check a specific row (e.g., first one)
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