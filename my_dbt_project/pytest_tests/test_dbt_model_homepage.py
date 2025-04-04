import pytest
import psycopg2
import os
import subprocess

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

def test_homepage_environment_by_media_by_month(db_connection):
    """Test the materialized view using dbt and pytest."""

    run_dbt_command(["seed"])

    run_dbt_command(["run", "--models", "homepage_environment_by_media_by_month"])

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.homepage_environment_by_media_by_month;")
    count = cur.fetchone()[0]
    cur.close()

    assert count == 3, "count error"

def test_core_query_environmental_shares(db_connection):
    """Test the materialized view using dbt and pytest."""

    run_dbt_command(["seed"])

    run_dbt_command(["run", "--models", "core_query_environmental_shares"])

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.core_query_environmental_shares;") 
    count = cur.fetchone()[0]
    cur.close()

    assert count == 3, "count error" #TODO
 
def test_core_query_thematics_keywords(db_connection):
    """Test the materialized view using dbt and pytest."""

    run_dbt_command(["seed"])

    run_dbt_command(["run", "--models", "core_query_thematics_keywords"])

    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM public.core_query_thematics_keywords;") 
    count = cur.fetchone()[0]
    cur.close()

    assert count == 3, "count error" #TODO
