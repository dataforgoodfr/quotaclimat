import datetime
import logging
import os
import subprocess
from decimal import *

import psycopg2
import pytest

from my_dbt_project.pytest_tests.test_dbt_model_homepage import run_dbt_command


@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", ""),
        user=os.getenv("POSTGRES_USER", ""),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        host=os.getenv("POSTGRES_HOST", ""),
        port=os.getenv("POSTGRES_PORT", ""),
    )
    yield conn
    conn.close()


def seed_dbt_labelstudio():
    """Run dbt seed once before any test."""
    commands = [
        "seed",
        "--select",
        "labelstudio_task_aggregate",
        "--select",
        "labelstudio_task_completion_aggregate",
        "--full-refresh",
    ]
    logging.info(f"pytest running dbt seed : {commands}")
    run_dbt_command(commands)


seed_dbt_labelstudio()


@pytest.fixture(scope="module", autouse=True)
def run_task_global_completion():
    """Run dbt for the thematics model once before related tests."""
    logging.info("pytest running dbt task_global_completion")
    run_dbt_command(
        [
            "run",
            "--models",
            "task_global_completion",
            "--target",
            "analytics",
            "--full-refresh",
            "--debug",
        ]
    )


def test_task_global_completion(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
                "analytics"."task_global_completion"."task_completion_aggregate_id",
                "analytics"."task_global_completion"."country",
                "analytics"."task_global_completion"."data_item_channel_name",
                "analytics"."task_global_completion"."mesinfo_choice"
            FROM analytics.task_global_completion
            ORDER BY analytics.task_global_completion.task_completion_aggregate_id
            LIMIT 1
        """)
        row = cur.fetchone()
    
    expected = (
        '0e7ee7f70a223e21b10c0dad27464bebb8cc6a7f4bd5f5b7746c661a44ec7b45',
        "france",
        'europe1',
        "Correct"
    )

    assert row == expected, f"Unexpected values: {row}"
