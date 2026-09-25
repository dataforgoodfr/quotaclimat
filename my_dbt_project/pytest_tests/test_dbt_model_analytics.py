import datetime
import logging
import os
import subprocess

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
    # seed and dbt run upstream tables
    commands = [
        "seed",
        "--select",
        "program_metadata",
        "--select",
        "time_monitored",
        "--select",
        "keywords",
        "--select",
        "dictionary",
        "--select",
        "keyword_macro_category",
        "--full-refresh",
    ]
    run_dbt_command(commands)

seed_dbt_labelstudio()

GRANT_ROLES = ["rrs-read-dev", "rrs-read-prod", "climateguard-reader-user"]


@pytest.fixture(scope="module")
def create_test_roles(db_connection):
    with db_connection.cursor() as cur:
        for role in GRANT_ROLES:
            cur.execute(f"""
                DO $$ BEGIN
                    CREATE ROLE "{role}";
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$;
            """)
    db_connection.commit()
    yield


AD_CLASSIFICATION_TABLE = "download_classification_pub_ome_20260716171520"


@pytest.fixture(scope="module")
def create_advertising_tables(db_connection):
    """The advertising tables are created by alembic in production, which the dbt CI job
    does not run: create them here (same columns as postgres/schemas/advertising/models.py)
    with a few test rows, dated 2000-01-01 to not mix with real data in a local database."""
    with db_connection.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS advertising")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS advertising.ad (
                id text PRIMARY KEY,
                first_detection_date timestamp NOT NULL,
                duration_sec double precision NOT NULL,
                chunks json NOT NULL,
                fragment_type varchar NOT NULL,
                transcript text,
                prediction json,
                prediction_status varchar,
                prediction_confidence double precision,
                predicted_sector varchar,
                predicted_product_category varchar,
                predicted_brand varchar,
                prediction_method varchar
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS advertising.ad_occurrence (
                id text PRIMARY KEY,
                deleted_at timestamp,
                occurrence_date timestamp NOT NULL,
                channel_name varchar NOT NULL,
                ad_id text REFERENCES advertising.ad (id)
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS public.{AD_CLASSIFICATION_TABLE} (
                sector_code text, cat_code text, sector_label_fr text, product_category_fr text
            )
        """)
        cur.execute(f"DELETE FROM public.{AD_CLASSIFICATION_TABLE}")
        cur.execute(f"""
            INSERT INTO public.{AD_CLASSIFICATION_TABLE} VALUES
                ('PYTEST_AUTO', 'PYTEST_AUTO_EV', 'Automobile', 'Voiture électrique'),
                ('PYTEST_AUTO', 'PYTEST_AUTO_ICE', 'Automobile', 'Voiture thermique'),
                ('PYTEST_FOOD', 'PYTEST_FOOD_SNACK', 'Alimentation', 'Snacks')
        """)
        cur.execute("DELETE FROM advertising.ad_occurrence WHERE id LIKE 'pytest_%'")
        cur.execute("DELETE FROM advertising.ad WHERE id LIKE 'pytest_%'")
        cur.execute("""
            INSERT INTO advertising.ad (
                id, first_detection_date, duration_sec, chunks, fragment_type,
                prediction_status, predicted_sector, predicted_product_category
            ) VALUES
                ('pytest_ad_1', '2000-01-01', 30, '[]', 'AD', 'subcat_done', 'PYTEST_AUTO', 'PYTEST_AUTO_EV'),
                ('pytest_ad_2', '2000-01-01', 20, '[]', 'AD', 'dict_tier1', 'PYTEST_FOOD', NULL),
                ('pytest_ad_3', '2000-01-01', 10, '[]', 'AD', 'pending', NULL, NULL),
                ('pytest_other', '2000-01-01', 15, '[]', 'OTHER', 'pending', NULL, NULL)
        """)
        cur.execute("""
            INSERT INTO advertising.ad_occurrence (id, deleted_at, occurrence_date, channel_name, ad_id) VALUES
                -- tunnel 1: 10:00:00 -> 10:00:52
                ('pytest_occ_1', NULL, '2000-01-01 10:00:00', 'arte', 'pytest_ad_1'),
                ('pytest_occ_1_duplicate', NULL, '2000-01-01 10:00:00', 'arte', 'pytest_ad_1'),
                ('pytest_occ_2', NULL, '2000-01-01 10:00:32', 'arte', 'pytest_ad_2'),
                ('pytest_occ_3_overlap', NULL, '2000-01-01 10:00:35', 'arte', 'pytest_ad_3'),
                -- deleted: ignored
                ('pytest_occ_deleted', '2000-01-02', '2000-01-01 10:30:00', 'arte', 'pytest_ad_1'),
                -- tunnel 2: 11:00:00 -> 11:00:30
                ('pytest_occ_4', NULL, '2000-01-01 11:00:00', 'arte', 'pytest_ad_1'),
                -- OTHER fragment: ignored by tunnels
                ('pytest_occ_other', NULL, '2000-01-01 12:00:00', 'arte', 'pytest_other')
        """)
    db_connection.commit()
    yield


@pytest.fixture(scope="module", autouse=True)
def run_analytics(create_test_roles, create_advertising_tables):
    logging.info("Run dbt for the thematics model once before related tests.")
    run_dbt_command(
        [
            "run",
            "--exclude",
            "core_query_causal_links",
            "--exclude",
            "task_global_completion",
            "--exclude",
            "environmental_shares_with_desinfo_counts",
            "--full-refresh",
        ]
    )
    logging.info("pytest running dbt task_global_completion")
    run_dbt_command(
        [
            "run",
            "--select",
            "task_global_completion",
            "--select",
            "environmental_shares_with_desinfo_counts",
            "--target",
            "analytics",
            "--full-refresh",
        ]
    )


def test_task_global_completion(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
                "analytics"."task_global_completion"."task_completion_aggregate_id",
                "analytics"."task_global_completion"."country",
                "analytics"."task_global_completion"."data_item_channel_name",
                "analytics"."task_global_completion"."mesinfo_choice",
                "analytics"."task_global_completion"."sum_duration_minutes"
            FROM analytics.task_global_completion
            ORDER BY analytics.task_global_completion.task_completion_aggregate_id
            LIMIT 1
        """)
        row = cur.fetchone()

    expected = (
        "0e7ee7f70a223e21b10c0dad27464bebb8cc6a7f4bd5f5b7746c661a44ec7b45",
        "france",
        "europe1",
        "Correct",
        None,
    )

    assert row == expected, f"Unexpected values: {row}"

def test_environmental_shares_desinfo(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
                "analytics"."environmental_shares_with_desinfo_counts"."start",
                "analytics"."environmental_shares_with_desinfo_counts"."channel_name",
                "analytics"."environmental_shares_with_desinfo_counts"."sum_duration_minutes",
                "analytics"."environmental_shares_with_desinfo_counts"."weekly_perc_climat",
                "analytics"."environmental_shares_with_desinfo_counts"."total_mesinfo"
            FROM analytics.environmental_shares_with_desinfo_counts
            ORDER BY analytics.environmental_shares_with_desinfo_counts.start
            LIMIT 1
        """)
        row = cur.fetchone()
    expected = (
        datetime.datetime(2025, 1, 27, 0, 0),
        "arte",
        65,
        0.13846153846153847,
        0,
    )
    assert row == expected


def test_ad_tunnels(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT tunnel_id, channel_name, start_date, end_date
            FROM public.ad_tunnels
            WHERE channel_name = 'arte' AND start_date::date = '2000-01-01'
            ORDER BY start_date
        """)
        rows = cur.fetchall()
    epoch_10h = int(datetime.datetime(2000, 1, 1, 10, tzinfo=datetime.timezone.utc).timestamp())
    epoch_11h = int(datetime.datetime(2000, 1, 1, 11, tzinfo=datetime.timezone.utc).timestamp())
    expected = [
        (
            f"arte@{epoch_10h}",
            "arte",
            datetime.datetime(2000, 1, 1, 10, 0, 0),
            datetime.datetime(2000, 1, 1, 10, 0, 52),
        ),
        (
            f"arte@{epoch_11h}",
            "arte",
            datetime.datetime(2000, 1, 1, 11, 0, 0),
            datetime.datetime(2000, 1, 1, 11, 0, 30),
        ),
    ]
    assert rows == expected


def test_ad_occurrences_classified(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT occurrence_id, channel_title, sector_label_fr, product_category_fr, label_final
            FROM public.ad_occurrences_classified
            WHERE occurrence_id LIKE 'pytest_%'
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    expected = [
        ("pytest_occ_1", "Arte", "Automobile", "Voiture électrique", "Voiture électrique"),
        ("pytest_occ_1_duplicate", "Arte", "Automobile", "Voiture électrique", "Voiture électrique"),
        ("pytest_occ_2", "Arte", "Alimentation", None, "Alimentation"),
        ("pytest_occ_4", "Arte", "Automobile", "Voiture électrique", "Voiture électrique"),
    ]
    assert rows == expected


def test_advertising_grants(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.role_table_grants
            WHERE grantee = 'rrs-read-dev'
              AND privilege_type = 'SELECT'
              AND table_name IN ('ad_tunnels', 'ad_occurrences_classified')
            ORDER BY table_name
        """)
        rows = cur.fetchall()
    assert rows == [("ad_occurrences_classified",), ("ad_tunnels",)]
