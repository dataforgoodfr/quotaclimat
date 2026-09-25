import datetime
import logging
import os
import subprocess

import psycopg2
import pytest
import yaml

from my_dbt_project.pytest_tests.test_dbt_model_homepage import run_dbt_command
from postgres.database_connection import connect_to_db
from quotaclimat.data_ingestion.external_sources.load_external_sources import load_source


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


PRODUCTION_EXTERNAL_SOURCES_CONFIG = "my_dbt_project/external_sources.yml"


TEST_FOLDER_ID = "FAKE_folder_id_123"


def classification_test_source() -> dict:
    """Production classification source settings."""
    config = yaml.safe_load(open(PRODUCTION_EXTERNAL_SOURCES_CONFIG))
    return next(s for s in config["sources"] if s["name"] == "classification_pub_ome")


def fake_fetch(sheets: dict[str, list[list]]):
    """Stands for the Google Drive / Sheets API calls: returns the rows the API would return."""
    def fetch(folder_id, spreadsheet_name):
        assert folder_id == TEST_FOLDER_ID
        assert spreadsheet_name == classification_test_source()["spreadsheet"]
        return sheets
    return fetch


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


@pytest.fixture(scope="module")
def db_engine():
    engine = connect_to_db(
        database=os.getenv("POSTGRES_DB", ""),
        user=os.getenv("POSTGRES_USER", ""),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        host=os.getenv("POSTGRES_HOST", ""),
        port=os.getenv("POSTGRES_PORT", ""),
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def load_test_external_sources(db_engine):
    """Same loading step as entrypoints/dbt.sh, with the Google API calls replaced by test rows."""
    os.environ["EXTERNAL_SOURCES_DRIVE_FOLDER"] = f"https://drive.google.com/drive/folders/{TEST_FOLDER_ID}?usp=sharing"
    sheets = {
        # the API drops trailing empty cells
        "secteurs": [
            ["sector_code", "sector_label_fr", "sector_label_en"],
            ["PYTEST_AUTO", "Automobile", "Cars"],
            [" PYTEST_FOOD ", "Alimentation", "Food"],
            [],
        ],
        "catégories": [
            ["sector_code", "cat_code", "product_category_fr"],
            ["PYTEST_AUTO", "PYTEST_AUTO_EV", "Voiture électrique"],
            ["PYTEST_AUTO", "PYTEST_AUTO_ICE", "Voiture thermique"],
            ["PYTEST_FOOD", "PYTEST_FOOD_SNACK", "Snacks"],
        ],
        "Notes de version": [["date", "note"], ["2026-07-16", "v1"]],
    }
    results = load_source(db_engine, classification_test_source(), fetch=fake_fetch(sheets))
    assert results == {
        "ref_classification_pub_ome_secteurs": True,
        "ref_classification_pub_ome_categories": True,
        "ref_classification_pub_ome_notes_de_version": True,
    }
    yield
    os.environ.pop("EXTERNAL_SOURCES_DRIVE_FOLDER", None)


@pytest.fixture(scope="module", autouse=True)
def run_analytics(create_test_roles, create_advertising_tables, load_test_external_sources):
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


def test_ad_occurrence_tunnels(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT occurrence_id, tunnel_id
            FROM public.ad_occurrence_tunnels
            WHERE occurrence_id LIKE 'pytest_%'
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    epoch_10h = int(datetime.datetime(2000, 1, 1, 10, tzinfo=datetime.timezone.utc).timestamp())
    epoch_11h = int(datetime.datetime(2000, 1, 1, 11, tzinfo=datetime.timezone.utc).timestamp())
    # deleted occurrences and OTHER fragments are not part of tunnels
    assert rows == [
        ("pytest_occ_1", f"arte@{epoch_10h}"),
        ("pytest_occ_1_duplicate", f"arte@{epoch_10h}"),
        ("pytest_occ_2", f"arte@{epoch_10h}"),
        ("pytest_occ_3_overlap", f"arte@{epoch_10h}"),
        ("pytest_occ_4", f"arte@{epoch_11h}"),
    ]


def test_ad_occurrences_classified(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT occurrence_id, channel_title, sector_label_fr, product_category_fr, label_final, tunnel_id
            FROM public.ad_occurrences_classified
            WHERE occurrence_id LIKE 'pytest_%'
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    epoch_10h = int(datetime.datetime(2000, 1, 1, 10, tzinfo=datetime.timezone.utc).timestamp())
    epoch_11h = int(datetime.datetime(2000, 1, 1, 11, tzinfo=datetime.timezone.utc).timestamp())
    expected = [
        ("pytest_occ_1", "Arte", "Automobile", "Voiture électrique", "Voiture électrique", f"arte@{epoch_10h}"),
        ("pytest_occ_1_duplicate", "Arte", "Automobile", "Voiture électrique", "Voiture électrique", f"arte@{epoch_10h}"),
        ("pytest_occ_2", "Arte", "Alimentation", None, "Alimentation", f"arte@{epoch_10h}"),
        ("pytest_occ_4", "Arte", "Automobile", "Voiture électrique", "Voiture électrique", f"arte@{epoch_11h}"),
    ]
    assert rows == expected


def test_advertising_grants(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.role_table_grants
            WHERE grantee = 'rrs-read-dev'
              AND privilege_type = 'SELECT'
              AND table_name IN ('ad_tunnels', 'ad_occurrences_classified', 'ad_occurrence_tunnels')
            ORDER BY table_name
        """)
        rows = cur.fetchall()
    assert rows == [("ad_occurrence_tunnels",), ("ad_occurrences_classified",), ("ad_tunnels",)]


def test_external_source_loaded(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT sector_code, sector_label_fr, sector_label_en
            FROM public.ref_classification_pub_ome_secteurs
            ORDER BY sector_code
        """)
        sectors = cur.fetchall()
        cur.execute("SELECT * FROM public.ref_classification_pub_ome_notes_de_version")
        notes = cur.fetchall()
        cur.execute("""
            SELECT DISTINCT ON (table_name) table_name, row_count FROM public.ref_external_source_load
            WHERE name = 'classification_pub_ome'
              AND table_name IN (
                'ref_classification_pub_ome_categories',
                'ref_classification_pub_ome_notes_de_version',
                'ref_classification_pub_ome_secteurs'
              )
            ORDER BY table_name, loaded_at DESC
        """)
        history = cur.fetchall()
    # values are stripped, empty rows are dropped, extra columns and tabs are kept
    assert sectors == [
        ("PYTEST_AUTO", "Automobile", "Cars"),
        ("PYTEST_FOOD", "Alimentation", "Food"),
    ]
    assert notes == [("2026-07-16", "v1")]
    assert history == [
        ("ref_classification_pub_ome_categories", 3),
        ("ref_classification_pub_ome_notes_de_version", 1),
        ("ref_classification_pub_ome_secteurs", 2),
    ]


@pytest.mark.parametrize(
    "categories",
    [
        # missing required column
        [["sector_code", "cat_code"], ["A", "A1"]],
        # duplicated unique key
        [["sector_code", "cat_code", "product_category_fr"], ["A", "A1", "P"], ["B", "A1", "P"]],
        # no rows
        [["sector_code", "cat_code", "product_category_fr"]],
        # duplicated column names
        [["sector_code", "cat_code", "cat_code", "product_category_fr"], ["A", "A1", "A1", "P"]],
    ],
)
def test_external_source_invalid_tab_keeps_table(db_connection, db_engine, monkeypatch, categories):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", TEST_FOLDER_ID)
    source = classification_test_source()
    source["sheets"] = {"catégories": source["sheets"]["catégories"]}
    assert load_source(db_engine, source, fetch=fake_fetch({"catégories": categories})) == {
        "ref_classification_pub_ome_categories": False
    }
    with db_connection.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.ref_classification_pub_ome_categories")
        assert cur.fetchone()[0] == 3


def test_external_source_only_writes_ref_tables(db_connection, db_engine, monkeypatch):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", TEST_FOLDER_ID)
    source = classification_test_source()
    source["sheets"] = {"catégories": {"table": "keywords"}}
    sheets = {"catégories": [["a"], ["b"]], "Tab'; DROP TABLE keywords; --": [["a"], ["b"]]}
    assert load_source(db_engine, source, fetch=fake_fetch(sheets)) == {
        "ref_classification_pub_ome_tab_drop_table_keywords": True
    }
    with db_connection.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.keywords")
        assert cur.fetchone()[0] > 0


def test_external_source_unreachable_keeps_tables(db_connection, db_engine, monkeypatch):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", TEST_FOLDER_ID)

    def failing_fetch(folder_id, spreadsheet_name):
        raise RuntimeError("403 Forbidden")

    assert load_source(db_engine, classification_test_source(), fetch=failing_fetch) == {}
    with db_connection.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.ref_classification_pub_ome_categories")
        assert cur.fetchone()[0] == 3


def test_external_source_without_folder_is_skipped(db_connection, db_engine, monkeypatch):
    monkeypatch.delenv("EXTERNAL_SOURCES_DRIVE_FOLDER", raising=False)
    assert load_source(db_engine, classification_test_source(), fetch=fake_fetch({})) == {}
    with db_connection.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.ref_classification_pub_ome_categories")
        assert cur.fetchone()[0] == 3


def test_external_source_helpers():
    from quotaclimat.data_ingestion.external_sources.load_external_sources import (
        ExternalSourceError,
        drive_query_literal,
        parse_folder_id,
        to_snake_case,
    )

    assert parse_folder_id("FAKE_folder_id_123") == "FAKE_folder_id_123"
    assert parse_folder_id("https://drive.google.com/drive/folders/FAKE_folder_id_123?usp=sharing") == "FAKE_folder_id_123"
    assert parse_folder_id("https://drive.google.com/drive/u/0/folders/FAKE_folder_id_123") == "FAKE_folder_id_123"
    for value in [
        "https://evil.example/drive.google.com/drive/folders/FAKE_folder_id_123",
        "https://drive.google.com.evil.example/drive/folders/FAKE_folder_id_123",
        "' or name contains 'a",
    ]:
        with pytest.raises(ExternalSourceError):
            parse_folder_id(value)
    assert drive_query_literal("it's a \\ test") == "'it\\'s a \\\\ test'"
    assert to_snake_case("Catégories Transversales (v2)") == "categories_transversales_v2"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


def test_fetch_google_sheet_api_calls(monkeypatch):
    """Checks the Drive / Sheets API requests with a fake authorized session."""
    from quotaclimat.data_ingestion.external_sources import load_external_sources as loader

    calls = []

    class FakeSession:
        def get(self, url, params, timeout):
            calls.append((url, params))
            if url == loader.DRIVE_FILES_API_URL:
                return FakeResponse({"files": [{"id": "SHEET_ID_1234567890", "name": "classification_pub_ome"}]})
            if url.endswith("/values:batchGet"):
                return FakeResponse({"valueRanges": [{"values": [["a"], ["1"]]}, {}]})
            return FakeResponse({"sheets": [
                {"properties": {"title": "secteurs", "sheetType": "GRID"}},
                {"properties": {"title": "Tab 'quoted'", "sheetType": "GRID"}},
                {"properties": {"title": "Chart", "sheetType": "OBJECT"}},
            ]})

    monkeypatch.setattr(loader, "get_session", lambda: FakeSession())
    sheets = loader.fetch_google_sheet("FOLDER_ID_123", "classification_pub_ome")

    assert sheets == {"secteurs": [["a"], ["1"]], "Tab 'quoted'": []}
    assert calls[0][1]["q"] == (
        "'FOLDER_ID_123' in parents and name = 'classification_pub_ome'"
        " and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    )
    assert calls[1][0] == f"{loader.SHEETS_API_URL}/SHEET_ID_1234567890"
    assert calls[2][1]["ranges"] == ["'secteurs'", "'Tab ''quoted'''"]
    assert calls[2][1]["valueRenderOption"] == "FORMATTED_VALUE"


def test_fetch_google_sheet_requires_exactly_one_file(monkeypatch):
    from quotaclimat.data_ingestion.external_sources import load_external_sources as loader

    class FakeSession:
        def get(self, url, params, timeout):
            return FakeResponse({"files": [{"id": "a"}, {"id": "b"}]})

    monkeypatch.setattr(loader, "get_session", lambda: FakeSession())
    with pytest.raises(loader.ExternalSourceError, match="2 spreadsheets"):
        loader.fetch_google_sheet("FOLDER_ID_123", "classification_pub_ome")


def test_get_session_requires_credentials(monkeypatch):
    from quotaclimat.data_ingestion.external_sources import load_external_sources as loader

    monkeypatch.delenv(loader.CREDENTIALS_ENV, raising=False)
    with pytest.raises(loader.ExternalSourceError, match="no Google service account credentials"):
        loader.get_session()
