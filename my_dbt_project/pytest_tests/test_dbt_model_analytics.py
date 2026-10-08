import datetime
import logging
import os
import re
import subprocess

import psycopg2
import pytest
import yaml

from my_dbt_project.pytest_tests.test_dbt_model_homepage import run_dbt_command
from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.normalize import normalize, nospace
from quotaclimat.data_ingestion.external_sources.download_external_sources import SEEDS_DIR, download_source


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


def production_source(name: str) -> dict:
    """Production settings of an external source."""
    config = yaml.safe_load(open(PRODUCTION_EXTERNAL_SOURCES_CONFIG))
    return next(s for s in config["sources"] if s["name"] == name)


def classification_test_source() -> dict:
    """Production classification source settings."""
    return production_source("ome_dictionnaire_marques_secteurs")


def brand_inventory_test_source() -> dict:
    """Production brand inventory source settings."""
    return production_source("inventaire_des_marques")


def fake_fetch(sheets: dict[str, list[list]], source: dict | None = None):
    """Stands for the Google Drive / Sheets API calls: returns the rows the API would return."""
    def fetch(folder_id, spreadsheet_name):
        assert folder_id == TEST_FOLDER_ID
        assert spreadsheet_name == (source or classification_test_source())["spreadsheet"]
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
        cur.execute("DELETE FROM advertising.ad_occurrence WHERE id LIKE 'pytest_%' OR id LIKE 'mesinfo_pytest_%' OR id LIKE 'program_pytest_%'")
        cur.execute("DELETE FROM advertising.ad WHERE id LIKE 'pytest_%'")
        cur.execute("""
            INSERT INTO advertising.ad (
                id, first_detection_date, duration_sec, chunks, fragment_type,
                prediction_status, predicted_sector, predicted_product_category, predicted_brand
            ) VALUES
                ('pytest_ad_1', '2000-01-01', 30, '[]', 'AD', 'subcat_done', 'PYTEST_AUTO', 'PYTEST_AUTO_EV', 'Pytest Škoda'),
                ('pytest_ad_2', '2000-01-01', 20, '[]', 'AD', 'dict_tier1', 'PYTEST_FOOD', NULL, 'Pytest Biscuits'),
                ('pytest_ad_3', '2000-01-01', 10, '[]', 'AD', 'pending', NULL, NULL, 'Pytest Škoda'),
                ('pytest_other', '2000-01-01', 15, '[]', 'OTHER', 'pending', NULL, NULL, NULL)
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
        # next to the validated misinformation of the labelstudio seeds (not prefixed pytest_,
        # to leave the other advertising tests as they are)
        cur.execute("""
            INSERT INTO advertising.ad_occurrence (id, deleted_at, occurrence_date, channel_name, ad_id) VALUES
                -- 30 min after a 'Correct' misinformation (2025-04-05 13:08)
                ('mesinfo_pytest_near', NULL, '2025-04-05 13:38:00', 'franceinfotv', 'pytest_ad_1'),
                -- between two 'Correct' ones (2025-04-02 09:10 and 2025-04-06 07:10): the nearest
                ('mesinfo_pytest_between', NULL, '2025-04-04 09:10:00', 'sud-radio', 'pytest_ad_1'),
                -- annotated twice (2025-04-10 04:54): the first annotation only
                ('mesinfo_pytest_two_versions', NULL, '2025-04-10 05:00:00', 'lci', 'pytest_ad_1'),
                -- only an 'Incorrect' one (2025-06-12 20:34)
                ('mesinfo_pytest_incorrect', NULL, '2025-06-12 20:40:00', 'itele', 'pytest_ad_1'),
                -- 15 days after the nearest 'Correct' one: out of the window
                ('mesinfo_pytest_out_of_window', NULL, '2025-04-20 13:08:00', 'franceinfotv', 'pytest_ad_1'),
                -- deleted: ignored
                ('mesinfo_pytest_deleted', '2025-04-06', '2025-04-05 13:10:00', 'franceinfotv', 'pytest_ad_1')
        """)
        # around the france2 programs of Monday 2025-04-07 (Paris time, UTC+2), 30 s tunnels
        cur.execute("""
            INSERT INTO advertising.ad_occurrence (id, deleted_at, occurrence_date, channel_name, ad_id) VALUES
                -- 13:10 Paris: inside the JT 13h (13:00 - 13:40)
                ('program_pytest_inside', NULL, '2025-04-07 11:10:00', 'france2', 'pytest_ad_1'),
                -- 13:45 Paris: 5 min after the JT 13h, next program (19:55) out of the window
                ('program_pytest_after_news', NULL, '2025-04-07 11:45:00', 'france2', 'pytest_ad_1'),
                -- 19:50 Paris: JT 13h out of the window, 4 min 30 s before the JT 20h (19:55)
                ('program_pytest_before_news', NULL, '2025-04-07 17:50:00', 'france2', 'pytest_ad_1'),
                -- 13:39:50 Paris: starts in the JT 13h, ends after it
                ('program_pytest_overlap_end', NULL, '2025-04-07 11:39:50', 'france2', 'pytest_ad_1')
        """)
    db_connection.commit()
    yield


@pytest.fixture(scope="module")
def load_test_external_sources():
    """Same steps as entrypoints/dbt.sh (download, dbt seed, dbt test), with the Google API calls
    replaced by test rows."""
    os.environ["EXTERNAL_SOURCES_DRIVE_FOLDER"] = TEST_FOLDER_ID
    sheets = {
        # the API drops trailing empty cells
        "secteurs": [
            ["sector_code", "sector_label_fr", "sector_label_en", "classes_nice"],
            ["PYTEST_AUTO", "Automobile", "Cars", "12; 37; 39"],
            [" PYTEST_FOOD ", "Alimentation", "Food", "29"],
            ["001", "Code numérique", "Numeric code"],
            [],
        ],
        "catégories": [
            ["sector_code", "cat_code", "product_category_fr"],
            ["PYTEST_AUTO", "PYTEST_AUTO_EV", "Voiture électrique"],
            ["PYTEST_AUTO", "PYTEST_AUTO_ICE", "Voiture thermique"],
            ["PYTEST_FOOD", "PYTEST_FOOD_SNACK", "Snacks"],
        ],
        "Notes de version": [["version", "date", "note"], ["1", "2026-07-16", "v1"], ["2", "2026-09-25", "v2"]],
    }
    results = download_source(classification_test_source(), SEEDS_DIR, fetch=fake_fetch(sheets))
    assert results == {
        "ref_ome_secteurs": True,
        "ref_ome_categories": True,
        "ref_ome_notes_de_version": True,
    }
    brand_sheets = {
        "Marques": [
            ["marque", "entreprise", "source", "statut", "commentaire"],
            # same key as the predicted brand "Pytest Škoda": the verified row wins, its company is
            # written differently from the tab Entreprises (same name_key)
            ["PYTEST SKODA", "PYTEST VOLKSWAGEN", "wikidata", "vérifié"],
            ["pytest-škoda", "Pytest Wrong Group", "llm", "non vérifié"],
            # ultimate parent of the company not verified in the tab Entreprises
            ["Pytest Biscuits", "Pytest Biscuits Group", "llm", "non vérifié"],
            # listed but company not filled in yet
            ["Pytest Brand Without Group"],
            # numeric looking brand stays text
            ["1664", "Pytest Carlsberg", "manuel", "non vérifié", "Kronenbourg"],
        ],
        "Entreprises": [
            ["entreprise", "alias", "entreprise_id", "siren", "societe_mere_ultime", "source", "statut", "commentaire"],
            ["Pytest Volkswagen", "", "Q246", "", "Pytest Porsche SE", "gleif", "vérifié"],
            # not verified: the company is known, its ultimate parent is ignored; its alias is the label
            ["Pytest Biscuits Group", "Pytest Biscuits", "", "", "Pytest Holding", "llm", "non vérifié"],
        ],
        "_lisez-moi": [["note"], ["for humans only"]],
    }
    results = download_source(
        brand_inventory_test_source(), SEEDS_DIR, fetch=fake_fetch(brand_sheets, brand_inventory_test_source())
    )
    assert results == {"ref_inventaire_marques": True, "ref_inventaire_entreprises": True}
    run_dbt_command(["seed", "--full-refresh", "--select", "path:seeds/ref"])
    run_dbt_command(["test", "--select", "path:seeds/ref"])
    yield
    os.environ.pop("EXTERNAL_SOURCES_DRIVE_FOLDER", None)
    for csv_file in SEEDS_DIR.glob("ref_*.csv"):
        csv_file.unlink()


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
            "--exclude",
            "path:models/advertising",
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
    logging.info("pytest running dbt advertising models, after the analytics tables they read")
    run_dbt_command(
        [
            "run",
            "--select",
            "path:models/advertising",
            # the advertising test rows are dated 2000-01-01, before the real analysis start date
            "--vars",
            '{"ad_analysis_start_date": "2000-01-01"}',
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
            FROM advertising.ad_tunnels
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
            FROM advertising.ad_occurrence_tunnels
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
            FROM advertising.ad_occurrences_classified
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


def test_ad_occurrences_classified_mesinfo(db_connection):
    """Distance to the nearest validated misinformation on the same channel, within 7 days."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT occurrence_id, mesinfo_distance_sec, nearest_mesinfo_task_aggregate_id
            FROM advertising.ad_occurrences_classified
            WHERE occurrence_id LIKE 'mesinfo_pytest_%'
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    assert rows == [
        ("mesinfo_pytest_between", 165600, "1b8930946a8c392c3c101869f111f3d1aed359c4aabf952de62b30d98aef1ded"),
        ("mesinfo_pytest_incorrect", None, None),
        ("mesinfo_pytest_near", 1800, "70ff0eebb0d606feacde156e1584139ef1bea0d3a5f6b100dec9cb437617a23a"),
        ("mesinfo_pytest_out_of_window", None, None),
        ("mesinfo_pytest_two_versions", 360, "1b7df93021fa55a3c9291fb572edbb7d73822a12db7e6262c360a8b971bbbbb9"),
    ]


def test_ad_occurrences_classified_programs(db_connection):
    """Monitored programs around the ad tunnel of each occurrence."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT
                occurrence_id, inside_program,
                program_before, program_before_type, program_before_gap_sec,
                program_after, program_after_type, program_after_gap_sec
            FROM advertising.ad_occurrences_classified
            WHERE occurrence_id LIKE 'program_pytest_%'
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    assert rows == [
        ("program_pytest_after_news", False, "JT 13h", "Information - Journal", 300, None, None, None),
        ("program_pytest_before_news", False, None, None, None, "JT 20h + météo", "Information - Journal", 270),
        ("program_pytest_inside", True, "JT 13h", "Information - Journal", 0, None, None, None),
        ("program_pytest_overlap_end", False, "JT 13h", "Information - Journal", 0, None, None, None),
    ]


def test_ad_brands(db_connection):
    """One row per brand of the tab Marques, independently of the ad tables."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT brand_key, brand, inventory_company, brand_company_status,
                company_in_inventory, company_id, ultimate_parent_verified, brand_company, brand_ultimate_parent,
                brand_company_label
            FROM advertising.ad_brands
            WHERE brand_key LIKE 'pytest%' OR brand_key = '1664'
            ORDER BY brand_key
        """)
        rows = cur.fetchall()
    assert rows == [
        # company not in the tab Entreprises: kept as written
        (
            "1664", "1664", "Pytest Carlsberg", "non vérifié",
            False, None, False, "Pytest Carlsberg", "Pytest Carlsberg", "Pytest Carlsberg",
        ),
        # ultimate parent not verified: ignored, the company instead; its alias as label
        (
            "pytestbiscuits", "Pytest Biscuits", "Pytest Biscuits Group", "non vérifié",
            True, None, False, "Pytest Biscuits Group", "Pytest Biscuits Group", "Pytest Biscuits",
        ),
        # no company yet: the brand itself
        (
            "pytestbrandwithoutgroup", "Pytest Brand Without Group", None, None,
            False, None, False, "Pytest Brand Without Group", "Pytest Brand Without Group", "Pytest Brand Without Group",
        ),
        # two spellings of the brand, the verified one wins; official name of the tab Entreprises and
        # verified ultimate parent
        (
            "pytestskoda", "PYTEST SKODA", "PYTEST VOLKSWAGEN", "vérifié",
            True, "Q246", True, "Pytest Volkswagen", "Pytest Porsche SE", "Pytest Volkswagen",
        ),
    ]


def test_ad_occurrences_classified_brand_companies(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT occurrence_id, predicted_brand, brand_company, brand_ultimate_parent
            FROM advertising.ad_occurrences_classified
            WHERE occurrence_id IN ('pytest_occ_1', 'pytest_occ_2')
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    assert rows == [
        ("pytest_occ_1", "Pytest Škoda", "Pytest Volkswagen", "Pytest Porsche SE"),
        ("pytest_occ_2", "Pytest Biscuits", "Pytest Biscuits Group", "Pytest Biscuits Group"),
    ]


def test_brand_inventory_loaded_as_text(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("SELECT marque, entreprise FROM advertising.ref_inventaire_marques WHERE marque = '1664'")
        assert cur.fetchall() == [("1664", "Pytest Carlsberg")]


@pytest.mark.parametrize(
    "name",
    [
        "L’Oréal Paris", "L'OREAL  PARIS", "Cœur de Lion", "CŒUR DE LION", "Škoda", "Coca-Cola®",
        "Leclerc — E.Leclerc", "Kronenbourg 1664", "Ça c'est Paris !", "Bière_Æther", "Größe",
        "Øresund", "Mc Donald's™", "Intermarché\u00a0", "« Franprix »", "Łódź", "3M", "élan 50€", "",
    ],
)
def test_name_key_matches_python(db_connection, name):
    """The name_key dbt macro gives the same key as the Python classification pipeline."""
    macro = open("my_dbt_project/macros/name_key.sql").read()
    sql = re.search(r"\{% macro name_key\(column\) -%\}(.*)\{%- endmacro", macro, re.S).group(1)
    with db_connection.cursor() as cur:
        cur.execute("SELECT " + sql.replace("{{ column }}", "%s"), (name,))
        assert cur.fetchone()[0] == nospace(normalize(name))
    db_connection.rollback()


def test_advertising_grants(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.role_table_grants
            WHERE grantee = 'rrs-read-dev'
              AND privilege_type = 'SELECT'
              AND table_schema = 'advertising'
              AND table_name IN ('ad_tunnels', 'ad_occurrences_classified', 'ad_occurrence_tunnels', 'ad_brands')
            ORDER BY table_name
        """)
        rows = cur.fetchall()
    assert rows == [("ad_brands",), ("ad_occurrence_tunnels",), ("ad_occurrences_classified",), ("ad_tunnels",)]


def test_external_source_nice_classes_are_text(db_connection):
    """classes_nice (brand groups job) stays text, even for a single class."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT sector_code, classes_nice FROM advertising.ref_ome_secteurs ORDER BY sector_code")
        assert cur.fetchall() == [("001", None), ("PYTEST_AUTO", "12; 37; 39"), ("PYTEST_FOOD", "29")]


def test_external_source_loaded(db_connection):
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT sector_code, sector_label_fr, sector_label_en
            FROM advertising.ref_ome_secteurs
            ORDER BY sector_code
        """)
        sectors = cur.fetchall()
        cur.execute("SELECT version, note FROM advertising.ref_ome_notes_de_version ORDER BY version")
        notes = cur.fetchall()
    # values are stripped, empty rows are dropped, extra columns and tabs are kept,
    # identifiers stay text (column_types in _ref_seeds.yml), other types are inferred by dbt
    assert sectors == [
        ("001", "Code numérique", "Numeric code"),
        ("PYTEST_AUTO", "Automobile", "Cars"),
        ("PYTEST_FOOD", "Alimentation", "Food"),
    ]
    assert notes == [(1, "v1"), (2, "v2")]


@pytest.mark.parametrize(
    "categories",
    [
        # no header
        [],
        # duplicated column names
        [["sector_code", "cat_code", "cat_code"], ["A", "A1", "A1"]],
        # values without header
        [["sector_code", "", "cat_code"], ["A", "oops", "A1"]],
        # no rows
        [["sector_code", "cat_code"], []],
    ],
)
def test_external_source_invalid_tab_is_not_written(tmp_path, monkeypatch, categories):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", TEST_FOLDER_ID)
    sheets = {"catégories": categories, "secteurs": [["sector_code"], ["A"]]}
    assert download_source(classification_test_source(), tmp_path, fetch=fake_fetch(sheets)) == {
        "ref_ome_categories": False,
        "ref_ome_secteurs": True,
    }
    assert sorted(p.name for p in tmp_path.iterdir()) == ["ref_ome_secteurs.csv"]


def test_external_source_only_writes_ref_tables(tmp_path, monkeypatch):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", TEST_FOLDER_ID)
    source = classification_test_source()
    source["sheets"] = {"catégories": {"table": "keywords"}, "secteurs": {"skip": True}}
    sheets = {
        "catégories": [["a"], ["b"]],
        "secteurs": [["a"], ["b"]],
        "Tab'; DROP TABLE keywords; --": [["a"], ["b"]],
        # same table name as the previous tab
        "Tab DROP TABLE keywords": [["a"], ["b"]],
        # for humans only
        "_lisez-moi": [["a"], ["b"]],
    }
    assert download_source(source, tmp_path, fetch=fake_fetch(sheets)) == {"ref_ome_tab_drop_table_keywords": True}
    assert sorted(p.name for p in tmp_path.iterdir()) == ["ref_ome_tab_drop_table_keywords.csv"]


def test_external_source_unreachable_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", TEST_FOLDER_ID)

    def failing_fetch(folder_id, spreadsheet_name):
        raise RuntimeError("403 Forbidden")

    assert download_source(classification_test_source(), tmp_path, fetch=failing_fetch) == {}
    assert list(tmp_path.iterdir()) == []


def test_external_source_without_folder_is_skipped(tmp_path, monkeypatch):
    monkeypatch.delenv("EXTERNAL_SOURCES_DRIVE_FOLDER", raising=False)
    assert download_source(classification_test_source(), tmp_path, fetch=fake_fetch({})) == {}
    assert list(tmp_path.iterdir()) == []


def test_download_external_sources_removes_previous_seeds(tmp_path, monkeypatch):
    from quotaclimat.data_ingestion.external_sources import download_external_sources as downloader

    monkeypatch.delenv("EXTERNAL_SOURCES_DRIVE_FOLDER", raising=False)
    (tmp_path / "ref_ome_old_tab.csv").write_text("a\nb\n")
    (tmp_path / "_ref_seeds.yml").write_text("version: 2\n")
    downloader.download_external_sources(PRODUCTION_EXTERNAL_SOURCES_CONFIG, tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["_ref_seeds.yml"]


def test_external_source_helpers():
    from quotaclimat.data_ingestion.external_sources.download_external_sources import (
        ExternalSourceError,
        drive_query_literal,
        parse_folder_id,
        to_snake_case,
    )

    assert parse_folder_id(" FAKE_folder_id_123 ") == "FAKE_folder_id_123"
    for value in [
        "https://drive.google.com/drive/folders/FAKE_folder_id_123",
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
    from quotaclimat.data_ingestion.external_sources import download_external_sources as loader

    calls = []

    class FakeSession:
        def get(self, url, params, timeout):
            calls.append((url, params))
            if url == loader.DRIVE_FILES_API_URL:
                return FakeResponse({"files": [{"id": "SHEET_ID_1234567890", "name": "Dictionnaire_marques_secteurs"}]})
            if url.endswith("/values:batchGet"):
                return FakeResponse({"valueRanges": [{"values": [["a"], ["1"]]}, {}]})
            return FakeResponse({"sheets": [
                {"properties": {"title": "secteurs", "sheetType": "GRID"}},
                {"properties": {"title": "Tab 'quoted'", "sheetType": "GRID"}},
                {"properties": {"title": "Chart", "sheetType": "OBJECT"}},
                # for humans only: its values are not even downloaded
                {"properties": {"title": "_lisez-moi", "sheetType": "GRID"}},
            ]})

    monkeypatch.setattr(loader, "get_session", lambda: FakeSession())
    checked = []
    monkeypatch.setattr(loader, "ensure_not_public", lambda spreadsheet: checked.append(spreadsheet["id"]))
    sheets = loader.fetch_google_sheet("FOLDER_ID_123", "Dictionnaire_marques_secteurs")

    assert sheets == {"secteurs": [["a"], ["1"]], "Tab 'quoted'": []}
    assert checked == ["SHEET_ID_1234567890"]
    assert calls[0][1]["q"] == (
        "'FOLDER_ID_123' in parents and name = 'Dictionnaire_marques_secteurs'"
        " and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    )
    assert calls[1][0] == f"{loader.SHEETS_API_URL}/SHEET_ID_1234567890"
    assert calls[2][1]["ranges"] == ["'secteurs'", "'Tab ''quoted'''"]
    assert calls[2][1]["valueRenderOption"] == "FORMATTED_VALUE"


def test_fetch_google_sheet_requires_exactly_one_file(monkeypatch):
    from quotaclimat.data_ingestion.external_sources import download_external_sources as loader

    class FakeSession:
        def get(self, url, params, timeout):
            return FakeResponse({"files": [{"id": "a"}, {"id": "b"}]})

    monkeypatch.setattr(loader, "get_session", lambda: FakeSession())
    with pytest.raises(loader.ExternalSourceError, match="2 spreadsheets"):
        loader.fetch_google_sheet("FOLDER_ID_123", "Dictionnaire_marques_secteurs")


def test_get_session_requires_credentials(monkeypatch):
    from quotaclimat.data_ingestion.external_sources import download_external_sources as loader

    monkeypatch.delenv(loader.CREDENTIALS_ENV, raising=False)
    with pytest.raises(loader.ExternalSourceError, match="no Google service account credentials"):
        loader.get_session()


class FakeAnonymousResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.closed = False

    def close(self):
        self.closed = True


@pytest.mark.parametrize(
    "status, headers",
    [
        (302, {"Location": "https://accounts.google.com/ServiceLogin?continue=..."}),
        (401, {}),
        (403, {}),
        (404, {}),
    ],
)
def test_ensure_not_public_accepts_private_spreadsheet(status, headers):
    from quotaclimat.data_ingestion.external_sources.download_external_sources import ensure_not_public

    calls = []

    def anonymous_get(url, **kwargs):
        calls.append((url, kwargs))
        return FakeAnonymousResponse(status, headers)

    ensure_not_public({"id": "SHEET_ID_1234567890", "permissionIds": ["12345", "67890"]}, anonymous_get)
    url, kwargs = calls[0]
    assert url == "https://docs.google.com/spreadsheets/d/SHEET_ID_1234567890/export?format=csv"
    assert kwargs["allow_redirects"] is False and kwargs["stream"] is True
    assert "headers" not in kwargs and "auth" not in kwargs


@pytest.mark.parametrize(
    "permission_ids, status, headers, message",
    [
        # shared "anyone with the link" / public on the web, seen in the Drive permissions
        (["12345", "anyoneWithLink"], 302, {"Location": "https://accounts.google.com/"}, "shared publicly"),
        (["anyone"], 302, {"Location": "https://accounts.google.com/"}, "shared publicly"),
        # readable without credentials, even if the permission is not listed
        (["12345"], 307, {"Location": "https://doc-0s-sheets.googleusercontent.com/export/..."}, "readable without credentials"),
        (["12345"], 200, {"Content-Type": "text/csv"}, "readable without credentials"),
        # cannot conclude: refused too
        (["12345"], 200, {"Content-Type": "text/html"}, "could not check"),
        (["12345"], 500, {}, "could not check"),
    ],
)
def test_ensure_not_public_refuses_public_spreadsheet(permission_ids, status, headers, message):
    from quotaclimat.data_ingestion.external_sources.download_external_sources import (
        ExternalSourceError,
        ensure_not_public,
    )

    responses = []

    def anonymous_get(url, **kwargs):
        responses.append(FakeAnonymousResponse(status, headers))
        return responses[-1]

    with pytest.raises(ExternalSourceError, match=message):
        ensure_not_public({"id": "SHEET_ID_1234567890", "permissionIds": permission_ids}, anonymous_get)
    assert all(r.closed for r in responses)


def test_advertising_models_schema(db_connection):
    """The advertising models and the reference seeds are built in the advertising schema (+schema,
    generate_schema_name), the other models and seeds stay in the target schema."""
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name IN (
                'ad_tunnels', 'ad_occurrences_classified', 'ad_occurrence_tunnels', 'ad_occurrence_mesinfo',
                'ad_tunnel_programs', 'ref_ome_secteurs',
                'core_query_environmental_shares', 'task_global_completion', 'keywords'
            )
            ORDER BY table_name
        """)
        rows = cur.fetchall()
    assert rows == [
        ("advertising", "ad_occurrence_mesinfo"),
        ("advertising", "ad_occurrence_tunnels"),
        ("advertising", "ad_occurrences_classified"),
        ("advertising", "ad_tunnel_programs"),
        ("advertising", "ad_tunnels"),
        ("public", "core_query_environmental_shares"),
        ("public", "keywords"),
        ("advertising", "ref_ome_secteurs"),
        ("analytics", "task_global_completion"),
    ]


def test_ad_occurrences_classified_start_date(db_connection):
    """Occurrences before ad_analysis_start_date are excluded (run last: rebuilds the model)."""
    # end the transaction left open by the previous SELECTs: its lock on the table would block dbt
    db_connection.rollback()
    run_dbt_command([
        "run", "--select", "ad_occurrences_classified",
        "--vars", '{"ad_analysis_start_date": "2000-01-01 10:30:00"}',
    ])
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT occurrence_id
            FROM advertising.ad_occurrences_classified
            WHERE occurrence_id LIKE 'pytest_%'
            ORDER BY occurrence_id
        """)
        rows = cur.fetchall()
    # pytest_occ_1, pytest_occ_1_duplicate and pytest_occ_2 are at 10:00, before the start date
    assert rows == [("pytest_occ_4",)]
