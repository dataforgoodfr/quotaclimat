"""Manual corrections of the ads (s04_consolidation/e02_manual_corrections), on a PostgreSQL database, with an
in-memory spreadsheet."""

import asyncio
import json
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad, Ad_Occurrence, AdvertisingBase
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections import (
    run as corrections_run,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.corrections import (
    new_ad_id,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.references import (
    References,
    load_references,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.sheet import (
    CorrectionsSheet,
    column_letter,
    parse_rows,
)

T0 = datetime(2026, 4, 13, 20, 0, 0)
SPREADSHEET = "Corrections_publicites"
REFERENCES = References(
    sectors={"AUT", "FOD"},
    category_sector={"AUT1": "AUT", "FOD2": "FOD"},
    dictionary_sectors={"renault": {"AUT"}},
    known_brands={"renault": "Renault", "mazda": "Mazda"},
)


class MemorySheet(CorrectionsSheet):
    """The spreadsheet as {tab: rows of values}, the Google API calls replaced."""

    def __init__(self, tabs: dict[str, list[list[str]]]):
        super().__init__(session=None, spreadsheet_id="memory")
        self.grid = tabs

    def tabs(self) -> list[str]:
        return [tab for tab in self.grid if not tab.startswith("_")]

    def read(self, tab):
        return parse_rows(self.grid[tab])

    def _write_cells(self, tab, cells):
        grid = self.grid[tab]
        for row, column, value in cells:
            while len(grid) < row:
                grid.append([])
            line = grid[row - 1]
            while len(line) <= column:
                line.append("")
            line[column] = value

    def column(self, tab, name) -> list[str]:
        header = self.grid[tab][0]
        index = header.index(name)
        return [line[index] if index < len(line) else "" for line in self.grid[tab][1:]]


@pytest.fixture
def engine():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    yield engine
    engine.dispose()


def make_ad(ad_id, status="subcat_done", fragment_type="AD", brand="Pytest", **kwargs):
    return Ad(
        id=ad_id,
        first_detection_date=T0,
        duration_sec=kwargs.pop("duration_sec", 20.0),
        chunks=[{"hash": "h", "fingerprints": []}],
        fragment_type=fragment_type,
        prediction=kwargs.pop(
            "prediction",
            [{"stage": "ad_type", "raw_response": {"content_type": fragment_type}}],
        ),
        prediction_status=status,
        predicted_brand=brand,
        **kwargs,
    )


def make_occ(occ_id, ad_id, seconds, channel="tf1"):
    return Ad_Occurrence(
        id=occ_id,
        occurrence_date=T0 + timedelta(seconds=seconds),
        channel_name=channel,
        ad_id=ad_id,
    )


def insert(engine, *rows):
    with Session(engine) as session:
        session.add_all(rows)
        session.commit()


def get_ad(engine, ad_id) -> Ad:
    with Session(engine) as session:
        ad = session.get(Ad, ad_id)
        session.expunge_all()
        return ad


def run(engine, sheet, export_video=None, dry_run=False):
    with Session(engine) as session:
        return corrections_run.run(
            sheet,
            session,
            SPREADSHEET,
            export_video or (lambda ad_id, occurrences, duration, dry: occurrences[0]),
            dry_run,
            references=REFERENCES,
        )


AD_HEADER = ["ad_id", "marque", "secteur", "categorie", "type_contenu", "commentaire"]


# ---------- sheet ----------


def test_column_letter():
    assert [column_letter(i) for i in (0, 25, 26, 27, 51, 52)] == [
        "A",
        "Z",
        "AA",
        "AB",
        "AZ",
        "BA",
    ]


def test_parse_rows_skips_empty_rows_and_numbers_the_others():
    header, rows = parse_rows([["ad_id", "marque"], ["a1", "X"], [], ["a2"]])
    assert header == ["ad_id", "marque"]
    assert [(r.number, r.values) for r in rows] == [
        (2, {"ad_id": "a1", "marque": "X"}),
        (4, {"ad_id": "a2", "marque": ""}),
    ]


def test_tab_kind():
    assert corrections_run.tab_kind("correction_ads_mai") == "correction_ads"
    assert corrections_run.tab_kind("nouvelle_ad_renault") == "nouvelle_ad"
    assert corrections_run.tab_kind("dechets") == "dechets"
    assert corrections_run.tab_kind("reattribution_mai") == "reattribution"
    assert corrections_run.tab_kind("autre") is None


def test_write_rows_finds_a_row_moved_since_the_read():
    sheet = MemorySheet({"dechets": [["occurrence_id"], ["o1"], ["o2"]]})
    _, rows = sheet.read("dechets")
    # a human sorts the tab between the read and the write
    sheet.grid["dechets"] = [["occurrence_id"], ["o2"], ["o1"]]
    sheet.write_rows("dechets", "occurrence_id", [(rows[0], {"traite_le": "x"})])
    assert sheet.grid["dechets"] == [
        ["occurrence_id", "traite_le"],
        ["o2"],
        ["o1", "x"],
    ]


# ---------- ad corrections ----------


def test_load_references_reads_the_dictionary_and_the_predicted_brands(engine):
    insert(engine, make_ad("a1", brand="Peugeot"))
    with Session(engine) as session:
        references = load_references(session)
    # no ref_* table in the test database: no check of the codes
    assert references.sectors == set()
    assert references.known_brands == {"peugeot": "Peugeot"}


def test_correction_of_a_classified_ad_replaces_the_prediction(engine):
    insert(
        engine,
        make_ad(
            "a1", brand="Renaut", predicted_sector="FOD", prediction_method="llm_subcat"
        ),
    )
    sheet = MemorySheet(
        {
            "correction_ads": [
                AD_HEADER,
                ["a1", "Renault", "aut", "AUT1", "", "faute de frappe"],
            ]
        }
    )
    assert run(engine, sheet) == {"appliqué": 1}

    ad = get_ad(engine, "a1")
    assert (ad.predicted_brand, ad.predicted_sector, ad.predicted_product_category) == (
        "Renault",
        "AUT",
        "AUT1",
    )
    assert (ad.prediction_status, ad.prediction_method) == ("subcat_done", "manual")
    entry = ad.prediction[-1]
    assert entry["stage"] == "manual_correction"
    assert entry["previous"]["predicted_brand"] == "Renaut"
    assert entry["source"] == {
        "spreadsheet": SPREADSHEET,
        "tab": "correction_ads",
        "row": 2,
    }
    assert sheet.column("correction_ads", "resultat") == ["appliqué"]
    previous = json.loads(sheet.column("correction_ads", "valeurs_precedentes")[0])
    assert previous["predicted_sector"] == "FOD"
    assert sheet.column("correction_ads", "traite_le")[0]


def test_correction_inconsistent_with_the_references_is_applied_with_warnings(engine):
    insert(engine, make_ad("a1"))
    sheet = MemorySheet(
        {"correction_ads": [AD_HEADER, ["a1", "Renaul", "FOD", "AUT1", ""]]}
    )
    assert run(engine, sheet) == {"avertissement": 1}

    assert get_ad(engine, "a1").predicted_brand == "Renaul"
    message = sheet.column("correction_ads", "message")[0]
    assert "la catégorie AUT1 est du secteur AUT, pas de FOD" in message
    assert "marque Renaul inconnue" in message
    assert "proches : Renault" in message


def test_brand_classified_elsewhere_by_the_dictionary_gives_a_warning(engine):
    insert(engine, make_ad("a1"))
    sheet = MemorySheet(
        {"correction_ads": [AD_HEADER, ["a1", "Renault", "FOD", "", ""]]}
    )
    run(engine, sheet)
    assert (
        "le dictionnaire classe Renault dans AUT, pas dans FOD"
        in sheet.column("correction_ads", "message")[0]
    )


def test_invalid_code_is_an_error_and_changes_nothing(engine):
    insert(engine, make_ad("a1", predicted_sector="FOD"))
    sheet = MemorySheet(
        {
            "correction_ads": [
                AD_HEADER,
                ["a1", "Renault", "XXX", "", ""],
                ["inconnue", "Renault", "", "", ""],
                ["a1", "", "", "", "PUB"],
            ]
        }
    )
    assert run(engine, sheet) == {"erreur": 3}
    ad = get_ad(engine, "a1")
    assert (ad.predicted_brand, ad.predicted_sector) == ("Pytest", "FOD")
    assert sheet.column("correction_ads", "valeurs_precedentes") == ["", "", ""]


def test_ad_not_classified_yet_needs_a_content_type(engine):
    insert(engine, make_ad("a1", status=None, fragment_type="advertising", brand=None))
    sheet = MemorySheet(
        {
            "correction_ads": [
                AD_HEADER,
                ["a1", "Renault", "AUT", "", ""],
                ["a1", "Renault", "AUT", "", "AD"],
            ]
        }
    )
    assert run(engine, sheet) == {"erreur": 1, "appliqué": 1}
    assert "renseigner type_contenu" in sheet.column("correction_ads", "message")[0]
    ad = get_ad(engine, "a1")
    assert (ad.fragment_type, ad.prediction_status) == ("AD", "ad_type_done")


def test_content_type_correction_takes_the_ad_out_of_the_analyses(engine):
    insert(
        engine,
        make_ad(
            "a1",
            predicted_sector="AUT",
            predicted_product_category="AUT1",
            prediction=[
                {
                    "stage": "ad_type",
                    "raw_response": {"content_type": "AD", "brand_name": "Pytest"},
                }
            ],
        ),
    )
    sheet = MemorySheet({"correction_ads": [AD_HEADER, ["a1", "", "", "", "bumper"]]})
    assert run(engine, sheet) == {"appliqué": 1}
    ad = get_ad(engine, "a1")
    # like a content the classification found not to be an advertising
    assert (
        ad.fragment_type,
        ad.prediction_status,
        ad.predicted_brand,
        ad.predicted_sector,
        ad.predicted_product_category,
        ad.prediction_method,
    ) == ("BUMPER", "ad_type_done", None, None, None, "manual")
    # the dictionary stage skips it, the brand of the classification stays for a correction back to AD
    assert ad.prediction[0]["raw_response"] == {
        "content_type": "BUMPER",
        "brand_name": "Pytest",
    }
    assert ad.prediction[-1]["previous"]["predicted_brand"] == "Pytest"
    assert ad.prediction[-1]["previous"]["predicted_sector"] == "AUT"


def test_content_type_correction_back_to_ad_goes_back_to_the_classification(engine):
    insert(engine, make_ad("a1", predicted_sector="AUT"))
    sheet = MemorySheet({"correction_ads": [AD_HEADER, ["a1", "", "", "", "bumper"]]})
    run(engine, sheet)
    sheet = MemorySheet({"correction_ads": [AD_HEADER, ["a1", "", "", "", "AD"]]})
    run(engine, sheet)
    ad = get_ad(engine, "a1")
    assert (ad.fragment_type, ad.prediction_status, ad.predicted_brand) == (
        "AD",
        "ad_type_done",
        None,
    )
    assert ad.prediction[0]["raw_response"]["content_type"] == "AD"


def test_not_classified_ad_corrected_to_ad_goes_back_to_the_dictionary_stage(engine):
    insert(
        engine,
        make_ad(
            "a1",
            status="not_ad",
            fragment_type="BUMPER",
            brand=None,
            prediction=[
                {
                    "stage": "ad_type",
                    "raw_response": {"content_type": "BUMPER", "brand_name": None},
                }
            ],
        ),
    )
    sheet = MemorySheet(
        {"correction_ads": [AD_HEADER, ["a1", "Renault", "AUT", "AUT1", "AD"]]}
    )
    assert run(engine, sheet) == {"avertissement": 1}

    ad = get_ad(engine, "a1")
    assert (ad.fragment_type, ad.prediction_status, ad.predicted_sector) == (
        "AD",
        "ad_type_done",
        "AUT",
    )
    raw = ad.prediction[0]["raw_response"]
    assert (raw["content_type"], raw["brand_name"], raw["sector_code"]) == (
        "AD",
        "Renault",
        "AUT",
    )
    assert ad.prediction[-1]["previous"]["prediction_status"] == "not_ad"
    assert "catégorie ignorée" in sheet.column("correction_ads", "message")[0]


def test_each_row_is_processed_once(engine):
    insert(engine, make_ad("a1"))
    sheet = MemorySheet({"correction_ads": [AD_HEADER, ["a1", "Renault", "", "", ""]]})
    assert run(engine, sheet) == {"appliqué": 1}
    assert run(engine, sheet) == {}
    assert len(get_ad(engine, "a1").prediction) == 2


def test_dry_run_writes_nothing(engine):
    insert(engine, make_ad("a1"), make_occ("o1", "a1", 0))
    sheet = MemorySheet(
        {
            "correction_ads": [AD_HEADER, ["a1", "Renault", "", "", ""]],
            "dechets": [["occurrence_id"], ["o1"]],
        }
    )
    assert run(engine, sheet, dry_run=True) == {"appliqué": 2}
    assert get_ad(engine, "a1").predicted_brand == "Pytest"
    with Session(engine) as session:
        assert session.get(Ad_Occurrence, "o1").deleted_at is None
    assert sheet.grid["dechets"] == [["occurrence_id"], ["o1"]]


def test_tabs_for_humans_and_unknown_tabs_are_skipped(engine):
    insert(engine, make_ad("a1"))
    sheet = MemorySheet(
        {
            "_lisez-moi": [AD_HEADER, ["a1", "Renault", "", "", ""]],
            "autre": [AD_HEADER, ["a1", "Renault", "", "", ""]],
        }
    )
    assert run(engine, sheet) == {}
    assert get_ad(engine, "a1").predicted_brand == "Pytest"


# ---------- junk occurrences ----------


def test_junk_occurrences_are_soft_deleted(engine):
    insert(
        engine,
        make_ad("a1"),
        make_occ("o1", "a1", 0),
        make_occ("o2", "a1", 60),
    )
    with Session(engine) as session:
        session.get(Ad_Occurrence, "o2").deleted_at = T0
        session.commit()
    sheet = MemorySheet({"dechets": [["occurrence_id"], ["o1"], ["o2"], ["inconnue"]]})
    assert run(engine, sheet) == {"appliqué": 1, "avertissement": 1, "erreur": 1}
    with Session(engine) as session:
        assert session.get(Ad_Occurrence, "o1").deleted_at is not None
    assert sheet.column("dechets", "resultat") == [
        "appliqué",
        "avertissement",
        "erreur",
    ]


# ---------- reattribution to an existing ad ----------


def test_occurrences_are_moved_to_an_existing_ad(engine):
    insert(
        engine,
        make_ad("wrong"),
        make_ad("right"),
        make_occ("o1", "wrong", 0),
        make_occ("o2", "wrong", 600),
    )
    sheet = MemorySheet(
        {
            "reattribution": [
                ["occurrence_id", "ad_id", "commentaire"],
                ["o1", "right", "même spot"],
                ["o2", "right", ""],
            ]
        }
    )
    assert run(engine, sheet) == {"appliqué": 2}

    with Session(engine) as session:
        live = {
            o.id: o.ad_id
            for o in session.scalars(
                select(Ad_Occurrence).where(Ad_Occurrence.deleted_at.is_(None))
            )
        }
        assert session.get(Ad_Occurrence, "o1").deleted_at is not None
        assert session.get(Ad_Occurrence, "o2").deleted_at is not None
    new_ids = sheet.column("reattribution", "nouvelle_occurrence_id")
    assert live == {new_ids[0]: "right", new_ids[1]: "right"}


def test_reattribution_errors_change_nothing(engine):
    insert(
        engine,
        make_ad("wrong"),
        make_ad("right"),
        make_occ("o1", "wrong", 0),
        make_occ("o2", "right", 600),
        make_occ("o3", "wrong", 1200),
    )
    with Session(engine) as session:
        session.get(Ad_Occurrence, "o3").deleted_at = T0
        session.commit()
    sheet = MemorySheet(
        {
            "reattribution": [
                ["occurrence_id", "ad_id"],
                ["o1", "inconnue"],
                ["o1", ""],
                ["inconnue", "right"],
                ["o2", "right"],
                ["o3", "right"],
            ]
        }
    )
    assert run(engine, sheet) == {"erreur": 5}
    assert sheet.column("reattribution", "message") == [
        "ad inconnue inconnue",
        "occurrence en double dans l'onglet",
        "occurrence inconnue inconnue",
        "occurrence déjà dans cette ad",
        f"occurrence supprimée le {T0:%Y-%m-%d}",
    ]
    with Session(engine) as session:
        assert session.get(Ad_Occurrence, "o1").deleted_at is None
        assert session.get(Ad_Occurrence, "o1").ad_id == "wrong"


def test_reattribution_to_an_ad_of_another_duration_or_not_classified_gives_warnings(
    engine,
):
    insert(
        engine,
        make_ad("wrong", duration_sec=20.0),
        make_ad("bumper", fragment_type="BUMPER", duration_sec=5.0),
        make_occ("o1", "wrong", 0),
    )
    sheet = MemorySheet(
        {"reattribution": [["occurrence_id", "ad_id"], ["o1", "bumper"]]}
    )
    assert run(engine, sheet) == {"avertissement": 1}
    message = sheet.column("reattribution", "message")[0]
    assert "durée de l'ad (5s) différente de celle de l'ad d'origine (20s)" in message
    assert "l'occurrence n'apparaît pas dans les analyses" in message


# ---------- new ads ----------


def test_new_ads_from_occurrences_one_per_ad_indicator(engine):
    insert(
        engine,
        make_ad("a1"),
        make_occ("o1", "a1", 0),
        make_occ("o2", "a1", 600),
        make_occ("o3", "a1", 1200),
        make_occ("o4", "a1", 1800),
    )
    sheet = MemorySheet(
        {
            "nouvelle_ad_avril": [
                ["occurrence_id", "ad_indicator"],
                ["o1", "MAZDA"],
                ["o2", "RENAULT"],
                ["o3", "MAZDA"],
                ["inconnue", "MAZDA"],
            ]
        }
    )
    exported = []

    def export_video(ad_id, occurrences, duration_sec, dry_run):
        exported.append((ad_id, sorted(o.id for o in occurrences), duration_sec))
        return occurrences[-1]

    assert run(engine, sheet, export_video) == {"appliqué": 3, "erreur": 1}

    mazda = new_ad_id(SPREADSHEET, "nouvelle_ad_avril", "MAZDA")
    renault = new_ad_id(SPREADSHEET, "nouvelle_ad_avril", "RENAULT")
    assert sorted(exported) == sorted(
        [(mazda, ["o1", "o3"], 20.0), (renault, ["o2"], 20.0)]
    )
    with Session(engine) as session:
        live = {
            o.ad_id: o.occurrence_date
            for o in session.scalars(
                select(Ad_Occurrence).where(Ad_Occurrence.deleted_at.is_(None))
            )
            if o.ad_id != "a1"
        }
        assert set(live) == {mazda, renault}
        assert sorted(
            o.id
            for o in session.scalars(
                select(Ad_Occurrence).where(Ad_Occurrence.deleted_at.is_not(None))
            )
        ) == ["o1", "o2", "o3"]
        new_ad = session.get(Ad, mazda)
        assert (new_ad.chunks, new_ad.prediction_status, new_ad.fragment_type) == (
            [],
            None,
            "advertising",
        )
        assert new_ad.first_detection_date == T0
    assert sheet.column("nouvelle_ad_avril", "nouvelle_ad_id") == [
        mazda,
        renault,
        mazda,
        "",
    ]
    assert sheet.column("nouvelle_ad_avril", "resultat") == [
        "appliqué",
        "appliqué",
        "appliqué",
        "erreur",
    ]
    # the old occurrence keeps its ad, with a deleted_at: the new one is in nouvelle_occurrence_id
    new_occurrences = sheet.column("nouvelle_ad_avril", "nouvelle_occurrence_id")
    assert new_occurrences[3] == ""
    with Session(engine) as session:
        assert session.get(Ad_Occurrence, "o1").ad_id == "a1"
        assert [
            session.get(Ad_Occurrence, occurrence_id).ad_id
            for occurrence_id in new_occurrences[:3]
        ] == [mazda, renault, mazda]


def test_new_ad_without_video_is_not_created(engine):
    insert(engine, make_ad("a1"), make_occ("o1", "a1", 0))
    sheet = MemorySheet({"nouvelle_ad": [["occurrence_id"], ["o1"]]})
    assert run(engine, sheet, lambda *args: None) == {"erreur": 1}
    with Session(engine) as session:
        assert session.get(Ad, new_ad_id(SPREADSHEET, "nouvelle_ad", "")) is None
        assert session.get(Ad_Occurrence, "o1").deleted_at is None
    assert "aucune occurrence n'a sa vidéo" in sheet.column("nouvelle_ad", "message")[0]


def test_rows_added_later_go_to_the_same_new_ad(engine):
    insert(engine, make_ad("a1"), make_occ("o1", "a1", 0), make_occ("o2", "a1", 600))
    sheet = MemorySheet({"nouvelle_ad": [["occurrence_id"], ["o1"]]})
    run(engine, sheet)
    sheet.grid["nouvelle_ad"].append(["o2"])
    exported = []
    run(engine, sheet, lambda *args: exported.append(args))
    # the ad exists: no new video
    assert exported == []
    ad_id = new_ad_id(SPREADSHEET, "nouvelle_ad", "")
    assert sheet.column("nouvelle_ad", "nouvelle_ad_id") == [ad_id, ad_id]
    with Session(engine) as session:
        assert {
            o.ad_id
            for o in session.scalars(
                select(Ad_Occurrence).where(Ad_Occurrence.deleted_at.is_(None))
            )
        } == {ad_id}


def test_new_ad_from_ads_of_different_durations_gives_a_warning(engine):
    insert(
        engine,
        make_ad("a1", duration_sec=20.0),
        make_ad("a2", duration_sec=30.0),
        make_occ("o1", "a1", 0),
        make_occ("o2", "a1", 600),
        make_occ("o3", "a2", 1200),
    )
    sheet = MemorySheet({"nouvelle_ad": [["occurrence_id"], ["o1"], ["o2"], ["o3"]]})
    assert run(engine, sheet) == {"avertissement": 3}
    assert (
        get_ad(engine, new_ad_id(SPREADSHEET, "nouvelle_ad", "")).duration_sec == 20.0
    )
    assert "durée retenue 20s" in sheet.column("nouvelle_ad", "message")[0]


# ---------- video of a new ad ----------


def test_video_is_cut_from_the_most_recent_occurrence_with_its_archives(monkeypatch):
    from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections import (
        video,
    )

    old = make_occ("old", "a1", 0)
    middle = make_occ("middle", "a1", 3600)
    recent = make_occ("recent", "a1", 7200)
    ad = SimpleNamespace(id="new", duration_sec=20.0)
    # the archives of the most recent occurrence are missing
    archives = video.required_part_starts(
        [video._ad_export_window(ad, old), video._ad_export_window(ad, middle)]
    )
    exported = []

    async def available(fs, channel, day):
        return archives

    async def download(fs, channel, part_starts, local_dir, disable_progress):
        return {start: {"mp4": "x"} for start in part_starts}

    async def export(ad, occurrence, from_date, to_date, parts, local_dir, fs):
        exported.append((ad.id, occurrence.id))

    class Filesystem:
        async def set_session(self):
            return SimpleNamespace(close=lambda: asyncio.sleep(0))

    monkeypatch.setattr(video, "get_s3_filesystem", lambda **kwargs: Filesystem())
    monkeypatch.setattr(video, "list_available_part_starts", available)
    monkeypatch.setattr(video, "download_media_parts", download)
    monkeypatch.setattr(video, "_export_ad", export)

    occurrences = [old, recent, middle]
    assert video.export_new_ad_video("new", occurrences, 20.0, dry_run=True) is middle
    assert exported == []
    assert video.export_new_ad_video("new", occurrences, 20.0, dry_run=False) is middle
    assert exported == [("new", "middle")]


class EmptyBucketHandler(BaseHTTPRequestHandler):
    """A S3 endpoint whose bucket has no archive: every listing is empty."""

    requests: list[str] = []

    def do_GET(self):
        EmptyBucketHandler.requests.append(self.path)
        body = (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            b"<Name>mediatree-videos-prod</Name><KeyCount>0</KeyCount>"
            b"<IsTruncated>false</IsTruncated></ListBucketResult>"
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def local_s3(monkeypatch):
    from quotaclimat.data_ingestion.advertising.tools.mediatree import (
        bucket_mediatree,
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), EmptyBucketHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    EmptyBucketHandler.requests = []
    monkeypatch.setattr(
        bucket_mediatree, "ENDPOINT_URL", f"http://127.0.0.1:{server.server_port}"
    )
    monkeypatch.setattr(bucket_mediatree, "ACCESS_KEY", "key")
    monkeypatch.setattr(bucket_mediatree, "SECRET_KEY", "secret")
    for proxy in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.delenv(proxy, raising=False)
    yield EmptyBucketHandler.requests
    server.shutdown()
    server.server_close()


def test_videos_of_several_new_ads_in_the_same_job(local_s3):
    # one event loop per new ad: the S3 session of the first one must not be reused by the next ones
    from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections import (
        video,
    )

    for ad_id in ("new1", "new2"):
        occurrences = [make_occ(f"{ad_id}-occ", "a1", 0)]
        assert video.export_new_ad_video(ad_id, occurrences, 20.0, dry_run=True) is None
    assert len(local_s3) >= 2
