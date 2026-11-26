"""Export of the report data to Google Sheets (quotaclimat/reports/export_report_data.py), without database nor Google."""

import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest
import requests

from quotaclimat.reports import export_report_data as export
from quotaclimat.utils.google_sheets import DRIVE_FILES_API_URL, SHEETS_API_URL

REPORT = "2027-02-pub-exemple"
# the Google Sheet is named like the report
SPREADSHEET = REPORT
# values that must never reach the logs
SECRET_TEXT = "Annonceur confidentiel SECRET_42"
SECRET_NUMBER = 98765.4321


def write_report(tmp_path: Path, report: str = REPORT) -> Path:
    queries = tmp_path / report / "queries"
    queries.mkdir(parents=True)
    (queries / "bulles_sheet.sql").write_text("-- bulles\nSELECT 1 AS etiquette;\n")
    (queries / "secteurs_sheet.sql").write_text("SELECT 'a' AS secteur")
    # not exported: no _sheet suffix, or not a query
    (queries / "exploration.sql").write_text("SELECT 'exploration'")
    (queries / "notes.md").write_text("not a query")
    return tmp_path


# ------------------------------------------------------------------ configuration


def test_load_config_from_the_naming_conventions(tmp_path):
    """No configuration file: the Sheet is named like the report, queries/<tab>_sheet.sql goes to the tab <tab>, the
    other queries are not exported."""
    config = export.load_config(REPORT, write_report(tmp_path))
    assert config.report == REPORT
    assert config.queries_dir == tmp_path / REPORT / "queries"
    assert config.outputs == (
        export.SheetOutput(
            spreadsheet=REPORT,
            tabs=(
                export.TabExport("bulles", "bulles_sheet.sql"),
                export.TabExport("secteurs", "secteurs_sheet.sql"),
            ),
        ),
    )


@pytest.mark.parametrize(
    "report",
    [
        "",
        "../2027-02-pub",
        "2027-02-pub/../../x",
        "2027-13-pub",
        "27-02-pub",
        "2027-02-Pub",
        "2027-02-pub-",
    ],
)
def test_invalid_report_id(tmp_path, report):
    with pytest.raises(export.ExportError, match="invalid report"):
        export.load_config(report, tmp_path)


def test_report_without_queries_to_export_lists_available_reports(tmp_path):
    write_report(tmp_path)
    queries = tmp_path / "2027-03-autre" / "queries"
    queries.mkdir(parents=True)
    (queries / "exploration.sql").write_text("SELECT 1")
    with pytest.raises(
        export.ExportError,
        match=r"no query to export in 2027-03-autre/queries/.*\['2027-02-pub-exemple'\]",
    ):
        export.load_config("2027-03-autre", tmp_path)
    with pytest.raises(export.ExportError, match="no query to export"):
        export.load_config("2027-04-absent", tmp_path)


@pytest.mark.parametrize("name", ["_cachee", "a b", "é", "x" * 101])
def test_invalid_tab_name(tmp_path, name):
    write_report(tmp_path)
    (tmp_path / REPORT / "queries" / f"{name}_sheet.sql").write_text("SELECT 1")
    with pytest.raises(export.ExportError, match=r"tab name|too long"):
        export.load_config(REPORT, tmp_path)


def test_tab_name():
    assert (
        export.tab_name("part_exposee_par_annonceur_sheet.sql")
        == "part_exposee_par_annonceur"
    )


def test_report_queries_of_the_repository_are_valid():
    """Every report of decisions/rapports/ with queries to export gives valid tab names, and each query is a single
    statement."""
    reports = export.available_reports()
    assert "2027-02-pub-desinformation" in reports
    for report in reports:
        config = export.load_config(report)
        for output in config.outputs:
            assert output.spreadsheet == report
            for tab in output.tabs:
                export.single_statement(
                    (config.queries_dir / tab.query).read_text(encoding="utf-8")
                )


# ------------------------------------------------------------------ queries


@pytest.mark.parametrize(
    "sql, statement",
    [
        ("SELECT 1", "SELECT 1"),
        ("SELECT 1;\n-- fin\n", "SELECT 1"),
        ("SELECT 1 ; ; \n", "SELECT 1 "),
        ("SELECT 1 /* ; */;", "SELECT 1 /* ; */"),
        (
            "-- a; b\nSELECT ';' AS x, \"a;b\" FROM t",
            "-- a; b\nSELECT ';' AS x, \"a;b\" FROM t",
        ),
        ("SELECT 'it''s; ok'", "SELECT 'it''s; ok'"),
        ("SELECT $$;$$, $tag$ ; $tag$", "SELECT $$;$$, $tag$ ; $tag$"),
        ("SELECT 1; /* fin */ -- fin", "SELECT 1"),
    ],
)
def test_single_statement(sql, statement):
    assert export.single_statement(sql) == statement


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DELETE FROM t",
        "SELECT 1; COMMIT; SELECT 2;",
        "SELECT ';'; DROP TABLE t",
    ],
)
def test_single_statement_refuses_several_statements(sql):
    with pytest.raises(export.ExportError, match="several statements"):
        export.single_statement(sql)


@pytest.mark.parametrize("sql", ["", "  ;", "-- rien\n"])
def test_single_statement_refuses_empty_file(sql):
    with pytest.raises(export.ExportError, match="empty"):
        export.single_statement(sql)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.itersize = None
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement):
        self.connection.executed.append(statement)
        if self.connection.error:
            raise self.connection.error
        columns, self._rows = self.connection.results[statement]
        self._rows = list(self._rows)
        self.description = [(c,) for c in columns]

    def fetchmany(self, size):
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch


class FakeConnection:
    def __init__(self, results=None, error=None):
        self.results = results or {}
        self.error = error
        self.executed = []
        self.cursor_names = []
        self.sessions = []
        self.rollbacks = 0
        self.closed = False

    def set_session(self, **kwargs):
        self.sessions.append(kwargs)

    def cursor(self, name=None):
        self.cursor_names.append(name)
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_run_query_read_only_server_side_cursor():
    connection = FakeConnection(
        {"SELECT x FROM t": (["x"], [(i,) for i in range(25_001)])}
    )
    result = export.run_query(connection, "SELECT x FROM t;\n")
    assert result.columns == ["x"]
    assert len(result.rows) == 25_001
    assert connection.sessions == [{"readonly": True, "autocommit": False}]
    assert connection.cursor_names == [
        "report_export"
    ]  # named: DECLARE ... CURSOR FOR, a single SELECT
    assert connection.rollbacks == 1


def test_run_query_rolls_back_on_error():
    connection = FakeConnection(
        error=psycopg2.errors.ReadOnlySqlTransaction("cannot execute DELETE")
    )
    with pytest.raises(psycopg2.Error):
        export.run_query(connection, "SELECT 1")
    assert connection.rollbacks == 1


# ------------------------------------------------------------------ Google Sheets


@pytest.mark.parametrize(
    "value, cell",
    [
        (None, {}),
        (True, {"userEnteredValue": {"boolValue": True}}),
        (3, {"userEnteredValue": {"numberValue": 3.0}}),
        (Decimal("0.25"), {"userEnteredValue": {"numberValue": 0.25}}),
        (float("nan"), {}),
        (date(2026, 11, 17), {"userEnteredValue": {"stringValue": "2026-11-17"}}),
        (
            '=IMPORTDATA("https://x")',
            {"userEnteredValue": {"stringValue": '=IMPORTDATA("https://x")'}},
        ),
    ],
)
def test_to_cell(value, cell):
    assert export.to_cell(value) == cell


def test_build_requests_existing_new_and_small_tabs():
    existing = [
        {
            "sheetId": 7,
            "title": "donnees",
            "sheetType": "GRID",
            "gridProperties": {"rowCount": 1000, "columnCount": 26},
        },
        {
            "sheetId": 8,
            "title": "petit",
            "sheetType": "GRID",
            "gridProperties": {"rowCount": 2, "columnCount": 1},
        },
    ]
    contents = {
        "donnees": export.TabContent([["a"], [1]]),
        "petit": export.TabContent([["a", "b"], [1, 2], [3, 4]]),
        "nouveau": export.TabContent([["a"]]),
    }
    reqs = export.build_requests(existing, contents)

    assert reqs[0] == {
        "updateCells": {"range": {"sheetId": 7}, "fields": "userEnteredValue"}
    }  # clear all values
    assert reqs[1]["updateCells"]["start"] == {
        "sheetId": 7,
        "rowIndex": 0,
        "columnIndex": 0,
    }
    assert reqs[1]["updateCells"]["rows"] == [
        {"values": [{"userEnteredValue": {"stringValue": "a"}}]},
        {"values": [{"userEnteredValue": {"numberValue": 1.0}}]},
    ]
    assert reqs[2]["updateSheetProperties"]["properties"] == {
        "sheetId": 8,
        "gridProperties": {"rowCount": 3, "columnCount": 2},
    }
    assert reqs[3] == {
        "updateCells": {"range": {"sheetId": 8}, "fields": "userEnteredValue"}
    }
    added = reqs[5]["addSheet"]["properties"]
    assert added["title"] == "nouveau" and added["sheetId"] not in (7, 8)
    assert reqs[6]["updateCells"]["start"]["sheetId"] == added["sheetId"]
    assert len(reqs) == 7


def test_build_requests_columns_only():
    """With columns, only these columns are cleared (every row) and written: a chart or notes next to the
    data stay as they are."""
    existing = [
        {
            "sheetId": 7,
            "title": "donnees",
            "sheetType": "GRID",
            "gridProperties": {"rowCount": 1000, "columnCount": 4},
        }
    ]
    reqs = export.build_requests(
        existing, {"donnees": export.TabContent([["a", "b"], [1, 2]], (2, 10))}
    )
    # the grid is enlarged to hold the columns C:J
    assert reqs[0]["updateSheetProperties"]["properties"]["gridProperties"] == {
        "rowCount": 1000,
        "columnCount": 10,
    }
    assert reqs[1] == {
        "updateCells": {
            "range": {"sheetId": 7, "startColumnIndex": 2, "endColumnIndex": 10},
            "fields": "userEnteredValue",
        }
    }
    assert reqs[2]["updateCells"]["start"] == {
        "sheetId": 7,
        "rowIndex": 0,
        "columnIndex": 2,
    }


def test_build_requests_refuses_chart_sheet():
    with pytest.raises(export.ExportError, match="not a grid"):
        export.build_requests(
            [{"sheetId": 1, "title": "donnees", "sheetType": "OBJECT"}],
            {"donnees": export.TabContent([["a"]])},
        )


def test_check_size():
    export.check_size([["a"] * 10] * 100)
    with pytest.raises(export.ExportError, match="more than the limit"):
        export.check_size([["a"] * 100] * 10_001)


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload or {}
        self.status_code = status_code
        self.reason = "Bad Request" if status_code >= 400 else "OK"

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            # Google's error bodies can quote the values sent
            raise requests.HTTPError(
                f"{self.status_code} for url: invalid value {SECRET_TEXT}",
                response=self,
            )


class FakeSession:
    def __init__(self, files=None, post_status=200):
        self.files = (
            [{"id": "SHEET_ID_1234567890", "name": SPREADSHEET}]
            if files is None
            else files
        )
        self.post_status = post_status
        self.folders = [{"id": "REPORTS_FOLDER_123", "name": "Rapports"}]
        self.gets = []
        self.posts = []

    def get(self, url, params=None, timeout=None):
        self.gets.append((url, params))
        if url == DRIVE_FILES_API_URL:
            if "application/vnd.google-apps.folder" in params["q"]:
                return FakeResponse({"files": self.folders})
            return FakeResponse({"files": self.files})
        return FakeResponse(
            {
                "sheets": [
                    {
                        "properties": {
                            "sheetId": 0,
                            "title": "bulles",
                            "sheetType": "GRID",
                            "gridProperties": {"rowCount": 1000, "columnCount": 26},
                        }
                    },
                    {
                        "properties": {
                            "sheetId": 5,
                            "title": "graphique",
                            "sheetType": "GRID",
                            "gridProperties": {"rowCount": 1000, "columnCount": 26},
                        }
                    },
                ]
            }
        )

    def post(self, url, json=None, timeout=None):
        self.posts.append((url, json))
        return FakeResponse({}, self.post_status)


def query_results():
    return {
        "-- bulles\nSELECT 1 AS etiquette": (
            ["etiquette", "x"],
            [(SECRET_TEXT, Decimal(str(SECRET_NUMBER))), (None, 2)],
        ),
        "SELECT 'a' AS secteur": (["secteur"], [("Alimentation",)]),
    }


def run_export(
    tmp_path, session, connection, apply=True, check_public=lambda spreadsheet: None
):
    write_report(tmp_path)
    return export.export_report(
        REPORT,
        apply=apply,
        reports_dir=tmp_path,
        connect_db=lambda: connection,
        open_session=lambda: session,
        folder_id="FOLDER_ID_123",
        check_public=check_public,
    )


def assert_no_data_logged(caplog):
    text = caplog.text + " ".join(str(r.args) for r in caplog.records)
    assert "SECRET_42" not in text
    assert "98765" not in text
    assert "Alimentation" not in text


def test_export_report_writes_all_tabs_in_one_request(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    checked = []
    session, connection = FakeSession(), FakeConnection(query_results())

    assert (
        run_export(tmp_path, session, connection, check_public=checked.append) is True
    )

    assert (
        checked and checked[0]["id"] == "SHEET_ID_1234567890"
    )  # privacy checked before writing
    assert session.gets[0][1]["q"].startswith(
        "'FOLDER_ID_123' in parents and name = '2027-02-pub-exemple'"
    )
    assert len(session.posts) == 1  # one batchUpdate: all the tabs or none
    url, body = session.posts[0]
    assert url == f"{SHEETS_API_URL}/SHEET_ID_1234567890:batchUpdate"
    writes = [
        r["updateCells"] for r in body["requests"] if "rows" in r.get("updateCells", {})
    ]
    assert writes[0]["start"]["sheetId"] == 0  # existing tab bulles
    # only the 2 columns of the result (A:B) are cleared: a chart on their right is kept
    assert {
        "updateCells": {
            "range": {"sheetId": 0, "startColumnIndex": 0, "endColumnIndex": 2},
            "fields": "userEnteredValue",
        }
    } in body["requests"]
    assert writes[0]["rows"][0] == {
        "values": [
            {"userEnteredValue": {"stringValue": "etiquette"}},
            {"userEnteredValue": {"stringValue": "x"}},
        ]
    }
    assert writes[0]["rows"][1]["values"][1] == {
        "userEnteredValue": {"numberValue": SECRET_NUMBER}
    }
    assert writes[0]["rows"][2]["values"][0] == {}  # NULL: empty cell
    added = [
        r["addSheet"]["properties"]["title"]
        for r in body["requests"]
        if "addSheet" in r
    ]
    assert added == [
        "secteurs",
        "_export",
    ]  # missing tabs created, graphique untouched
    assert all(
        r.get("updateCells", {}).get("range", {}).get("sheetId") != 5
        for r in body["requests"]
    )
    export_rows = writes[-1]["rows"]
    assert (
        export_rows[0]["values"][0]["userEnteredValue"]["stringValue"]
        == "Dernier export (heure de Paris)"
    )
    assert export_rows[-1]["values"][2] == {
        "userEnteredValue": {"numberValue": 1.0}
    }  # rows of secteurs
    # each query in its own read-only transaction
    assert len(connection.executed) == 2
    assert connection.closed
    assert (
        "query bulles_sheet.sql -> 2 rows, 2 columns for the tab 'bulles'"
        in caplog.text
    )
    assert_no_data_logged(caplog)


def test_export_report_check_only_writes_nothing(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    session = FakeSession()
    assert (
        run_export(tmp_path, session, FakeConnection(query_results()), apply=False)
        is True
    )
    assert session.posts == []
    assert "check only, tab 'bulles' would be replaced" in caplog.text
    assert "check only, tab 'secteurs' would be created" in caplog.text
    assert_no_data_logged(caplog)


def test_export_report_query_error_logs_no_data(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    error = psycopg2.errors.InvalidTextRepresentation(
        f'invalid input syntax for type integer: "{SECRET_TEXT}"'
    )
    session = FakeSession()
    assert run_export(tmp_path, session, FakeConnection(error=error)) is False
    assert session.posts == []  # nothing written when a query fails
    assert (
        "query bulles_sheet.sql: database error InvalidTextRepresentation"
        in caplog.text
    )
    assert_no_data_logged(caplog)


def test_export_report_google_error_logs_no_data(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    assert (
        run_export(
            tmp_path, FakeSession(post_status=400), FakeConnection(query_results())
        )
        is False
    )
    assert "Google API error HTTP 400 Bad Request, nothing written" in caplog.text
    assert_no_data_logged(caplog)


def test_export_report_refuses_public_spreadsheet(tmp_path, caplog):
    """The real check (ensure_not_public): refused from its Drive permissions, before any query."""
    session = FakeSession(
        files=[{"id": "SHEET_ID_1234567890", "permissionIds": ["anyoneWithLink"]}]
    )
    connection = FakeConnection(query_results())
    assert (
        run_export(tmp_path, session, connection, check_public=export.ensure_not_public)
        is False
    )
    assert "shared publicly" in caplog.text
    assert session.posts == [] and connection.executed == []


def test_export_report_spreadsheet_not_found(tmp_path, caplog):
    session = FakeSession(files=[])
    assert run_export(tmp_path, session, FakeConnection(query_results())) is False
    assert "0 spreadsheets named" in caplog.text


def test_main_logs_no_data_on_connection_error(monkeypatch, caplog, tmp_path):
    caplog.set_level(logging.DEBUG)
    write_report(tmp_path)
    monkeypatch.setattr(export, "REPORTS_DIR", tmp_path)

    def failing_export(report, apply):
        raise psycopg2.OperationalError(
            f"connection to server failed: password {SECRET_TEXT}"
        )

    monkeypatch.setattr(export, "export_report", failing_export)
    assert export.main(["--report", REPORT]) == 1
    assert "database error OperationalError" in caplog.text
    assert_no_data_logged(caplog)


def test_main_check_flag(monkeypatch):
    calls = []
    monkeypatch.setattr(
        export,
        "export_report",
        lambda report, apply: calls.append((report, apply)) or True,
    )
    monkeypatch.setenv("REPORT_ID", REPORT)
    assert export.main(["--check"]) == 0
    assert calls == [(REPORT, False)]


def test_export_tab_values():
    values = export.export_tab_values(
        REPORT,
        datetime(2026, 11, 17, 10, 5, tzinfo=export.PARIS),
        "2.1.252",
        [("donnees", "bulles.sql", 3)],
    )
    assert values[0] == ["Dernier export (heure de Paris)", "2026-11-17 10:05:00"]
    assert values[-1] == ["donnees", "queries/bulles.sql", 3]


def test_export_report_finds_the_reports_folder(tmp_path, monkeypatch):
    """Without folder_id, the spreadsheets are looked up in the folder "Rapports" of the shared folder of
    EXTERNAL_SOURCES_DRIVE_FOLDER."""
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", "SHARED_FOLDER_123")
    monkeypatch.delenv("REPORTS_DRIVE_FOLDER_NAME", raising=False)
    write_report(tmp_path)
    session = FakeSession()
    assert (
        export.export_report(
            REPORT,
            reports_dir=tmp_path,
            connect_db=lambda: FakeConnection(query_results()),
            open_session=lambda: session,
            check_public=lambda spreadsheet: None,
        )
        is True
    )
    folder_query, sheet_query = session.gets[0][1]["q"], session.gets[1][1]["q"]
    assert folder_query == (
        "'SHARED_FOLDER_123' in parents and name = 'Rapports'"
        " and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    assert sheet_query.startswith("'REPORTS_FOLDER_123' in parents and name = ")


def test_reports_folder_name_from_env(monkeypatch):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", "SHARED_FOLDER_123")
    monkeypatch.setenv("REPORTS_DRIVE_FOLDER_NAME", "Autres rapports")
    session = FakeSession()
    assert export.reports_folder_id(session) == "REPORTS_FOLDER_123"
    assert "name = 'Autres rapports'" in session.gets[0][1]["q"]


def test_reports_folder_missing(monkeypatch):
    monkeypatch.setenv("EXTERNAL_SOURCES_DRIVE_FOLDER", "SHARED_FOLDER_123")
    session = FakeSession()
    session.folders = []
    with pytest.raises(export.GoogleSheetsError, match="0 folders named 'Rapports'"):
        export.reports_folder_id(session)
