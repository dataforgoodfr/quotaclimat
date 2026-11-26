"""Export the data of a report to Google Sheets: runs the SQL queries of the report on the barometre database and
writes each result to a tab of a Google Sheet, replacing the whole tab, or only its columns given in
export_data.yml (e.g. A:H, a chart next to them being kept), headers included.

Run by hand by the flow export_report_data (Windmill f/quotaclimat/export_report_data, Kestra
quotaclimat/export-report-data):

    python -m quotaclimat.reports.export_report_data --report 2027-02-pub-desinformation [--check]

The report is the name of its folder decisions/rapports/<AAAA-MM>-<sujet>/. Its file export_data.yml lists the
outputs (format: decisions/rapports/export_data.md); the queries are the files of its
queries/ folder, plain SQL without Metabase variables, one SELECT (or WITH ... SELECT) per file.

Google Sheets: never named by id (the repository is public), looked up by exact name in the folder "Rapports"
(REPORTS_DRIVE_FOLDER_NAME) of the shared Drive folder of the reference data (EXTERNAL_SOURCES_DRIVE_FOLDER, the
only id kept in a secret), and refused when readable without credentials. They are created by hand in that folder,
which is shared as editor with the service account of GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON; the job never
creates a spreadsheet, but creates a missing tab. All the tabs of one spreadsheet, and its tab _export (date of the
export, row counts), are written in a single request: either all of them change, or none.

Database: POSTGRES_* env variables, meant for a read-only user. On top of that, every query runs in a read-only
transaction (default_transaction_read_only, BEGIN READ ONLY), through a server-side cursor (DECLARE ... CURSOR FOR
accepts a single SELECT, and no data-modifying WITH), after a check that the file holds a single statement.

The results are unpublished monitoring data: nothing of them is logged, nor sent to Sentry (not initialized here).
Logs only hold names of reports, queries, spreadsheets and tabs, and row counts; errors are reduced to their type,
their SQLSTATE code or their HTTP status (error messages of Postgres or Google can quote values).
"""

import argparse
import logging
import math
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import requests
import tomllib
import yaml

from quotaclimat.utils.google_sheets import (
    SHEETS_API_URL,
    TIMEOUT_SEC,
    GoogleSheetsError,
    ensure_not_public,
    find_folder,
    find_spreadsheet,
    parse_folder_id,
    service_account_session,
)

REPORTS_DIR = Path("decisions/rapports")
CONFIG_FILE = "export_data.yml"
QUERIES_DIR = "queries"
# <AAAA-MM>-<sujet>: 2027-02-pub-desinformation
REPORT_ID_PATTERN = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])-[a-z0-9]+(-[a-z0-9]+)*$")
QUERY_FILE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+\.sql$")
COLUMNS_PATTERN = re.compile(r"^([A-Z]{1,2}):([A-Z]{1,2})$")
OUTPUT_TYPES = {"sheet"}
CREDENTIALS_ENV = "GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON"
# shared Drive folder of the reference data (my_dbt_project/external_sources.yml), holding the folder of the reports
FOLDER_ENV = "EXTERNAL_SOURCES_DRIVE_FOLDER"
REPORTS_FOLDER_NAME_ENV = "REPORTS_DRIVE_FOLDER_NAME"
REPORTS_FOLDER_NAME = "Rapports"
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",  # find the spreadsheet in the folder
    "https://www.googleapis.com/auth/spreadsheets",  # write its tabs
]
# tab written by the job with the date of the export; the tabs of the queries cannot start with "_"
# (tabs for humans, see download_external_sources.HUMAN_TAB_PREFIX)
EXPORT_TAB = "_export"
MAX_TAB_NAME_LENGTH = 100  # Google Sheets limit
MAX_CELLS_PER_TAB = 1_000_000  # Google Sheets allows 10 million cells per spreadsheet
STATEMENT_TIMEOUT_MS = 15 * 60 * 1000
FETCH_SIZE = 10_000
PARIS = ZoneInfo("Europe/Paris")

logger = logging.getLogger(__name__)


class ExportError(Exception):
    """Error whose message holds no data: safe to log."""


@dataclass(frozen=True)
class TabExport:
    tab: str
    query: str  # file name in queries/
    # columns written, 0-based, end excluded: None = the whole tab; (0, 8) = A:H, the other columns untouched
    columns: tuple[int, int] | None = None


@dataclass(frozen=True)
class TabContent:
    """Values of a tab, header first, written from the first column of columns (A when None)."""

    values: list[list]
    columns: tuple[int, int] | None = None


@dataclass(frozen=True)
class SheetOutput:
    spreadsheet: str  # exact name of the Google Sheet in the Drive folder
    tabs: tuple[TabExport, ...]


@dataclass(frozen=True)
class ExportConfig:
    report: str
    queries_dir: Path
    outputs: tuple[SheetOutput, ...]


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[tuple]


# ------------------------------------------------------------------ configuration


def check_report_id(report: str) -> str:
    report = (report or "").strip()
    if not REPORT_ID_PATTERN.match(report):
        raise ExportError(
            f"invalid report {report!r}: expected the name of its folder decisions/rapports/<AAAA-MM>-<sujet>/"
        )
    return report


def available_reports(reports_dir: Path = REPORTS_DIR) -> list[str]:
    """Reports whose folder has an export_data.yml."""
    return sorted(p.parent.name for p in reports_dir.glob(f"*/{CONFIG_FILE}"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ExportError(message)


def column_index(letters: str) -> int:
    """A -> 0, Z -> 25, AA -> 26."""
    index = 0
    for letter in letters:
        index = index * 26 + ord(letter) - ord("A") + 1
    return index - 1


def parse_columns(value: object, where: str) -> tuple[int, int] | None:
    """'A:H' -> (0, 8): columns written by the export, the other ones left as they are."""
    if value is None:
        return None
    match = COLUMNS_PATTERN.match(value) if isinstance(value, str) else None
    _require(match is not None, f"{where}: columns must be a range of columns like A:H")
    start, end = column_index(match.group(1)), column_index(match.group(2))
    _require(start <= end, f"{where}: columns {value}: first column after the last one")
    return start, end + 1


def _check_keys(item: dict, allowed: set[str], where: str) -> None:
    unknown = set(item) - allowed
    _require(
        not unknown,
        f"{where}: unknown keys {sorted(unknown)}, expected {sorted(allowed)}",
    )


def parse_config(report: str, config: object, queries_dir: Path) -> ExportConfig:
    """Validates the content of export_data.yml (see decisions/rapports/decisions.md)."""
    where = f"{report}/{CONFIG_FILE}"
    _require(
        isinstance(config, dict), f"{where}: expected a mapping with the key outputs"
    )
    _check_keys(config, {"outputs"}, where)
    outputs_config = config.get("outputs")
    _require(
        isinstance(outputs_config, list) and outputs_config,
        f"{where}: outputs must be a non-empty list",
    )

    outputs = []
    for index, output in enumerate(outputs_config, start=1):
        output_where = f"{where}, output {index}"
        _require(isinstance(output, dict), f"{output_where}: expected a mapping")
        output_type = output.get("type")
        _require(
            output_type in OUTPUT_TYPES,
            f"{output_where}: type must be one of {sorted(OUTPUT_TYPES)}",
        )
        _check_keys(output, {"type", "spreadsheet", "tabs"}, output_where)

        spreadsheet = output.get("spreadsheet")
        _require(
            isinstance(spreadsheet, str)
            and spreadsheet.strip() == spreadsheet
            and spreadsheet,
            f"{output_where}: spreadsheet must be the exact name of the Google Sheet, without spaces around it",
        )
        tabs_config = output.get("tabs")
        _require(
            isinstance(tabs_config, list) and tabs_config,
            f"{output_where}: tabs must be a non-empty list",
        )

        tabs = []
        for tab_config in tabs_config:
            _require(
                isinstance(tab_config, dict),
                f"{output_where}: each tab must be a mapping with tab and query",
            )
            _check_keys(tab_config, {"tab", "query", "columns"}, f"{output_where}, tab")
            tab, query = tab_config.get("tab"), tab_config.get("query")
            _require(
                isinstance(tab, str)
                and tab.strip() == tab
                and 0 < len(tab) <= MAX_TAB_NAME_LENGTH,
                f"{output_where}: invalid tab name {tab!r}",
            )
            _require(
                not tab.startswith("_"),
                f"{output_where}: tab {tab!r}: names starting with _ are for humans and for {EXPORT_TAB}",
            )
            _require(
                isinstance(query, str) and QUERY_FILE_PATTERN.match(query),
                f"{output_where}, tab {tab!r}: query must be the name of a .sql file of {QUERIES_DIR}/",
            )
            _require(
                (queries_dir / query).is_file(),
                f"{output_where}, tab {tab!r}: {QUERIES_DIR}/{query} not found",
            )
            columns = parse_columns(
                tab_config.get("columns"), f"{output_where}, tab {tab!r}"
            )
            tabs.append(TabExport(tab=tab, query=query, columns=columns))

        names = [t.tab for t in tabs]
        _require(
            len(set(names)) == len(names), f"{output_where}: tab names must be unique"
        )
        outputs.append(SheetOutput(spreadsheet=spreadsheet, tabs=tuple(tabs)))

    spreadsheets = [o.spreadsheet for o in outputs]
    _require(
        len(set(spreadsheets)) == len(spreadsheets),
        f"{where}: one output per spreadsheet (group its tabs in one output)",
    )
    return ExportConfig(report=report, queries_dir=queries_dir, outputs=tuple(outputs))


def load_config(report: str, reports_dir: Path = REPORTS_DIR) -> ExportConfig:
    report = check_report_id(report)
    config_path = reports_dir / report / CONFIG_FILE
    if not config_path.is_file():
        raise ExportError(
            f"no {CONFIG_FILE} for the report {report}; reports with one: {available_reports(reports_dir)}"
        )
    try:
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ExportError(
            f"{report}/{CONFIG_FILE} is not valid YAML ({type(e).__name__})"
        ) from None
    return parse_config(report, config, reports_dir / report / QUERIES_DIR)


# ------------------------------------------------------------------ queries


def single_statement(sql: str) -> str:
    """The statement of a query file, without its final semicolons. Refuses a file with several statements
    (a semicolon outside strings, quoted identifiers and comments, followed by something else than comments).
    """
    i, n = 0, len(sql)
    end = None  # position of the first top-level semicolon
    has_code = False  # something else than comments and spaces before it
    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = n if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if sql.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif sql.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
            continue
        if c in ("'", '"'):
            has_code = True
            i += 1
            while i < n:
                if sql[i] == c:
                    if i + 1 < n and sql[i + 1] == c:  # doubled quote
                        i += 2
                        continue
                    break
                i += 1
            i += 1
            continue
        if c == "$":
            tag = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if tag:
                has_code = True
                closing = sql.find(tag.group(0), i + len(tag.group(0)))
                i = n if closing < 0 else closing + len(tag.group(0))
                continue
        if c == ";":
            if end is None:
                end = i
        elif not c.isspace():
            if end is not None:
                raise ExportError(
                    "the query file holds several statements, expected a single SELECT"
                )
            has_code = True
        i += 1
    statement = sql if end is None else sql[:end]
    if not has_code:
        raise ExportError("the query file is empty")
    return statement


def connect():
    """Connection to the barometre database, every transaction read-only."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "barometre"),
        user=os.environ.get("POSTGRES_USER", "user"),
        password=os.environ.get("POSTGRES_PASSWORD", "password"),
        application_name="export_report_data",
        options=f"-c default_transaction_read_only=on -c statement_timeout={STATEMENT_TIMEOUT_MS}",
    )


def run_query(connection, sql: str) -> QueryResult:
    """Runs one SELECT in a read-only transaction, through a server-side cursor, and rolls back."""
    statement = single_statement(sql)
    connection.set_session(readonly=True, autocommit=False)
    try:
        with connection.cursor(name="report_export") as cursor:
            cursor.itersize = FETCH_SIZE
            cursor.execute(statement)
            rows = []
            while True:
                batch = cursor.fetchmany(FETCH_SIZE)
                rows.extend(batch)
                if not batch:
                    break
            columns = [d[0] for d in cursor.description or []]
    finally:
        connection.rollback()
    if not columns:
        raise ExportError("the query returns no column")
    return QueryResult(columns=columns, rows=rows)


# ------------------------------------------------------------------ Google Sheets


def to_cell(value) -> dict:
    """CellData of a value: numbers stay numbers (charts), text is never read as a formula, NULL is an empty cell."""
    if value is None:
        return {}
    if isinstance(value, bool):
        return {"userEnteredValue": {"boolValue": value}}
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return {}
        return {"userEnteredValue": {"numberValue": number}}
    if isinstance(value, (datetime, date, time)):
        return {"userEnteredValue": {"stringValue": value.isoformat()}}
    return {"userEnteredValue": {"stringValue": str(value)}}


def _rows(values: list[list]) -> list[dict]:
    return [{"values": [to_cell(v) for v in row]} for row in values]


def tab_values(result: QueryResult) -> list[list]:
    return [list(result.columns)] + [list(row) for row in result.rows]


def check_size(values: list[list]) -> None:
    cells = sum(len(row) for row in values)
    if cells > MAX_CELLS_PER_TAB:
        raise ExportError(
            f"{cells} cells, more than the limit of {MAX_CELLS_PER_TAB} per tab"
        )


def export_tab_values(
    report: str, exported_at: datetime, version: str, counts: list[tuple[str, str, int]]
) -> list[list]:
    """Content of the tab _export: date of the export, report, version of the pipeline, rows of each tab."""
    return [
        ["Dernier export (heure de Paris)", exported_at.strftime("%Y-%m-%d %H:%M:%S")],
        ["Rapport", report],
        ["Version du pipeline", version],
        ["", ""],
        [
            "Les onglets ci-dessous sont remplacés à chaque export : ne rien y écrire à la main.",
            "",
        ],
        ["Onglet", "Requête", "Lignes"],
        *[[tab, f"{QUERIES_DIR}/{query}", rows] for tab, query, rows in counts],
    ]


def build_requests(existing: list[dict], contents: dict[str, TabContent]) -> list[dict]:
    """spreadsheets.batchUpdate requests replacing the content of each tab: a missing tab is added, a tab too
    small is enlarged (never shrunk: charts may point beyond), then its values are cleared, in every row of
    its columns (all of them when columns is None), and the new ones written from row 1. Formats, charts and
    the values of the other columns are kept."""
    by_title = {p["title"]: p for p in existing}
    used_ids = {p["sheetId"] for p in existing}
    requests_ = []
    for tab, content in contents.items():
        values = content.values
        start_column, end_column = content.columns or (0, None)
        n_rows = len(values)
        n_columns = max(
            start_column + max((len(row) for row in values), default=1),
            end_column or 0,
        )
        properties = by_title.get(tab)
        if properties is None:
            sheet_id = random.randint(1, 2**31 - 1)
            while sheet_id in used_ids:
                sheet_id = random.randint(1, 2**31 - 1)
            used_ids.add(sheet_id)
            requests_.append(
                {
                    "addSheet": {
                        "properties": {
                            "sheetId": sheet_id,
                            "title": tab,
                            "gridProperties": {
                                "rowCount": max(n_rows, 1000),
                                "columnCount": max(n_columns, 26),
                            },
                        }
                    }
                }
            )
        else:
            if properties.get("sheetType", "GRID") != "GRID":
                raise ExportError(
                    f"tab {tab!r} exists but is not a grid (chart sheet?)"
                )
            sheet_id = properties["sheetId"]
            grid = properties.get("gridProperties", {})
            row_count, column_count = grid.get("rowCount", 0), grid.get(
                "columnCount", 0
            )
            if n_rows > row_count or n_columns > column_count:
                requests_.append(
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_id,
                                "gridProperties": {
                                    "rowCount": max(n_rows, row_count),
                                    "columnCount": max(n_columns, column_count),
                                },
                            },
                            "fields": "gridProperties.rowCount,gridProperties.columnCount",
                        }
                    }
                )
            requests_.append(
                {
                    "updateCells": {
                        "range": (
                            {"sheetId": sheet_id}
                            if content.columns is None
                            else {
                                "sheetId": sheet_id,
                                "startColumnIndex": start_column,
                                "endColumnIndex": end_column,
                            }
                        ),
                        "fields": "userEnteredValue",
                    }
                }
            )
        requests_.append(
            {
                "updateCells": {
                    "start": {
                        "sheetId": sheet_id,
                        "rowIndex": 0,
                        "columnIndex": start_column,
                    },
                    "rows": _rows(values),
                    "fields": "userEnteredValue",
                }
            }
        )
    return requests_


class ReportSheet:
    """A Google Sheet of the export folder, opened with the editor service account."""

    def __init__(self, session, spreadsheet_id: str):
        self.session = session
        self.spreadsheet_id = spreadsheet_id

    @classmethod
    def open(
        cls,
        session,
        folder_id: str,
        spreadsheet_name: str,
        check_public=ensure_not_public,
    ) -> "ReportSheet":
        spreadsheet = find_spreadsheet(session, folder_id, spreadsheet_name)
        check_public(spreadsheet)
        return cls(session, spreadsheet["id"])

    def tab_properties(self) -> list[dict]:
        response = self.session.get(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}",
            params={
                "fields": "sheets.properties(sheetId,title,sheetType,gridProperties)"
            },
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()
        return [s["properties"] for s in response.json().get("sheets", [])]

    def replace_tabs(self, contents: dict[str, TabContent]) -> None:
        """Replaces the content of the tabs in one batchUpdate: Google applies all the requests or none."""
        response = self.session.post(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}:batchUpdate",
            json={"requests": build_requests(self.tab_properties(), contents)},
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()


# ------------------------------------------------------------------ export


def safe_error(error: Exception) -> str:
    """Description of an error without its message when it may quote data."""
    if isinstance(error, (ExportError, GoogleSheetsError)):
        return str(error)
    if isinstance(error, psycopg2.Error):
        return f"database error {type(error).__name__} (SQLSTATE {error.pgcode or 'unknown'})"
    if isinstance(error, requests.HTTPError) and error.response is not None:
        return f"Google API error HTTP {error.response.status_code} {error.response.reason}"
    return f"{type(error).__name__}"


def pipeline_version(pyproject: Path = Path("pyproject.toml")) -> str:
    try:
        return tomllib.loads(pyproject.read_text())["tool"]["poetry"]["version"]
    except Exception:  # noqa: BLE001 - the version is only informative
        return "inconnue"


def export_output(
    config: ExportConfig,
    output: SheetOutput,
    connection,
    session,
    folder_id: str,
    apply: bool,
    now=lambda: datetime.now(PARIS),
    check_public=ensure_not_public,
) -> None:
    label = f"{config.report} / {output.spreadsheet}"
    sheet = ReportSheet.open(
        session, folder_id, output.spreadsheet, check_public=check_public
    )
    logger.info("%s: spreadsheet found, not public", label)

    results: dict[str, QueryResult] = {}
    contents: dict[str, TabContent] = {}
    counts = []
    for tab in output.tabs:
        if tab.query not in results:
            sql = (config.queries_dir / tab.query).read_text(encoding="utf-8")
            try:
                results[tab.query] = run_query(connection, sql)
            except Exception as e:  # noqa: BLE001 - reduced to a message without data
                raise ExportError(f"query {tab.query}: {safe_error(e)}") from None
        result = results[tab.query]
        values = tab_values(result)
        try:
            check_size(values)
            if tab.columns and len(result.columns) > tab.columns[1] - tab.columns[0]:
                raise ExportError(
                    f"{len(result.columns)} columns, more than the "
                    f"{tab.columns[1] - tab.columns[0]} of its range columns"
                )
        except ExportError as e:
            raise ExportError(f"tab {tab.tab!r}: {e}") from None
        contents[tab.tab] = TabContent(values, tab.columns)
        counts.append((tab.tab, tab.query, len(result.rows)))
        logger.info(
            "%s: query %s -> %s rows, %s columns for the tab %r",
            label,
            tab.query,
            len(result.rows),
            len(result.columns),
            tab.tab,
        )
    contents[EXPORT_TAB] = TabContent(
        export_tab_values(config.report, now(), pipeline_version(), counts)
    )

    if not apply:
        existing = {p["title"] for p in sheet.tab_properties()}
        for tab in contents:
            logger.info(
                "%s: check only, tab %r would be %s",
                label,
                tab,
                "replaced" if tab in existing else "created",
            )
        return
    sheet.replace_tabs(contents)
    logger.info("%s: tabs %s replaced", label, list(contents))


def reports_folder_id(session) -> str:
    """Id of the folder "Rapports" (REPORTS_DRIVE_FOLDER_NAME) of the shared folder EXTERNAL_SOURCES_DRIVE_FOLDER."""
    parent_id = parse_folder_id(os.environ.get(FOLDER_ENV, ""))
    name = os.environ.get(REPORTS_FOLDER_NAME_ENV, "").strip() or REPORTS_FOLDER_NAME
    return find_folder(session, parent_id, name)["id"]


def export_report(
    report: str,
    apply: bool = True,
    reports_dir: Path = REPORTS_DIR,
    connect_db=connect,
    open_session=None,
    folder_id: str | None = None,
    check_public=ensure_not_public,
) -> bool:
    """Exports every output of the report. Returns True when all of them were exported."""
    config = load_config(report, reports_dir)
    session = (
        open_session()
        if open_session
        else service_account_session(CREDENTIALS_ENV, SCOPES)
    )
    if folder_id is None:
        folder_id = reports_folder_id(session)
    logger.info(
        "Report %s: %s output(s), %s",
        report,
        len(config.outputs),
        "apply" if apply else "check only",
    )

    succeeded = True
    connection = connect_db()
    try:
        for output in config.outputs:
            try:
                export_output(
                    config,
                    output,
                    connection,
                    session,
                    folder_id,
                    apply,
                    check_public=check_public,
                )
            except Exception as e:  # noqa: BLE001 - logged without data
                # nothing written for this output: its tabs are written in a single request, after all its queries
                logger.error(
                    "%s / %s: %s, nothing written",
                    report,
                    output.spreadsheet,
                    safe_error(e),
                )
                succeeded = False
    finally:
        connection.close()
    return succeeded


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    for noisy in ("urllib3", "google", "requests"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(
        description="Export the data of a report to Google Sheets."
    )
    parser.add_argument(
        "--report",
        default=os.environ.get("REPORT_ID", ""),
        help="folder of the report in decisions/rapports/ (default: env REPORT_ID)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="run the queries and check the spreadsheets, without writing anything",
    )
    args = parser.parse_args(argv)
    try:
        succeeded = export_report(args.report, apply=not args.check)
    except Exception as e:  # noqa: BLE001 - logged without data instead of a traceback
        logger.error("Report export failed: %s", safe_error(e))
        return 1
    return 0 if succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
