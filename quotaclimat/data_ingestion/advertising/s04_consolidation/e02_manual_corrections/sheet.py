"""Read the corrections spreadsheet and write the columns filled by the job, with the Google Sheets editor
service account (generic, shared with other jobs), shared as editor on this spreadsheet only.

Only the columns of the job (COLUMNS_FILLED_BY_THE_JOB in run.py) are written, never the columns of the
humans. The tabs whose name starts with "_" are for humans and never read: a tab whose rows are all processed is
renamed with a "_" in front (archive), so that the next runs do not read it again.
"""

import json
import logging
import os
from dataclasses import dataclass

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from quotaclimat.data_ingestion.external_sources.download_external_sources import (
    HUMAN_TAB_PREFIX,
    SHEETS_API_URL,
    ensure_not_public,
    find_spreadsheet,
    is_human_tab,
    parse_folder_id,
)

CREDENTIALS_ENV = "GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON"
FOLDER_ENV = "EXTERNAL_SOURCES_DRIVE_FOLDER"
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",  # find the spreadsheet in the folder
    "https://www.googleapis.com/auth/spreadsheets",  # read it and write the columns of the job
]
TIMEOUT_SEC = 60


@dataclass
class SheetRow:
    # number of the row in the tab (1 = header), to write it back
    number: int
    values: dict[str, str]


def raise_for_status(response) -> None:
    """raise_for_status with the body of the response, which says why Google refused the request."""
    if response.status_code >= 400:
        raise RuntimeError(
            f"{response.status_code} {response.reason} for {response.url}: {response.text[:2000]}"
        )


def _quote(tab: str) -> str:
    return "'" + tab.replace("'", "''") + "'"


def column_letter(index: int) -> str:
    """A1 letter of a 0-based column index: 0 -> A, 25 -> Z, 26 -> AA."""
    letters = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def parse_rows(values: list[list]) -> tuple[list[str], list[SheetRow]]:
    """Header and rows of a tab (values of the Sheets API), empty rows left out."""
    if not values:
        return [], []
    header = [str(h).strip() for h in values[0]]
    rows = []
    for offset, row in enumerate(values[1:]):
        cells = {
            h: (str(row[i]).strip() if i < len(row) else "")
            for i, h in enumerate(header)
            if h
        }
        if any(cells.values()):
            rows.append(SheetRow(number=offset + 2, values=cells))
    return header, rows


class CorrectionsSheet:
    def __init__(self, session: AuthorizedSession, spreadsheet_id: str):
        self.session = session
        self.spreadsheet_id = spreadsheet_id

    @classmethod
    def open(cls, spreadsheet_name: str) -> "CorrectionsSheet":
        credentials_json = os.environ.get(CREDENTIALS_ENV, "").strip()
        if not credentials_json:
            raise RuntimeError(
                f"no Google service account credentials (env {CREDENTIALS_ENV} not set)"
            )
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(credentials_json), scopes=SCOPES
        )
        session = AuthorizedSession(credentials)
        spreadsheet = find_spreadsheet(
            session, parse_folder_id(os.environ.get(FOLDER_ENV, "")), spreadsheet_name
        )
        ensure_not_public(spreadsheet)
        return cls(session, spreadsheet["id"])

    def _properties(self) -> list[dict]:
        """Properties of every tab (sheetId, title, sheetType, gridProperties)."""
        response = self.session.get(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}",
            params={
                "fields": "sheets.properties(sheetId,title,sheetType,gridProperties)"
            },
            timeout=TIMEOUT_SEC,
        )
        raise_for_status(response)
        return [s["properties"] for s in response.json().get("sheets", [])]

    def _tab_properties(self, tab: str) -> dict:
        return next(p for p in self._properties() if p["title"] == tab)

    def _update(self, requests: list[dict]) -> None:
        """spreadsheets.batchUpdate: changes of the structure of the spreadsheet (not of the values)."""
        response = self.session.post(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}:batchUpdate",
            json={"requests": requests},
            timeout=TIMEOUT_SEC,
        )
        raise_for_status(response)

    def tabs(self) -> list[str]:
        """Names of the grid tabs, without the tabs for humans (starting with "_")."""
        return [
            p["title"]
            for p in self._properties()
            if p.get("sheetType", "GRID") == "GRID" and not is_human_tab(p["title"])
        ]

    def _titles(self) -> set[str]:
        return {p["title"] for p in self._properties()}

    def _rename(self, tab: str, new_title: str) -> None:
        self._update(
            [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": self._tab_properties(tab)["sheetId"],
                            "title": new_title,
                        },
                        "fields": "title",
                    }
                }
            ]
        )

    def archive(self, tab: str) -> str | None:
        """Rename the tab with a "_" in front: the next runs skip it (a human removes the "_" to process it
        again). Returns the new name, None when a tab already has this name (the tab is left as it is)."""
        new_title = HUMAN_TAB_PREFIX + tab
        if new_title in self._titles():
            logging.warning(
                "Tab %s not archived: a tab %s already exists", tab, new_title
            )
            return None
        self._rename(tab, new_title)
        return new_title

    def _ensure_grid_size(self, tab: str, rows: int, columns: int) -> None:
        """Add rows or columns at the end of the tab when it is smaller than rows x columns: the values API
        refuses to write outside the grid (400 "exceeds grid limits"), e.g. in the column added for the job in
        a tab imported from a CSV, whose grid has only the columns of the file."""
        properties = self._tab_properties(tab)
        grid = properties.get("gridProperties", {})
        requests = []
        for dimension, needed, current in (
            ("ROWS", rows, grid.get("rowCount", 0)),
            ("COLUMNS", columns, grid.get("columnCount", 0)),
        ):
            if needed > current:
                requests.append(
                    {
                        "appendDimension": {
                            "sheetId": properties["sheetId"],
                            "dimension": dimension,
                            "length": needed - current,
                        }
                    }
                )
        if requests:
            self._update(requests)

    def read(self, tab: str) -> tuple[list[str], list[SheetRow]]:
        response = self.session.get(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}/values/{_quote(tab)}",
            params={"valueRenderOption": "FORMATTED_VALUE"},
            timeout=TIMEOUT_SEC,
        )
        raise_for_status(response)
        return parse_rows(response.json().get("values", []))

    def _write_cells(self, tab: str, cells: list[tuple[int, int, str]]) -> None:
        """cells: (row number, 0-based column index, value), in one request."""
        if not cells:
            return
        self._ensure_grid_size(
            tab,
            max(row for row, _, _ in cells),
            max(column for _, column, _ in cells) + 1,
        )
        response = self.session.post(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}/values:batchUpdate",
            json={
                "valueInputOption": "RAW",
                "data": [
                    {
                        "range": f"{_quote(tab)}!{column_letter(column)}{row}",
                        "values": [[value]],
                    }
                    for row, column, value in cells
                ],
            },
            timeout=TIMEOUT_SEC,
        )
        raise_for_status(response)

    def write_rows(
        self,
        tab: str,
        key_column: str,
        updates: list[tuple[SheetRow, dict[str, str]]],
    ) -> int:
        """Write the values of the job in the rows of the tab, adding the missing columns after the last one.

        The tab is read again first: a row moved by a human since the first read (sorted, inserted above) is
        found again by its key (first row with this key whose columns of the job are empty). A row not found
        is logged and left: it is processed again by the next run. Returns the number of rows written.
        """
        if not updates:
            return 0
        header, rows = self.read(tab)
        columns = list(header)
        new_columns = []
        for _, values in updates:
            for column in values:
                if column not in columns:
                    columns.append(column)
                    new_columns.append(column)
        cells = [
            (1, columns.index(column), column) for column in new_columns
        ]  # header of the new columns

        rows_by_number = {row.number: row for row in rows}
        taken = set()
        written = 0
        for row, values in updates:
            key = row.values.get(key_column, "")
            current = rows_by_number.get(row.number)
            if current is None or current.values.get(key_column, "") != key:
                current = next(
                    (
                        r
                        for r in rows
                        if r.values.get(key_column, "") == key
                        and r.number not in taken
                        and not any(r.values.get(c) for c in values)
                    ),
                    None,
                )
                if current is None:
                    logging.error(
                        "Tab %s: row %s (%s=%s) not found any more, not written",
                        tab,
                        row.number,
                        key_column,
                        key,
                    )
                    continue
            taken.add(current.number)
            cells.extend(
                (current.number, columns.index(column), value)
                for column, value in values.items()
            )
            written += 1
        self._write_cells(tab, cells)
        return written
