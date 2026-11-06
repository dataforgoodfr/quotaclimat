"""Read and append rows to the Inventaire_des_marques Google Sheet, with the Google Sheets editor service
account (generic, shared with other jobs, not the read-only account of the dbt import), shared as editor
on this spreadsheet only.

New rows are appended; in the rows already there, only empty cells are filled (fill_empty_cells): a human
correction is never overwritten.
"""

import logging

import json
import os

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from quotaclimat.data_ingestion.external_sources.download_external_sources import (
    SHEETS_API_URL, ensure_not_public, find_spreadsheet, parse_folder_id)

CREDENTIALS_ENV = "GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON"
FOLDER_ENV = "EXTERNAL_SOURCES_DRIVE_FOLDER"
SPREADSHEET_NAME = "Inventaire_des_marques"
BRANDS_TAB = "Marques"
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",  # find the spreadsheet in the folder
    "https://www.googleapis.com/auth/spreadsheets",  # read it and append rows
]
TIMEOUT_SEC = 60


def _quote(tab: str) -> str:
    return "'" + tab.replace("'", "''") + "'"


class BrandInventorySheet:
    def __init__(self, session: AuthorizedSession, spreadsheet_id: str):
        self.session = session
        self.spreadsheet_id = spreadsheet_id

    @classmethod
    def open(cls) -> "BrandInventorySheet":
        credentials_json = os.environ.get(CREDENTIALS_ENV, "").strip()
        if not credentials_json:
            raise RuntimeError(f"no Google service account credentials (env {CREDENTIALS_ENV} not set)")
        credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json), scopes=SCOPES)
        session = AuthorizedSession(credentials)
        spreadsheet = find_spreadsheet(session, parse_folder_id(os.environ.get(FOLDER_ENV, "")), SPREADSHEET_NAME)
        ensure_not_public(spreadsheet)
        return cls(session, spreadsheet["id"])

    def _values(self, tab: str) -> list[list]:
        """All the rows of a tab, header first, an empty list when the tab does not exist."""
        response = self.session.get(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}/values/{_quote(tab)}",
            params={"valueRenderOption": "FORMATTED_VALUE"},
            timeout=TIMEOUT_SEC,
        )
        if response.status_code == 400:  # unknown tab
            return []
        response.raise_for_status()
        return response.json().get("values", [])

    def read(self, tab: str) -> list[dict[str, str]]:
        """Rows of a tab as {header: value}, an empty list when the tab does not exist. The index of a row in
        the list is its line in the sheet minus 2 (fill_empty_cells)."""
        rows = self._values(tab)
        if not rows:
            return []
        header = [str(h).strip() for h in rows[0]]
        return [
            {h: (str(row[i]).strip() if i < len(row) else "") for i, h in enumerate(header) if h}
            for row in rows[1:]
        ]

    def header(self, tab: str) -> list[str]:
        """Column names of a tab, an empty list when the tab does not exist."""
        response = self.session.get(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}/values/{_quote(tab)}!1:1", timeout=TIMEOUT_SEC
        )
        if response.status_code == 400:  # unknown tab
            return []
        response.raise_for_status()
        rows = response.json().get("values", [])
        return [str(h).strip() for h in rows[0]] if rows else []

    def append(self, tab: str, rows: list[dict[str, str]]) -> None:
        """Appends the rows after the last row of the tab, each value under the column of the same name;
        values of columns missing from the tab are dropped."""
        if not rows:
            return
        header = self.header(tab)
        if not header:
            raise RuntimeError(f"tab {tab} has no header row")
        values = [[row.get(h, "") for h in header] for row in rows]
        response = self.session.post(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}/values/{_quote(tab)}!A1:append",
            params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
            json={"values": values},
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()

    def fill_empty_cells(self, tab: str, updates: list[tuple[int, str, dict[str, str]]]) -> int:
        """Writes the values (index of the row as given by read, its brand, {column: value}), each only when
        the row still has this brand and the cell is still empty when written (the tab is read again just
        before: a human edit during the run wins). Returns the number of cells written."""
        rows = self._values(tab)
        if not rows or not updates:
            return 0
        header = [str(h).strip() for h in rows[0]]
        if "marque" not in header:
            raise RuntimeError(f"tab {tab} has no column marque")

        def cell(row: list, column: int) -> str:
            return str(row[column]).strip() if column < len(row) else ""

        data = []
        for index, brand, cells in updates:
            line = index + 2
            current = rows[index + 1] if index + 1 < len(rows) else []
            if cell(current, header.index("marque")) != brand.strip():
                logging.warning("Tab %s, line %s: no longer the brand %s, not completed", tab, line, brand)
                continue
            for column, value in cells.items():
                if column in header and value and not cell(current, header.index(column)):
                    data.append({
                        "range": f"{_quote(tab)}!{column_letter(header.index(column) + 1)}{line}",
                        "values": [[value]],
                    })
        if data:
            response = self.session.post(
                f"{SHEETS_API_URL}/{self.spreadsheet_id}/values:batchUpdate",
                json={"valueInputOption": "RAW", "data": data},
                timeout=TIMEOUT_SEC,
            )
            response.raise_for_status()
        return len(data)


def column_letter(number: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA."""
    letters = ""
    while number:
        number, rest = divmod(number - 1, 26)
        letters = chr(ord("A") + rest) + letters
    return letters
