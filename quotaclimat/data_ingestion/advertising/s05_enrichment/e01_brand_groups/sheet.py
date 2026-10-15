"""Read and append rows to the Inventaires_des_marques Google Sheet, with the Google Sheets editor service
account (generic, shared with other jobs, not the read-only account of the dbt import), shared as editor
on this spreadsheet only.

Rows are only ever appended, never modified: a human correction is never overwritten.
"""

import json
import os

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from quotaclimat.data_ingestion.external_sources.download_external_sources import (
    SHEETS_API_URL, ensure_not_public, find_spreadsheet, parse_folder_id)

CREDENTIALS_ENV = "GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON"
FOLDER_ENV = "EXTERNAL_SOURCES_DRIVE_FOLDER"
SPREADSHEET_NAME = "Inventaires_des_marques"
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

    def read(self, tab: str) -> list[dict[str, str]]:
        """Rows of a tab as {header: value}, an empty list when the tab does not exist."""
        response = self.session.get(
            f"{SHEETS_API_URL}/{self.spreadsheet_id}/values/{_quote(tab)}",
            params={"valueRenderOption": "FORMATTED_VALUE"},
            timeout=TIMEOUT_SEC,
        )
        if response.status_code == 400:  # unknown tab
            return []
        response.raise_for_status()
        rows = response.json().get("values", [])
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
