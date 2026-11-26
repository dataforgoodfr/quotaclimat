"""Shared helpers to reach the private Google Sheets of the pipeline with a service account.

The spreadsheets are never named by id in the repository (it is public): they are looked up by exact name in a
Google Drive folder whose id is kept in an env variable (find_spreadsheet), and a spreadsheet readable without
credentials is refused (ensure_not_public). Used by the dbt reference data import
(quotaclimat/data_ingestion/external_sources), the advertising jobs that write to their Sheets and the export of
the report data (quotaclimat/reports/export_report_data.py).
"""

import json
import os
import re
from urllib.parse import urlparse

import requests
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

SHEETS_API_URL = "https://sheets.googleapis.com/v4/spreadsheets"
DRIVE_FILES_API_URL = "https://www.googleapis.com/drive/v3/files"
SPREADSHEET_MIME_TYPE = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
# Drive permission ids of the "anyone" permissions (public on the web / anyone with the link)
PUBLIC_PERMISSION_IDS = {"anyone", "anyoneWithLink"}
ANONYMOUS_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
)
GOOGLE_LOGIN_HOSTS = {"accounts.google.com"}
TIMEOUT_SEC = 60
DRIVE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{10,}$")


class GoogleSheetsError(Exception):
    pass


def parse_folder_id(value: str) -> str:
    """Drive folder id: the last part of https://drive.google.com/drive/folders/<id>."""
    value = value.strip()
    if not DRIVE_ID_PATTERN.match(value):
        raise GoogleSheetsError("not a Google Drive folder id")
    return value


def drive_query_literal(value: str) -> str:
    """Quotes a value for a Drive API search query."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def quote_tab(tab: str) -> str:
    """Tab name in A1 notation: 'Tab name', with ' doubled."""
    return "'" + tab.replace("'", "''") + "'"


def service_account_session(
    credentials_env: str, scopes: list[str]
) -> AuthorizedSession:
    """Session authorized with the JSON key of the service account held by the env variable credentials_env."""
    credentials_json = os.environ.get(credentials_env, "").strip()
    if not credentials_json:
        raise GoogleSheetsError(
            f"no Google service account credentials (env {credentials_env} not set)"
        )
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(credentials_json), scopes=scopes
    )
    return AuthorizedSession(credentials)


def _find_in_folder(
    session: AuthorizedSession, folder_id: str, name: str, mime_type: str, kind: str
) -> dict:
    """Drive file (id, name, permissionIds) named exactly name, of this mime type, directly in the folder
    (not in sub-folders). Exactly one match expected."""
    response = session.get(
        DRIVE_FILES_API_URL,
        params={
            "q": (
                f"{drive_query_literal(folder_id)} in parents"
                f" and name = {drive_query_literal(name)}"
                f" and mimeType = '{mime_type}' and trashed = false"
            ),
            "fields": "files(id,name,permissionIds)",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        },
        timeout=TIMEOUT_SEC,
    )
    response.raise_for_status()
    files = response.json().get("files", [])
    if len(files) != 1:
        raise GoogleSheetsError(
            f"{len(files)} {kind} named {name!r} in the folder, expected exactly 1"
        )
    return files[0]


def find_spreadsheet(
    session: AuthorizedSession, folder_id: str, spreadsheet_name: str
) -> dict:
    """Drive file (id, name, permissionIds) of the spreadsheet named exactly spreadsheet_name in the
    folder (not in sub-folders)."""
    return _find_in_folder(
        session, folder_id, spreadsheet_name, SPREADSHEET_MIME_TYPE, "spreadsheets"
    )


def find_folder(session: AuthorizedSession, folder_id: str, folder_name: str) -> dict:
    """Drive folder named exactly folder_name directly in the folder folder_id."""
    return _find_in_folder(session, folder_id, folder_name, FOLDER_MIME_TYPE, "folders")


def ensure_not_public(spreadsheet: dict, anonymous_get=None) -> None:
    """Refuses a spreadsheet readable by anyone, checked in two independent ways. Fails closed: when
    the anonymous check cannot conclude, the spreadsheet is refused too."""
    public_permissions = PUBLIC_PERMISSION_IDS & set(
        spreadsheet.get("permissionIds") or []
    )
    if public_permissions:
        raise GoogleSheetsError(
            f"spreadsheet is shared publicly ({sorted(public_permissions)}), refused"
        )

    # try to read it without any credentials, like anyone who got the link
    response = (anonymous_get or requests.get)(
        ANONYMOUS_EXPORT_URL.format(spreadsheet_id=spreadsheet["id"]),
        allow_redirects=False,
        stream=True,  # never download the content
        timeout=TIMEOUT_SEC,
    )
    try:
        status = response.status_code
        location_host = urlparse(response.headers.get("Location", "")).hostname
        content_type = response.headers.get("Content-Type", "")
    finally:
        response.close()
    if status in (401, 403, 404) or (
        300 <= status < 400 and location_host in GOOGLE_LOGIN_HOSTS
    ):
        return  # login required: private
    if (status == 200 and "text/html" not in content_type) or 300 <= status < 400:
        raise GoogleSheetsError("spreadsheet is readable without credentials, refused")
    raise GoogleSheetsError(
        f"could not check that the spreadsheet is private (HTTP {status}), refused"
    )
