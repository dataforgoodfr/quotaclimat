"""Reference data kept in Google Sheets (taxonomy, brand dictionary), snapshotted locally.

``save_snapshot`` writes fetched tabs to ``<versions_dir>/<timestamp>_<hash8>/`` only when
their content changed, and records that snapshot as the latest in ``manifest.json``.
Stages read the ``latest`` snapshot and never touch the network, so they work against a
fixed version until the next sync. Snapshots are not versioned in git.
"""

import hashlib
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import gspread
import pandas as pd

from quotaclimat.data_ingestion.advertising.s03_classification import ROOT

logger = logging.getLogger(__name__)

MANIFEST = "manifest.json"
SPREADSHEET_ID = os.environ.get("ADVERTISING_SPREADSHEET_ID")
CREDENTIALS_ENV = "ADVERTISING_GOOGLE_SERVICE_ACCOUNT_JSON"
# Used when CREDENTIALS_ENV is not set: the editor account of s05_enrichment.
FALLBACK_CREDENTIALS_ENV = "GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON"


def credentials() -> dict:
    """The service account key: its JSON inline (as Kestra passes it), or the path of its
    file, relative to the project root (``secrets/<key>.json``)."""
    value = (
        os.environ.get(CREDENTIALS_ENV) or os.environ[FALLBACK_CREDENTIALS_ENV]
    ).strip()
    if value.startswith("{"):
        return json.loads(value)
    return json.loads((ROOT / Path(value).expanduser()).read_text())


def open_spreadsheet() -> gspread.Spreadsheet:
    client = gspread.service_account_from_dict(credentials())
    return client.open_by_key(SPREADSHEET_ID)


def fetch(sheet_names: tuple[str, ...]) -> dict[str, pd.DataFrame]:
    spreadsheet = open_spreadsheet()
    return {
        name: pd.DataFrame(spreadsheet.worksheet(name).get_all_records())
        for name in sheet_names
    }


def read_xlsx(path: Path, sheet_names: tuple[str, ...]) -> dict[str, pd.DataFrame]:
    return pd.read_excel(path, sheet_name=list(sheet_names))


def content_hash(tabs: dict[str, pd.DataFrame]) -> str:
    parts = []
    for df in tabs.values():
        df = df.fillna("").astype(str)
        if not df.empty:
            df = df.sort_values(by=list(df.columns))
        parts.append(df.to_csv(index=False))
    return hashlib.sha256("".join(parts).encode()).hexdigest()


def _manifest(versions_dir: Path) -> dict:
    path = versions_dir / MANIFEST
    if not path.exists():
        return {"latest": None, "versions": {}}
    return json.loads(path.read_text())


def save_snapshot(
    tabs: dict[str, pd.DataFrame],
    versions_dir: Path,
    filename: str,
) -> str:
    manifest = _manifest(versions_dir)
    digest = content_hash(tabs)
    version_id = next(
        (
            v
            for v, record in manifest["versions"].items()
            if record["content_hash"] == digest
            and (versions_dir / v / filename).exists()
        ),
        None,
    )
    if version_id:
        logger.info("%s unchanged, using snapshot %s", filename, version_id)
    else:
        fetched_at = datetime.now(UTC).replace(microsecond=0)
        version_id = f"{fetched_at:%Y%m%dT%H%M%SZ}_{digest[:8]}"
        path = versions_dir / version_id / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path) as writer:
            for name, df in tabs.items():
                df.to_excel(writer, sheet_name=name, index=False)
        manifest["versions"][version_id] = {
            "content_hash": digest,
            "fetched_at": fetched_at.isoformat(),
            "spreadsheet_id": SPREADSHEET_ID,
        }
        logger.info("%s changed, saved snapshot %s", filename, version_id)
    manifest["latest"] = version_id
    (versions_dir / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")
    return version_id


def latest(versions_dir: Path, filename: str) -> str | None:
    """The latest snapshot, or None when there is none on this machine yet."""
    version = _manifest(versions_dir)["latest"]
    return version if version and (versions_dir / version / filename).exists() else None
