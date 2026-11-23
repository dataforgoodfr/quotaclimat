"""Reviewer verdicts on brand merges, kept in the shared Google Sheet.

Each e5_canonicalise_brand run publishes its merge and near-miss audits to two tabs, with a ``verdict``
column where reviewers pick ``merge`` or ``split``. The next run collects those verdicts
into the ``brand_overrides`` tab, which outlives the audits (they are rewritten every
run), and e5_canonicalise_brand applies them: ``merge`` forces two brands together, ``split`` keeps
them apart.
"""

from datetime import UTC, datetime

import gspread
import pandas as pd
from gspread.exceptions import WorksheetNotFound

from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    dictionary,
    sheets,
)
from quotaclimat.data_ingestion.advertising.tools.brand_dictionary import (
    brand_key,
)

OVERRIDES_TAB = "brand_overrides"
AUDIT_TABS = {
    "brand_merges_audit": ("brand_variant", "brand_canonical"),
    "brand_near_misses_audit": ("variant", "near_canonical"),
}
VERDICTS = ("merge", "split")
OVERRIDE_COLUMNS = [
    "key_a",
    "key_b",
    "verdict",
    "display_a",
    "display_b",
    "source",
    "reviewer",
    "decided_at",
    "dict_content_hash",
]


def pair(a: object, b: object) -> frozenset[str] | None:
    keys = frozenset((brand_key(str(a)), brand_key(str(b))))
    return keys if len(keys) == 2 and all(keys) else None


def load_verdicts() -> dict[frozenset[str], str]:
    spreadsheet = sheets.open_spreadsheet()
    overrides = {}
    for row in _records(spreadsheet, OVERRIDES_TAB):
        key_a, key_b = sorted(str(row.get(k, "")).strip() for k in ("key_a", "key_b"))
        if key_a:
            overrides[frozenset((key_a, key_b))] = {
                **{column: row.get(column, "") for column in OVERRIDE_COLUMNS},
                "key_a": key_a,
                "key_b": key_b,
                "verdict": str(row.get("verdict", "")).strip().lower(),
            }

    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    changed = False
    for tab, columns in AUDIT_TABS.items():
        for row in _records(spreadsheet, tab):
            verdict = str(row.get("verdict", "")).strip().lower()
            shown = [str(row.get(column, "")).strip() for column in columns]
            keys = pair(*shown)
            if verdict not in VERDICTS or keys is None:
                continue
            previous = overrides.get(keys, {})
            if previous.get("verdict") != verdict:
                overrides[keys] = {
                    "key_a": min(keys),
                    "key_b": max(keys),
                    "verdict": verdict,
                    "display_a": shown[0],
                    "display_b": shown[1],
                    "source": tab,
                    "reviewer": previous.get("reviewer", ""),
                    "decided_at": now,
                    "dict_content_hash": dictionary.VERSION,
                }
                changed = True
    if changed:
        table = pd.DataFrame(list(overrides.values()), columns=OVERRIDE_COLUMNS)
        _write(spreadsheet, OVERRIDES_TAB, table.sort_values(["verdict", "key_a"]))

    return {
        keys: record["verdict"]
        for keys, record in overrides.items()
        if record["verdict"] in VERDICTS and len(keys) == 2
    }


def publish_audits(
    merges: pd.DataFrame,
    near_misses: pd.DataFrame,
    verdicts: dict[frozenset[str], str],
) -> None:
    spreadsheet = sheets.open_spreadsheet()
    for (tab, (a, b)), audit in zip(
        AUDIT_TABS.items(), (merges, near_misses), strict=True
    ):
        current = [
            verdicts.get(pair(x, y), "")
            for x, y in zip(audit[a], audit[b], strict=True)
        ]
        _write(spreadsheet, tab, audit.assign(verdict=current)[["verdict", *audit]])


def _records(spreadsheet: gspread.Spreadsheet, tab: str) -> list[dict]:
    try:
        return spreadsheet.worksheet(tab).get_all_records()
    except WorksheetNotFound:
        return []


def _write(spreadsheet: gspread.Spreadsheet, tab: str, table: pd.DataFrame) -> None:
    try:
        sheet = spreadsheet.worksheet(tab)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=tab, rows=100, cols=len(table.columns))
    rows = table.fillna("").values.tolist()
    sheet.clear()
    sheet.resize(rows=len(rows) + 1, cols=len(table.columns))
    sheet.update(range_name="A1", values=[list(table.columns), *rows])
    if not rows:
        return
    column = list(table.columns).index("verdict")
    dropdown = {
        "range": {
            "sheetId": sheet.id,
            "startRowIndex": 1,
            "endRowIndex": len(rows) + 1,
            "startColumnIndex": column,
            "endColumnIndex": column + 1,
        },
        "rule": {
            "condition": {
                "type": "ONE_OF_LIST",
                "values": [{"userEnteredValue": v} for v in VERDICTS],
            },
            "showCustomUi": True,
            "strict": False,
        },
    }
    spreadsheet.batch_update({"requests": [{"setDataValidation": dropdown}]})
