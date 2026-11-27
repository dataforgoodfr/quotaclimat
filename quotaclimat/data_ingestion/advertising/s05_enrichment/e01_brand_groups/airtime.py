"""Air time of each brand of the tab Marques of the Inventaire_des_marques Google Sheet, written to its column
temps_antenne_min, to review the brands by decreasing air time.

Air time: sum of analytics.publicites.duration_sec of the occurrences whose predicted brand has the same
name_key as the brand of the row (as dbt matches them in ad_brands), broadcast between two days of Paris,
both included, in minutes with one decimal. A brand not broadcast in the period gets 0. The cells are
overwritten on every run (a value computed by the job, never typed by a human), only when they change, a row
only when it still has the same brand when written.

Run with `run.py --airtime-only --airtime-start 2026-01-01 --airtime-end 2026-12-31` (a Scaleway cron can
change the command, not the env).
"""

import logging
from datetime import date

from sqlalchemy import text

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import name_key
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.sheet import (
    BRANDS_TAB,
    BrandInventorySheet,
)

AIRTIME_COLUMN = "temps_antenne_min"
# occurrence_date is stored in UTC, without time zone
AIRTIME_QUERY = text("""
    SELECT predicted_brand, SUM(duration_sec) AS duration_sec
    FROM analytics.publicites
    WHERE predicted_brand IS NOT NULL
      AND ((occurrence_date AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::date BETWEEN :start AND :end
    GROUP BY predicted_brand
""")


def airtime_by_brand(rows) -> dict[str, float]:
    """{name_key of the brand: seconds} from (predicted_brand, duration_sec) rows: the spellings of a brand
    are added together."""
    seconds: dict[str, float] = {}
    for brand, duration in rows:
        key = name_key(brand)
        if key:
            seconds[key] = seconds.get(key, 0.0) + float(duration or 0)
    return seconds


def minutes(seconds: float) -> float | int:
    value = round(seconds / 60, 1)
    return int(value) if value == int(value) else value


def same_number(cell: str, value: float) -> bool:
    """Whether the cell, as displayed (French or English decimal separator), already holds the value."""
    try:
        return float(cell.replace(" ", "").replace("\xa0", "").replace(" ", "").replace(",", ".")) == value
    except ValueError:
        return False


def airtime_updates(inventory: list[dict[str, str]], seconds: dict[str, float]) -> list[tuple[int, str, dict]]:
    """(index of the row, its brand, {temps_antenne_min: (value read, new value)}) for the rows with a brand
    whose air time changed."""
    updates = []
    for index, row in enumerate(inventory):
        brand = row.get("marque", "")
        key = name_key(brand)
        if not key:
            continue
        current = row.get(AIRTIME_COLUMN, "")
        value = minutes(seconds.get(key, 0.0))
        if not same_number(current, value):
            updates.append((index, brand, {AIRTIME_COLUMN: (current, value)}))
    return updates


def update_airtime(sheet: BrandInventorySheet, connection, start: date, end: date, dry_run: bool = False) -> int:
    """Writes the air time of every brand of the tab Marques between start and end (days of Paris, both
    included). Returns the number of cells written."""
    if start > end:
        raise ValueError(f"air time period: start {start} after end {end}")
    if AIRTIME_COLUMN not in sheet.header(BRANDS_TAB):
        raise RuntimeError(f"tab {BRANDS_TAB} has no column {AIRTIME_COLUMN}: add it before running the job")
    seconds = airtime_by_brand(connection.execute(AIRTIME_QUERY, {"start": start, "end": end}).all())
    inventory = sheet.read(BRANDS_TAB)
    updates = airtime_updates(inventory, seconds)
    logging.info(
        "Air time from %s to %s: %s brands broadcast, %s rows of the tab %s to update",
        start, end, len(seconds), len(updates), BRANDS_TAB,
    )
    if dry_run:
        print("----- BRAND AIR TIME (line, brand, (before, after) in minutes) -----")
        for index, brand, cells in updates:
            print(f"{index + 2}\t{brand}\t{cells[AIRTIME_COLUMN]}")
        print("----- END OF BRAND AIR TIME -----", flush=True)
        return 0
    written = sheet.replace_cells(BRANDS_TAB, updates, action="updated (air time)")
    logging.info("Air time: %s cells written to the tab %s", written, BRANDS_TAB)
    return written
