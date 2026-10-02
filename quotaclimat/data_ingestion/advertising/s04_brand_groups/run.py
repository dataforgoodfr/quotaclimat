"""Stage 4: propose the group of the broadcast brands missing from the tab Marques of the
Inventaires_des_marques Google Sheet, and append them to it with statut 'non vérifié'.

For each brand, by decreasing broadcast duration:
1. INPI: French trademarks in force named exactly like the brand, holder (company, SIREN) of the most
   trademarks covering the Nice classes of the brand's sector (column classes_nice of the tab secteurs of
   the Dictionnaire_marques_secteurs Google Sheet, loaded by dbt in advertising.ref_ome_secteurs);
2. parent company of the holder: Wikidata (P749 of the item with this SIREN), else GLEIF (direct parent of
   the LEI registered under this SIREN), else the holder itself;
3. a row is appended to the tab Marques, also when nothing was found (empty group), so that the brand is
   not searched again and a human can fill it in.

Env: POSTGRES_*, INPI_USERNAME, INPI_PASSWORD, GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON,
EXTERNAL_SOURCES_DRIVE_FOLDER, BRAND_GROUPS_MAX_BRANDS (default 50), BRAND_GROUPS_DRY_RUN (true: write
the rows to BRAND_GROUPS_DRY_RUN_CSV instead of the sheet).
"""

import csv
import logging
import os
import sys
from datetime import date

from sqlalchemy import text

from postgres.database_connection import connect_to_db
from quotaclimat.data_ingestion.advertising.s04_brand_groups import registries
from quotaclimat.data_ingestion.advertising.s04_brand_groups.inpi import InpiClient, name_key
from quotaclimat.data_ingestion.advertising.s04_brand_groups.propose import (
    choose_holder, parse_nice_classes, propose_row)
from quotaclimat.data_ingestion.advertising.s04_brand_groups.sheet import (
    BRANDS_TAB, GROUPS_TAB, BrandInventorySheet)
from quotaclimat.utils.sentry import sentry_init

BRANDS_QUERY = text("""
    SELECT predicted_brand, predicted_sector, SUM(duration_sec) AS duration_sec
    FROM advertising.ad_occurrences_classified
    WHERE predicted_brand IS NOT NULL
    GROUP BY predicted_brand, predicted_sector
""")
NICE_CLASSES_COLUMN_QUERY = text("""
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'advertising' AND table_name = 'ref_ome_secteurs' AND column_name = 'classes_nice'
""")
NICE_CLASSES_QUERY = text("SELECT sector_code, classes_nice FROM advertising.ref_ome_secteurs")
CSV_COLUMNS = ["marque", "groupe", "source", "statut", "commentaire", "titulaire", "siren", "lei", "numero_marque"]


def brands_to_process(rows, inventory_keys: set[str], max_brands: int) -> list[tuple[str, str | None]]:
    """(brand, sector) of the broadcast brands missing from the inventory, by decreasing broadcast
    duration: the most frequent spelling and the main sector of the brand."""
    by_key: dict[str, dict] = {}
    for brand, sector, duration in rows:
        key = name_key(brand)
        if not key or key in inventory_keys:
            continue
        entry = by_key.setdefault(key, {"total": 0.0, "spellings": {}, "sectors": {}})
        entry["total"] += duration or 0
        entry["spellings"][brand] = entry["spellings"].get(brand, 0) + (duration or 0)
        if sector:
            entry["sectors"][sector] = entry["sectors"].get(sector, 0) + (duration or 0)
    ranked = sorted(by_key.values(), key=lambda e: -e["total"])[:max_brands]
    return [
        (max(e["spellings"], key=e["spellings"].get), max(e["sectors"], key=e["sectors"].get) if e["sectors"] else None)
        for e in ranked
    ]


def propose(brand: str, sector: str | None, inpi: InpiClient, nice_classes, known_groups, today: date) -> dict:
    holder = choose_holder(inpi.brand_notices(brand), nice_classes.get(sector) if sector else None)
    wikidata_parent = gleif_lei = gleif_parent = None
    if holder:
        wikidata_parent = registries.wikidata_parent(holder.siren)
        gleif_lei = registries.gleif_lei(holder.siren)
        if gleif_lei:
            gleif_parent = registries.gleif_direct_parent(gleif_lei.identifier)
    return propose_row(brand, holder, wikidata_parent, gleif_lei, gleif_parent, known_groups, today)


def run() -> int:
    max_brands = int(os.environ.get("BRAND_GROUPS_MAX_BRANDS", "50"))
    dry_run = os.environ.get("BRAND_GROUPS_DRY_RUN", "false").lower() == "true"
    sheet = BrandInventorySheet.open()
    inventory_keys = {name_key(r.get("marque")) for r in sheet.read(BRANDS_TAB)}
    known_groups = {name_key(r.get("groupe")): r["groupe"] for r in sheet.read(GROUPS_TAB) if r.get("groupe")}

    engine = connect_to_db()
    with engine.connect() as connection:
        brands = brands_to_process(connection.execute(BRANDS_QUERY).all(), inventory_keys, max_brands)
        nice_classes = {}
        if connection.execute(NICE_CLASSES_COLUMN_QUERY).first():
            nice_classes = parse_nice_classes(connection.execute(NICE_CLASSES_QUERY).all())
    if not nice_classes:
        logging.warning(
            "No Nice classes in advertising.ref_ome_secteurs.classes_nice: trademark holders are chosen "
            "without the sector's Nice classes"
        )
    engine.dispose()
    logging.info("%s brands to process", len(brands))

    inpi = InpiClient(os.environ["INPI_USERNAME"], os.environ["INPI_PASSWORD"])
    today = date.today()
    rows = []
    for brand, sector in brands:
        try:
            row = propose(brand, sector, inpi, nice_classes, known_groups, today)
        except Exception:
            # an API error on one brand must not stop the others; the brand is retried on the next run
            logging.exception("Brand %s: proposal failed, skipped", brand)
            continue
        logging.info("Brand %s: group %r (%s)", brand, row["groupe"], row["source"])
        rows.append(row)

    if dry_run:
        path = os.environ.get("BRAND_GROUPS_DRY_RUN_CSV", "brand_groups_proposals.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        logging.info("Dry run: %s rows written to %s", len(rows), path)
    else:
        # re-read just before appending: a brand added by a human during the run is not added twice
        inventory_keys = {name_key(r.get("marque")) for r in sheet.read(BRANDS_TAB)}
        rows = [r for r in rows if name_key(r["marque"]) not in inventory_keys]
        sheet.append(BRANDS_TAB, rows)
        logging.info("%s rows appended to the tab %s", len(rows), BRANDS_TAB)
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"), stream=sys.stdout)
    sentry_init()
    run()
