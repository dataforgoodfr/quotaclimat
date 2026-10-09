"""Stage 4 (enriching), step 1: propose the company and the ultimate parent company of the broadcast
brands missing from the tab Marques of the Inventaires_des_marques Google Sheet, and append them to it
with e_statut and smu_statut 'non vérifié'.

For each brand, by decreasing broadcast duration:
1. INPI: trademarks in force named exactly like the brand, holder (company, SIREN) of the most
   trademarks covering the Nice classes of the brand's sector (column classes_nice of the tab secteurs of
   the Dictionnaire_marques_secteurs Google Sheet, loaded by dbt in advertising.ref_ome_secteurs);
2. company of the brand: the holder, named like the brand when its name contains it (FREE SAS -> Free),
   else under its own name (Dacia -> Renault). A company already in the tab (same name, or same name
   without legal form) is reused with its ultimate parent company;
3. ultimate parent company of a new company: GLEIF ultimate consolidating parent of the holder's LEI (by
   SIREN, or by exact legal name and country for a holder registered abroad), else the top of the
   Wikidata P749 chain of the item with its SIREN;
4. a row is appended to the tab Marques when a company was found, commentaires explaining how. A brand
   without any is not written, so that it is searched again on the next runs (and can be filled in by a
   human meanwhile).

Env: POSTGRES_*, INPI_USERNAME, INPI_PASSWORD, GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON,
EXTERNAL_SOURCES_DRIVE_FOLDER, BRAND_GROUPS_MAX_BRANDS (default 50), BRAND_GROUPS_DRY_RUN (true: write
the rows to BRAND_GROUPS_DRY_RUN_CSV and print them in the logs, tab-separated, instead of the sheet).
"""

import csv
import logging
import os
import sys
from datetime import date

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import (
    registries,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    InpiClient,
    InpiQuotaExceeded,
    name_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.propose import (
    KnownCompanies,
    KnownCompany,
    choose_holder,
    parse_nice_classes,
    propose_row,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.sheet import (
    BRANDS_TAB,
    BrandInventorySheet,
)
import requests
from sqlalchemy import text

from postgres.database_connection import connect_to_db
from quotaclimat.utils.sentry import sentry_init

BRANDS_QUERY = text("""
    SELECT predicted_brand, predicted_sector, SUM(duration_sec) AS duration_sec
    FROM analytics.publicites
    WHERE predicted_brand IS NOT NULL
    GROUP BY predicted_brand, predicted_sector
""")
NICE_CLASSES_COLUMN_QUERY = text("""
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'advertising' AND table_name = 'ref_ome_secteurs' AND column_name = 'classes_nice'
""")
NICE_CLASSES_QUERY = text(
    "SELECT sector_code, classes_nice FROM advertising.ref_ome_secteurs"
)
CSV_COLUMNS = [
    "marque",
    "entreprise",
    "societe_mere_ultime",
    "e_statut",
    "smu_statut",
    "commentaires",
]


def print_rows(rows: list[dict], out=None) -> None:
    """Rows in the logs, tab-separated between two markers: in a container the dry-run CSV is lost, the
    lines can be pasted in a sheet."""
    out = out or sys.stdout
    print("----- BRAND GROUPS PROPOSALS (tab-separated) -----", file=out)
    writer = csv.DictWriter(out, fieldnames=CSV_COLUMNS, delimiter="\t", lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    # one line per row: no tab nor line break inside a value
    writer.writerows({k: " ".join(str(v).split()) for k, v in row.items()} for row in rows)
    print("----- END OF BRAND GROUPS PROPOSALS -----", file=out, flush=True)


def brands_to_process(
    rows, inventory_keys: set[str], max_brands: int
) -> list[tuple[str, str | None]]:
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
        (
            max(e["spellings"], key=e["spellings"].get),
            max(e["sectors"], key=e["sectors"].get) if e["sectors"] else None,
        )
        for e in ranked
    ]


def propose(
    brand: str,
    sector: str | None,
    inpi: InpiClient,
    nice_classes,
    known: KnownCompanies,
    today: date,
) -> dict:
    """Row of the tab Marques for the brand."""
    holder = choose_holder(
        inpi.brand_notices(brand), nice_classes.get(sector) if sector else None
    )
    wikidata_parent = gleif_lei = gleif_parent = None
    # a registry still failing after its retries does not lose the INPI holder (and its quota): the brand
    # is proposed without the answers of this registry, noted in the commentaire
    errors: list[str] = []

    def lookup(source: str, function, *args):
        try:
            return function(*args)
        except requests.RequestException as e:
            logging.warning("Brand %s: %s lookup failed: %s", brand, source, e)
            errors.append(source)
            return None

    existing = holder and known.find(holder, brand)
    if holder is None or (existing and existing.parent):
        # no company, or a company already in the tab with its ultimate parent: no registry lookup needed
        pass
    elif holder.siren:
        wikidata_parent = lookup("Wikidata", registries.wikidata_ultimate_parent, holder.siren)
        gleif_lei = lookup("GLEIF", registries.gleif_lei, holder.siren)
    elif holder.country:
        # company registered abroad: no SIREN, its LEI by its exact legal name and country
        gleif_lei = lookup("GLEIF", registries.gleif_lei_by_name, holder.name, holder.country)
    if gleif_lei:
        gleif_parent = lookup("GLEIF", registries.gleif_ultimate_parent, gleif_lei.identifier)
    return propose_row(
        brand, holder, wikidata_parent, gleif_lei, gleif_parent, known, today, unavailable=errors
    )


def run() -> int:
    max_brands = int(os.environ.get("BRAND_GROUPS_MAX_BRANDS", "50"))
    dry_run = os.environ.get("BRAND_GROUPS_DRY_RUN", "false").lower() == "true"
    sheet = BrandInventorySheet.open()
    if not dry_run:
        # values of missing columns are dropped: the rows would be appended without them
        missing = [c for c in CSV_COLUMNS if c not in sheet.header(BRANDS_TAB)]
        if missing:
            raise RuntimeError(f"tab {BRANDS_TAB} has no column {', '.join(missing)}: add them before running the job")
    inventory = sheet.read(BRANDS_TAB)
    inventory_keys = {name_key(r.get("marque")) for r in inventory}
    known = KnownCompanies.from_rows(inventory)

    engine = connect_to_db()
    with engine.connect() as connection:
        brands = brands_to_process(
            connection.execute(BRANDS_QUERY).all(), inventory_keys, max_brands
        )
        nice_classes = {}
        if connection.execute(NICE_CLASSES_COLUMN_QUERY).first():
            nice_classes = parse_nice_classes(
                connection.execute(NICE_CLASSES_QUERY).all()
            )
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
            row = propose(brand, sector, inpi, nice_classes, known, today)
        except InpiQuotaExceeded as e:
            # every following brand would fail too: stop, keep the rows found so far
            logging.error("Brand %s: %s. The remaining brands are left for the next run", brand, e)
            break
        except Exception:
            # an API error on one brand must not stop the others; the brand is retried on the next run
            logging.exception("Brand %s: proposal failed, skipped", brand)
            continue
        if row["entreprise"]:
            logging.info(
                "Brand %s: company %r, ultimate parent %r", brand, row["entreprise"], row["societe_mere_ultime"]
            )
            # the next brands of the same company reuse it
            known.add(KnownCompany(row["entreprise"], row["societe_mere_ultime"], row["smu_statut"], brand))
        else:
            logging.info("Brand %s: no trademark in force found (FR, EU, WO), not written, searched again next run", brand)
        rows.append(row)
        logging.info("%s rows proposed, %s so far", len(rows), inpi.requests_summary)

    logging.info("INPI: %s for %s brands", inpi.requests_summary, len(rows))
    if dry_run:
        path = os.environ.get("BRAND_GROUPS_DRY_RUN_CSV", "brand_groups_proposals.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        logging.info("Dry run: %s rows written to %s", len(rows), path)
        print_rows(rows)
    else:
        # re-read just before appending: a brand added by a human during the run is not added twice
        inventory_keys = {name_key(r.get("marque")) for r in sheet.read(BRANDS_TAB)}
        # only the brands with a company: the others are searched again on the next runs
        rows = [r for r in rows if r["entreprise"] and name_key(r["marque"]) not in inventory_keys]
        sheet.append(BRANDS_TAB, rows)
        logging.info("%s rows appended to the tab %s", len(rows), BRANDS_TAB)
    return len(rows)

if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"), stream=sys.stdout)
    sentry_init()
    run()
