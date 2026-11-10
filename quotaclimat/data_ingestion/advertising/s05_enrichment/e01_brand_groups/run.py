"""Stage 5 (enriching), step 1: propose the company and the ultimate parent company of the broadcast
brands missing from the tab Marques of the Inventaire_des_marques Google Sheet, and append them to it
with e_statut 'non vérifié' and smu_statut 'à vérifier' (both empty when nothing was found). Then, unless
BRAND_GROUPS_UNIFORMIZE_ROWS=false, give the rows of the same company the same identity, ultimate parent
company and ultimate majority shareholder, the highest row of the tab winning on a conflict (uniformize.py).
Then, with BRAND_GROUPS_COMPLETE_ROWS=true, fill the empty cells of the rows already in the tab (complete.py).

For each brand, by decreasing broadcast duration:
1. INPI: trademarks in force named exactly like the brand, holder (company, SIREN) of the most
   trademarks covering the Nice classes of the brand's sector (column classes_nice of the tab secteurs of
   the Dictionnaire_marques_secteurs Google Sheet, loaded by dbt in advertising.ref_ome_secteurs);
2. company of the brand: the holder, named like the brand when its name contains it (FREE SAS -> Free),
   else under its own name (Dacia -> Renault). A company already in the tab (same name, or same name
   without legal form) is reused with its identity (country, SIREN, department) and ultimate parent
   company. Country and SIREN of a new company: the holder's;
3. ultimate parent company of a new company: GLEIF ultimate consolidating parent of the holder's LEI (by
   SIREN, or by exact legal name and country for a holder registered abroad), else the top of the
   Wikidata P749 chain of the item with its SIREN, with its country and SIREN;
4. departments of the head offices of the company and of its ultimate parent: API Recherche d'entreprises
   (Sirene), from their SIREN;
5. a row is appended to the tab Marques, commentaires explaining how. A brand without any trademark
   found is written too, without company nor status (for a human to fill in), so that it is not searched
   again on every run.

Env: POSTGRES_*, INPI_USERNAME, INPI_PASSWORD, GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON,
EXTERNAL_SOURCES_DRIVE_FOLDER, BRAND_GROUPS_MAX_BRANDS (default 50), BRAND_GROUPS_DRY_RUN (true: write
the rows to BRAND_GROUPS_DRY_RUN_CSV and print them in the logs, tab-separated, instead of the sheet; the
completed and uniformized cells only printed), BRAND_GROUPS_UNIFORMIZE_ROWS (default true),
BRAND_GROUPS_COMPLETE_ROWS (default false), BRAND_GROUPS_COMPLETE_MAX_REQUESTS
(registry requests of the completion step, default 1000).
"""

import csv
import logging
import os
import sys
from datetime import date

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import (
    registries,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.complete import (
    Registries,
    complete_rows,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    InpiClient,
    InpiQuotaExceeded,
    InpiUnavailable,
    name_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.propose import (
    KnownCompanies,
    KnownCompany,
    PARENT_COLUMNS,
    choose_holder,
    clean_siren,
    identity_from_row,
    parse_nice_classes,
    propose_row,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.sheet import (
    BRANDS_TAB,
    BrandInventorySheet,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.uniformize import (
    uniformize_rows,
    with_notes,
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
# columns of the tab Marques, in its order (dry-run lines pasted in the sheet); the job leaves
# actionnaire_maj_ultime and amu_statut empty, for a human to fill in
CSV_COLUMNS = [
    "marque",
    "entreprise",
    "pays_siege_social",
    "siren",
    "departement_siege_social",
    "e_statut",
    "societe_mere_ultime",
    "smu_pays",
    "smu_siren",
    "smu_departement",
    "smu_statut",
    "actionnaire_maj_ultime",
    "amu_statut",
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


DEPARTMENT_COLUMNS = (
    ("siren", "departement_siege_social", "de l'entreprise"),
    ("smu_siren", "smu_departement", "de la société mère ultime"),
)


def fill_departments(row: dict, departments: Registries) -> list[str]:
    """Departments of the head offices of the company and of its ultimate parent, from their SIREN, unless
    already given; notes for the ones not found."""
    notes = []
    for siren_column, department_column, label in DEPARTMENT_COLUMNS:
        siren = clean_siren(row.get(siren_column))
        if not siren or row.get(department_column):
            continue
        failures = departments.failures
        row[department_column] = departments.department(siren)
        if departments.failures > failures:
            notes.append(f"Recherche d'entreprises indisponible pendant le job, département du siège {label} à rechercher")
        elif not row[department_column]:
            notes.append(f"département du siège {label} non trouvé (Recherche d'entreprises)")
    return notes


def propose(
    brand: str,
    sector: str | None,
    inpi: InpiClient,
    nice_classes,
    known: KnownCompanies,
    today: date,
    departments: Registries | None = None,
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
    row = propose_row(brand, holder, wikidata_parent, gleif_lei, gleif_parent, known, today, unavailable=errors)
    notes = fill_departments(row, departments or Registries())
    if notes:
        row["commentaires"] += " ; " + " ; ".join(notes)
    return row


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

    today = date.today()
    rows = []
    departments = Registries()
    inpi = InpiClient(os.environ["INPI_USERNAME"], os.environ["INPI_PASSWORD"]) if brands else None
    for brand, sector in brands:
        try:
            row = propose(brand, sector, inpi, nice_classes, known, today, departments)
        except (InpiQuotaExceeded, InpiUnavailable) as e:
            # every following brand would fail too: stop, keep the rows found so far. The quota stops most
            # runs (about 100 requests an hour): an error only when the run processed no brand at all
            level = logging.WARNING if rows else logging.ERROR
            logging.log(level, "Brand %s: %s. The remaining brands are left for the next run", brand, e)
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
            known.add(KnownCompany(
                row["entreprise"], row["societe_mere_ultime"], row["smu_statut"], brand, identity_from_row(row),
                identity_from_row(row, PARENT_COLUMNS),
            ))
        else:
            logging.info("Brand %s: no trademark in force found (FR, EU, WO), written without company", brand)
        rows.append(row)
        logging.info("%s rows proposed, %s so far", len(rows), inpi.requests_summary)

    if inpi:
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
        # the brands without company too: not searched again on the next runs
        rows = [r for r in rows if name_key(r["marque"]) not in inventory_keys]
        sheet.append(BRANDS_TAB, rows)
        logging.info("%s rows appended to the tab %s", len(rows), BRANDS_TAB)

    if os.environ.get("BRAND_GROUPS_UNIFORMIZE_ROWS", "true").lower() == "true":
        uniformize(sheet, dry_run, today)
    if os.environ.get("BRAND_GROUPS_COMPLETE_ROWS", "false").lower() == "true":
        complete(sheet, dry_run, int(os.environ.get("BRAND_GROUPS_COMPLETE_MAX_REQUESTS", "1000")), departments)
    return len(rows)


def uniformize(sheet: BrandInventorySheet, dry_run: bool, today: date) -> int:
    """Gives the rows of the same company the same values, the highest row winning (uniformize.py); dry run:
    only in the logs."""
    rows = sheet.read(BRANDS_TAB)
    changes = with_notes(uniformize_rows(rows), rows, today)
    logging.info(
        "Uniformization: %s cells to overwrite in %s rows", sum(len(c.cells) - 1 for c in changes), len(changes)
    )
    if dry_run:
        print("----- BRAND GROUPS UNIFORMIZATION (line, brand, {column: (before, after)}) -----")
        for change in changes:
            print(f"{change.index + 2}\t{change.brand}\t{change.cells}")
        print("----- END OF BRAND GROUPS UNIFORMIZATION -----", flush=True)
        return 0
    written = sheet.replace_cells(BRANDS_TAB, [(c.index, c.brand, c.cells) for c in changes])
    logging.info("Uniformization: %s cells written to the tab %s (commentaires included)", written, BRANDS_TAB)
    return written


def complete(sheet: BrandInventorySheet, dry_run: bool, max_requests: int, departments: Registries) -> int:
    """Fills the empty cells of the rows of the tab (complete.py); dry run: only in the logs."""
    lookups = Registries(max_requests=max_requests, cache=departments.cache)
    updates = complete_rows(sheet.read(BRANDS_TAB), lookups)
    cells = sum(len(c) for _, _, c in updates)
    logging.info(
        "Completion: %s cells to fill in %s rows, %s registry requests (%s failed)",
        cells, len(updates), lookups.requests_sent, lookups.failures,
    )
    if dry_run:
        print("----- BRAND GROUPS COMPLETION (line, brand, cells) -----")
        for index, brand, values in updates:
            print(f"{index + 2}\t{brand}\t{values}")
        print("----- END OF BRAND GROUPS COMPLETION -----", flush=True)
        return 0
    written = sheet.fill_empty_cells(BRANDS_TAB, updates)
    logging.info("Completion: %s cells written to the tab %s", written, BRANDS_TAB)
    return written

if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"), stream=sys.stdout)
    sentry_init()
    run()
