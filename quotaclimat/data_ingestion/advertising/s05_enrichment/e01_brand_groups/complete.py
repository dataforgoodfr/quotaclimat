"""Completion of the rows already in the tab Marques (flag BRAND_GROUPS_COMPLETE_ROWS of run.py): fills
their empty cells, never a cell holding a value (written by a human or by the job):

1. country and SIREN of the company: from another row of the same company, else from the INPI holder the
   job wrote in commentaires ("titulaire INPI <name> (SIREN <n>)" or "(société étrangère, pays <code>)"),
   when the company is that holder (same name without legal form, or the brand named after the holder):
   for a human-corrected company, the holder can be another legal entity;
2. department of the head office of the company (API Recherche d'entreprises), from its SIREN;
3. ultimate parent company, for a row with neither ultimate parent nor smu_statut (a human clears
   'à vérifier' to have the parent searched again): the one of another row of the same company, else
   GLEIF and Wikidata from the SIREN of the company (or GLEIF from the name and country of a holder
   registered abroad), written 'à vérifier';
4. country and SIREN of the ultimate parent company: from the tab (another row with the same parent, or the
   row of the company of that name), else from the LEI or the Wikidata item the job wrote in commentaires
   for this parent;
5. department of the head office of the ultimate parent company.

INPI is never called: the step spends none of its quota.
"""

import logging
import re
from dataclasses import dataclass, field

import requests

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import (
    registries,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    name_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.propose import (
    COMPANY_COLUMNS,
    PARENT_COLUMNS,
    TO_CHECK,
    Identity,
    KnownCompanies,
    KnownCompany,
    _parent,
    core_key,
    identity_cells,
    identity_from_row,
    name_contains_brand,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.registries import (
    Company,
)

# what propose_row writes in commentaires
HOLDER_NOTE = re.compile(
    r"titulaire INPI (?P<name>.+?) \((?:SIREN (?P<siren>\d{9})|société étrangère, pays (?P<country>[A-Z]{2}|\?))\)"
)
GLEIF_PARENT_NOTE = re.compile(r"GLEIF : société mère ultime (?P<name>.+?) \(LEI (?P<lei>[A-Z0-9]{20})\)")
WIKIDATA_PARENT_NOTE = re.compile(r"Wikidata : société mère ultime (?P<name>.+?) \((?P<qid>Q\d+)\)")


@dataclass
class NotedHolder:
    name: str
    identity: Identity


def noted_holder(commentaires: str) -> NotedHolder | None:
    """INPI trademark holder written by the job in commentaires."""
    match = HOLDER_NOTE.search(commentaires or "")
    if not match:
        return None
    if match["siren"]:
        return NotedHolder(match["name"], Identity("FR", match["siren"]))
    return NotedHolder(match["name"], Identity(match["country"] if match["country"] != "?" else ""))


def is_the_holder(holder_name: str, company: str, brand: str) -> bool:
    """The company of the row is the holder: same name without legal form (RENAULT s.a.s. -> Renault, not
    Renault Trucks), or the company named after the brand the holder's name contains (FREE SAS -> Free)."""
    return core_key(holder_name) == core_key(company) or (
        name_key(company) == name_key(brand) and name_contains_brand(holder_name, brand)
    )


@dataclass
class Registries:
    """Registry lookups of the completion step: answers cached by argument, failures logged and counted
    (a failing lookup only leaves its cells empty), requests counted to stop at max_requests."""

    max_requests: int = 1000
    requests_sent: int = 0
    failures: int = 0
    cache: dict = field(default_factory=dict)

    @property
    def exhausted(self) -> bool:
        return self.requests_sent >= self.max_requests

    def call(self, function, *args):
        key = (function, args)
        if key not in self.cache:
            self.requests_sent += 1
            try:
                self.cache[key] = function(*args)
            except requests.RequestException as e:
                logging.warning("%s%s failed: %s", function.__name__, args, e)
                self.failures += 1
                return None  # not cached: tried again on the next row needing it
        return self.cache[key]

    def department(self, siren: str) -> str:
        return self.call(registries.head_office_department, siren) or ""


def _fill(cells: dict[str, str], row: dict[str, str], values: dict[str, str]) -> None:
    """The values of the cells still empty in the row (and not already filled by an earlier step)."""
    for column, value in values.items():
        if value and not row.get(column, "").strip() and not cells.get(column):
            cells[column] = value


def _fill_identity(cells, row, found: Identity, columns) -> None:
    """Fills the identity cells from found, unless the row already gives another country (a human wrote it)."""
    current = identity_from_row({**row, **cells}, columns)
    if current.country and found.country and current.country != found.country:
        return
    _fill(cells, row, identity_cells(found, columns))


def _parent_identity_from_notes(row: dict[str, str], parent: str, lookups: Registries) -> Identity:
    """Identity of the ultimate parent from the LEI or Wikidata item the job noted for it, when the parent of
    the row is still that one (same name)."""
    commentaires = row.get("commentaires", "")
    for pattern, function, key in (
        (GLEIF_PARENT_NOTE, registries.gleif_lei_record, "lei"),
        (WIKIDATA_PARENT_NOTE, registries.wikidata_item, "qid"),
    ):
        for match in pattern.finditer(commentaires):
            if name_key(match["name"]) == name_key(parent):
                company = lookups.call(function, match[key])
                if company and (company.siren or company.country):
                    return Identity.of_company(company)
    return Identity()


def _search_parent(identity: Identity, holder: NotedHolder | None, lookups: Registries) -> Company | None:
    """Ultimate parent from the registries: GLEIF (by SIREN, or by the exact name and country of a holder
    registered abroad), else Wikidata."""
    wikidata_parent = gleif_lei = None
    if identity.siren:
        wikidata_parent = lookups.call(registries.wikidata_ultimate_parent, identity.siren)
        gleif_lei = lookups.call(registries.gleif_lei, identity.siren)
    elif holder and holder.identity.country:
        gleif_lei = lookups.call(registries.gleif_lei_by_name, holder.name, holder.identity.country)
    gleif_parent = lookups.call(registries.gleif_ultimate_parent, gleif_lei.identifier) if gleif_lei else None
    parent, _ = _parent(wikidata_parent, gleif_parent)
    return parent


def complete_row(row: dict[str, str], known: KnownCompanies, lookups: Registries) -> dict[str, str]:
    """Values for the empty cells of a row of the tab Marques ({column: value}), see the module docstring."""
    company = row.get("entreprise", "").strip()
    if not company:
        return {}
    cells: dict[str, str] = {}
    same_company = known.find_by_name(company)
    holder = noted_holder(row.get("commentaires", ""))
    if holder and not is_the_holder(holder.name, company, row.get("marque", "")):
        holder = None

    # 1-2. the company
    if not identity_from_row(row).siren:
        found = same_company.identity if same_company else Identity()
        found = found.or_else(holder.identity if holder else Identity())
        _fill_identity(cells, row, found, COMPANY_COLUMNS)
    identity = identity_from_row({**row, **cells})
    if identity.siren and not identity.department:
        _fill(cells, row, {"departement_siege_social": lookups.department(identity.siren)})

    # 3. its ultimate parent, searched again when neither the parent nor its status is filled in
    parent = row.get("societe_mere_ultime", "").strip()
    if not parent and not row.get("smu_statut", "").strip():
        if same_company and same_company.parent:
            _fill(cells, row, {"societe_mere_ultime": same_company.parent, "smu_statut": same_company.parent_status})
            _fill_identity(cells, row, same_company.parent_identity, PARENT_COLUMNS)
        else:
            found_parent = _search_parent(identity, holder, lookups)
            if found_parent:
                _fill(cells, row, {"societe_mere_ultime": found_parent.name, "smu_statut": TO_CHECK})
                _fill_identity(cells, row, Identity.of_company(found_parent), PARENT_COLUMNS)
        parent = cells.get("societe_mere_ultime", "")

    # 4-5. the identity of the ultimate parent
    if parent and not identity_from_row({**row, **cells}, PARENT_COLUMNS).siren:
        found = known.parent_identity(parent)
        if not (found.siren or found.country):
            found = _parent_identity_from_notes(row, parent, lookups)
        _fill_identity(cells, row, found, PARENT_COLUMNS)
    parent_identity = identity_from_row({**row, **cells}, PARENT_COLUMNS)
    if parent_identity.siren and not parent_identity.department:
        _fill(cells, row, {"smu_departement": lookups.department(parent_identity.siren)})
    return cells


def complete_rows(
    rows: list[dict[str, str]], lookups: Registries
) -> list[tuple[int, str, dict[str, str]]]:
    """(index of the row in the tab, its brand, {column: value}) for the rows with cells to fill, until the
    registries request budget is spent. The values found complete the next rows of the same company."""
    known = KnownCompanies.from_rows(rows)
    updates = []
    for index, row in enumerate(rows):
        if lookups.exhausted:
            logging.warning(
                "Completion: %s registry requests sent, stopped at row %s of %s: the next rows are left for the "
                "next run", lookups.requests_sent, index + 2, len(rows) + 1,
            )
            break
        cells = complete_row(row, known, lookups)
        if not cells:
            continue
        logging.info("Completion of %s: %s", row.get("marque"), cells)
        updates.append((index, row.get("marque", ""), cells))
        completed = {**row, **cells}
        known.add(KnownCompany(
            completed["entreprise"], completed.get("societe_mere_ultime", ""), completed.get("smu_statut", ""),
            completed.get("marque", ""), identity_from_row(completed), identity_from_row(completed, PARENT_COLUMNS),
        ))
    return updates
