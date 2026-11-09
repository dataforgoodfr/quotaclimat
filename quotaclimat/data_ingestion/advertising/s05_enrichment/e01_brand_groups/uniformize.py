"""Uniformization of the rows of the tab Marques (flag BRAND_GROUPS_UNIFORMIZE_ROWS of run.py, on by default):
the rows of the same company (same entreprise, compared with name_key: "L'Oréal" = "L’OREAL") get the same
values in the columns describing the company, so that an ultimate parent company or an ultimate majority
shareholder written on one of its rows is on all of them. On a conflict, the highest row of the tab wins.

Three blocks of columns, each taken as a whole from the rows that give it, so that a value is never written
with the identity of another one:
1. identity of the company (pays_siege_social, siren, departement_siege_social): the one of the highest row
   with a SIREN or a country; its department, when that row has none, the one of the highest row with the
   same SIREN;
2. ultimate parent company (societe_mere_ultime, smu_statut) and its identity (smu_pays, smu_siren,
   smu_departement): the parent of the highest row with one used by dbt (status other than 'à vérifier'),
   else of the highest row with one 'à vérifier'. Its status: the best one of the rows with this parent
   ('vérifié', then 'non vérifié' or empty, then 'à vérifier'; the highest row on a tie). Its identity: as in
   1, among the rows with this parent (cleared on the other rows when none gives it: it was the identity of
   another parent);
3. ultimate majority shareholder (actionnaire_maj_ultime, amu_statut): as the ultimate parent, without identity.

A block no row of the company fills is left as it is. The cells are overwritten, human values included: the
previous values are appended to commentaires, to undo a change by hand. Rows without company are left as they
are.
"""

import logging
from dataclasses import dataclass, field
from datetime import date

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    name_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.propose import (
    COMPANY_COLUMNS,
    PARENT_COLUMNS,
    TO_CHECK,
    UNVERIFIED,
    VERIFIED,
    Identity,
    identity_cells,
    identity_from_row,
)

# rank of the statuses: the lowest wins between rows with the same value; an empty or unknown status is used
# by dbt like 'non vérifié'
STATUS_RANK = {VERIFIED: 0, UNVERIFIED: 1, TO_CHECK: 2}


def _status_rank(status: str) -> int:
    return STATUS_RANK.get((status or "").strip().lower(), 1)


@dataclass
class Block:
    """Columns of the tab Marques describing the company, taken together from the rows giving them."""

    value: str | None  # column of the value (None: the block is an identity only)
    status: str | None = None
    identity: tuple[str, ...] = ()

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(c for c in (self.value, self.status) if c) + self.identity


COMPANY = Block(None, identity=COMPANY_COLUMNS)
PARENT = Block("societe_mere_ultime", "smu_statut", PARENT_COLUMNS)
SHAREHOLDER = Block("actionnaire_maj_ultime", "amu_statut")
BLOCKS = (COMPANY, PARENT, SHAREHOLDER)


@dataclass
class RowChange:
    """Cells of a row of the tab to overwrite: {column: (current value, new value)} and, for each column, the
    line of the sheet its new value comes from."""

    index: int  # index of the row in the tab as given by BrandInventorySheet.read (line - 2)
    brand: str
    company: str
    cells: dict[str, tuple[str, str]] = field(default_factory=dict)
    sources: dict[str, int] = field(default_factory=dict)

    def note(self, today: date) -> str:
        """What the job changed, with the previous values, for commentaires."""
        changes = ", ".join(
            f"{column} {new or 'vide'} (ligne {self.sources[column]}, avant : {old or 'vide'})"
            for column, (old, new) in self.cells.items()
        )
        return (
            f"job {today.isoformat()} : valeurs de l'entreprise {self.company} uniformisées sur la ligne la plus "
            f"haute qui les donne : {changes}"
        )


def _line(index: int) -> int:
    return index + 2


def _identity(rows: list[tuple[int, dict[str, str]]], columns) -> tuple[Identity, int] | None:
    """Identity of the highest row giving a SIREN or a country, its department else the one of the highest row
    with the same SIREN; and the index of the row it comes from."""
    identities = [(index, identity_from_row(row, columns)) for index, row in rows]
    source = next(((index, i) for index, i in identities if i.siren or i.country), None)
    if source is None:
        return None
    index, identity = source
    department = identity.department or next(
        (i.department for _, i in identities if identity.siren and i.siren == identity.siren and i.department), ""
    )
    return Identity(identity.country, identity.siren, department), index


def _block_values(block: Block, rows: list[tuple[int, dict[str, str]]]) -> dict[str, tuple[str, int]] | None:
    """{column: (value, index of the row it comes from)} of the block for the company, None when no row
    gives it."""
    if block.value is None:
        found = _identity(rows, block.identity)
        if found is None:
            return None
        identity, index = found
        return {c: (v, index) for c, v in identity_cells(identity, block.identity).items()}

    with_value = [(index, row) for index, row in rows if row.get(block.value, "").strip()]
    if not with_value:
        return None
    used = [(index, row) for index, row in with_value if _status_rank(row.get(block.status, "")) < STATUS_RANK[TO_CHECK]]
    source_index, source = (used or with_value)[0]
    value = source[block.value].strip()
    same_value = [(index, row) for index, row in with_value if name_key(row[block.value]) == name_key(value)]
    # min keeps the first (highest) row on a tie
    status_index, status_row = min(same_value, key=lambda r: _status_rank(r[1].get(block.status, "")))
    values = {
        block.value: (value, source_index),
        block.status: (status_row.get(block.status, "").strip(), status_index),
    }
    if block.identity:
        found = _identity(same_value, block.identity)
        identity, index = found if found else (Identity(), source_index)
        values.update({c: (v, index) for c, v in identity_cells(identity, block.identity).items()})
    return values


def _normalized(row: dict[str, str], column: str) -> str:
    """Value of a cell as compared: SIREN without spaces and with its leading zeros, country in upper case."""
    for columns in (COMPANY_COLUMNS, PARENT_COLUMNS):
        if column in columns:
            return identity_cells(identity_from_row(row, columns), columns)[column]
    return row.get(column, "").strip()


def uniformize_rows(rows: list[dict[str, str]]) -> list[RowChange]:
    """Cells to overwrite so that the rows of the same company give the same values, the highest row winning
    on a conflict (see the module docstring), in the order of the tab."""
    companies: dict[str, list[tuple[int, dict[str, str]]]] = {}
    for index, row in enumerate(rows):
        key = name_key(row.get("entreprise"))
        if key:
            companies.setdefault(key, []).append((index, row))

    changes: dict[int, RowChange] = {}
    for company_rows in companies.values():
        if len(company_rows) < 2:
            continue
        for block in BLOCKS:
            values = _block_values(block, company_rows)
            if values is None:
                continue
            target = {column: value for column, (value, _) in values.items()}
            for index, row in company_rows:
                for column, (value, source_index) in values.items():
                    if column in block.identity:
                        # "552 081 317" and "552081317" are the same SIREN
                        same = _normalized(row, column) == _normalized(target, column)
                    else:
                        # the spelling of the parent or shareholder too
                        same = row.get(column, "").strip() == value
                    if same:
                        continue
                    change = changes.setdefault(
                        index, RowChange(index, row.get("marque", ""), company_rows[0][1]["entreprise"].strip())
                    )
                    change.cells[column] = (row.get(column, "").strip(), value)
                    change.sources[column] = _line(source_index)
    return [changes[index] for index in sorted(changes)]


def with_notes(changes: list[RowChange], rows: list[dict[str, str]], today: date) -> list[RowChange]:
    """The changes with their note appended to the commentaires of the row."""
    for change in changes:
        commentaires = rows[change.index].get("commentaires", "").strip()
        note = change.note(today)
        change.cells["commentaires"] = (commentaires, f"{commentaires} ; {note}" if commentaires else note)
        logging.info("Uniformization of %s (line %s): %s", change.brand, _line(change.index), note)
    return changes
