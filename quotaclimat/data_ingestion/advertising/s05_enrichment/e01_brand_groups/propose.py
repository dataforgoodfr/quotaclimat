"""Row of the tab Marques for a brand: its company, the INPI trademark holder, named like the brand when the
holder's name contains it (FREE -> Free, SOCIETE FRANCAISE DU RADIOTELEPHONE - SFR -> SFR), else like the
holder (Dacia -> Renault), with its country and SIREN; and the ultimate parent company of the holder (GLEIF,
else Wikidata), with its country and SIREN. The departments of the head offices are looked up afterwards
(run.py, API Recherche d'entreprises).

A company already in the tab (written by the job or corrected by a human) is reused, with its identity
(country, SIREN, department) and ultimate parent company. Statuses: 'vérifié', 'non vérifié' (used anyway)
or 'à vérifier' (not used by dbt). The job writes the company 'non vérifié' and the ultimate parent company
it found 'à vérifier', and leaves the status empty when it found nothing: a human checks and corrects them
afterwards in the sheet.
"""

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date

from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.normalize import normalize
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    COMPANY_NAME_WORDS,
    Notice,
    name_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.registries import (
    Company,
)


VERIFIED = "vérifié"
UNVERIFIED = "non vérifié"  # used anyway
TO_CHECK = "à vérifier"  # not used by dbt


@dataclass
class Holder:
    """Trademark holder chosen for a brand, among the notices of its trademarks."""

    name: str
    siren: str  # empty for a company registered abroad
    application_number: str
    other_holders: list[str]
    country: str | None = None
    # trademark number with its collection prefix (FR..., EU..., WO...)
    notice_number: str = ""
    # False when none of the trademarks read covers the Nice classes of the brand's sector
    in_sector_classes: bool = True

    @property
    def trademark(self) -> str:
        return self.notice_number or f"FR{self.application_number}"


def _holder_id(notice: Notice) -> str:
    """SIREN of the holder, else its name (companies registered abroad have no SIREN)."""
    return notice.holder_siren or f"name:{name_key(notice.holder_name)}"


def _holder_label(notice: Notice) -> str:
    return f"{notice.holder_name} ({notice.holder_siren or notice.holder_country or 'sans SIREN'})"


def parse_nice_classes(rows) -> dict[str, set[int]]:
    """(sector_code, classes_nice) rows of advertising.ref_ome_secteurs -> {sector_code: Nice classes},
    classes_nice like "3; 32" (or "3, 32")."""
    out = {}
    for code, classes_text in rows:
        code = str(code or "").strip()
        classes = {
            int(c)
            for c in str(classes_text or "").replace(",", ";").split(";")
            if c.strip().isdigit()
        }
        if code and classes:
            out[code] = classes
    return out


def choose_holder(
    notices: list[Notice], sector_classes: set[int] | None
) -> Holder | None:
    """The holder of the most trademarks covering the sector's Nice classes, most recent trademark first
    on a tie. All trademarks when the sector has no classes or when none covers them (e.g. Amazon: the
    few notices read can be in other classes), flagged for the human check."""
    if not notices:
        return None
    relevant = [n for n in notices if not sector_classes or n.classes & sector_classes]
    in_sector_classes = bool(relevant)
    relevant = relevant or notices
    counts = Counter(_holder_id(n) for n in relevant)
    # Counter keeps the first-seen order on ties, notices are the most recent first
    holder_id, _ = counts.most_common(1)[0]
    chosen = next(n for n in relevant if _holder_id(n) == holder_id)
    others = sorted({_holder_label(n) for n in relevant if _holder_id(n) != holder_id})
    return Holder(
        name=chosen.holder_name or chosen.holder_siren,
        siren=chosen.holder_siren or "",
        application_number=chosen.application_number,
        other_holders=others,
        country=chosen.holder_country,
        notice_number=chosen.notice_number,
        in_sector_classes=in_sector_classes,
    )


def name_contains_brand(name: str, brand: str) -> bool:
    """The brand is in the name as whole consecutive words, compared without spaces and punctuation:
    "SOCIETE FRANCAISE DU RADIOTELEPHONE - SFR" contains SFR, "McDonald's International" contains
    "Mc Donald's", "FREEDOM HOLDING" does not contain Free."""
    words, key = normalize(name).split(), name_key(brand)
    return bool(key) and any(
        "".join(words[i:j]) == key for i in range(len(words)) for j in range(i + 1, len(words) + 1)
    )


def company_name(holder: Holder, brand: str) -> str:
    """The brand when the holder's name contains it: the company is the brand itself, under its common
    name rather than its legal one (FREE SAS -> Free). Else the holder (Dacia -> its holder Renault)."""
    return brand if name_contains_brand(holder.name, brand) else holder.name


def core_key(name: str) -> str:
    """name_key without the legal form and generic company words: "RENAULT s.a.s.", "Renault SA" and
    "Groupe Renault" -> "renault". The whole name_key when nothing else is left."""
    # dotted acronyms in one word: s.a.s. -> sas
    name = re.sub(r"\b(?:\w\.){2,}", lambda m: m.group().replace(".", ""), name or "")
    words = [w for w in normalize(name).split() if w not in COMPANY_NAME_WORDS]
    return "".join(words) or name_key(name)


def clean_siren(value: str | None) -> str:
    """9-digit SIREN from a cell of the sheet: spaces and dots removed ("552 081 317"), leading zeros put
    back when the sheet read it as a number ("5520135" -> "005520135"); empty when not a SIREN."""
    digits = "".join(c for c in str(value or "") if c not in " .  ")
    return digits.zfill(9) if digits.isdigit() and 7 <= len(digits) <= 9 else ""


@dataclass
class Identity:
    """Country, SIREN and head office department of a company (all empty when unknown; no SIREN nor
    department for a company registered abroad)."""

    country: str = ""
    siren: str = ""
    department: str = ""

    @classmethod
    def of_company(cls, company: Company | None) -> "Identity":
        return cls(company.country, company.siren) if company else cls()

    def or_else(self, other: "Identity") -> "Identity":
        """This identity, else the other one when this one has no SIREN nor country."""
        return self if (self.siren or self.country) else other


# columns of the tab Marques holding the identity of the company and of its ultimate parent
COMPANY_COLUMNS = ("pays_siege_social", "siren", "departement_siege_social")
PARENT_COLUMNS = ("smu_pays", "smu_siren", "smu_departement")


def identity_from_row(row: dict[str, str], columns=COMPANY_COLUMNS) -> Identity:
    country, siren, department = (row.get(c, "").strip() for c in columns)
    return Identity(country.upper(), clean_siren(siren), department)


def identity_cells(identity: Identity, columns=COMPANY_COLUMNS) -> dict[str, str]:
    return dict(zip(columns, (identity.country, identity.siren, identity.department)))


@dataclass
class KnownCompany:
    name: str
    parent: str
    parent_status: str
    brand: str
    identity: Identity = field(default_factory=Identity)
    parent_identity: Identity = field(default_factory=Identity)


@dataclass
class KnownCompanies:
    """Companies of the tab Marques (and the ones proposed during the run), with their identity and their
    ultimate parent company (the verified one first): by name_key and by name without legal form (core_key).
    The identity of a company (or of its ultimate parent) missing on its first row is taken from another
    row of the same company."""

    by_key: dict[str, KnownCompany] = field(default_factory=dict)
    by_core: dict[str, KnownCompany] = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows: list[dict[str, str]]) -> "KnownCompanies":
        known = cls()
        # verified ultimate parents first, the ones to check last: they win for a company written on
        # several rows
        rank = {VERIFIED: 0, TO_CHECK: 2}
        for row in sorted(rows, key=lambda r: rank.get(r.get("smu_statut", ""), 1)):
            if row.get("entreprise"):
                known.add(KnownCompany(
                    row["entreprise"], row.get("societe_mere_ultime", ""), row.get("smu_statut", ""),
                    row.get("marque", ""), identity_from_row(row), identity_from_row(row, PARENT_COLUMNS),
                ))
        return known

    def add(self, company: KnownCompany) -> None:
        for index, key in ((self.by_key, name_key(company.name)), (self.by_core, core_key(company.name))):
            first = index.setdefault(key, company)
            if first is company:
                continue
            # the first row wins (its ultimate parent), completed by the identities of the other rows
            first.identity = first.identity.or_else(company.identity)
            if not first.identity.department and first.identity.siren == company.identity.siren:
                first.identity.department = company.identity.department
            if not first.parent:
                first.parent, first.parent_status = company.parent, company.parent_status
                first.parent_identity = company.parent_identity
            elif name_key(first.parent) == name_key(company.parent):
                first.parent_identity = first.parent_identity.or_else(company.parent_identity)

    def find(self, holder: Holder, brand: str) -> KnownCompany | None:
        """Company of the holder already in the tab, by name (company_name, or the holder's), else by name
        without legal form (Dacia, held by "RENAULT s.a.s.", gets the company "Renault" of the brand
        Renault), else the company whose name the holder's name contains."""
        return (
            self.by_key.get(name_key(company_name(holder, brand)))
            or self.by_key.get(name_key(holder.name))
            or self.by_core.get(core_key(company_name(holder, brand)))
            or self.by_core.get(core_key(holder.name))
            or self._contained_in(holder.name)
        )

    def find_by_name(self, name: str) -> KnownCompany | None:
        return self.by_key.get(name_key(name)) if name else None

    def parent_identity(self, parent: str) -> Identity:
        """Identity of an ultimate parent company from the tab: the one written for it on another row, else
        the one of the company of that name (the parent of a brand can be the company of another one)."""
        key = name_key(parent)
        for company in self.by_key.values():
            if company.parent and name_key(company.parent) == key and (
                company.parent_identity.siren or company.parent_identity.country
            ):
                return company.parent_identity
        same_name = self.find_by_name(parent)
        return same_name.identity if same_name else Identity()

    def _contained_in(self, holder_name: str) -> KnownCompany | None:
        """The company whose name the holder's name contains (as for a brand): "Inter IKEA Systems B.V."
        -> the company IKEA, written for the brand IKEA. The longest name when several."""
        found = [c for c in self.by_key.values() if name_contains_brand(holder_name, c.name)]
        return max(found, key=lambda c: len(name_key(c.name)), default=None)


def holder_identity(holder: Holder) -> Identity:
    return Identity("FR", holder.siren) if holder.siren else Identity(holder.country or "")


def company_identity(holder: Holder, existing: KnownCompany | None) -> Identity:
    """Identity of the company of the row: the one of the company already in the tab, else the holder's
    (the company is the holder). Never the holder's for a company of the tab without identity: the holder
    can be another legal entity than the one the tab names (a subsidiary, a holding)."""
    return existing.identity if existing else holder_identity(holder)


def _parent(wikidata_parent: Company | None, gleif_parent: Company | None) -> tuple[Company | None, str]:
    """Ultimate parent company found and its source: GLEIF, whose ultimate parent is the consolidating one
    (the definition of the column societe_mere_ultime), else Wikidata (top of the parent organizations,
    when it has a label)."""
    if gleif_parent:
        return gleif_parent, "gleif"
    if wikidata_parent and wikidata_parent.name:
        return wikidata_parent, "wikidata"
    return None, ""


def parent_notes(
    holder_has_siren: bool,
    wikidata_parent: Company | None,
    gleif_lei: Company | None,
    gleif_parent: Company | None,
    unavailable: list[str] | None = None,
) -> tuple[Company | None, list[str]]:
    """Ultimate parent company found (GLEIF, else Wikidata) and the notes explaining it."""
    notes = []
    parent, parent_source = _parent(wikidata_parent, gleif_parent)
    if gleif_lei:
        how = "par son SIREN" if holder_has_siren else "par son nom et son pays"
        notes.append(f"LEI du titulaire trouvé {how} : {gleif_lei.identifier}")
    if gleif_parent:
        notes.append(f"GLEIF : société mère ultime {gleif_parent.name} (LEI {gleif_parent.identifier})")
    if wikidata_parent:
        label = wikidata_parent.name or "sans libellé"
        notes.append(f"Wikidata : société mère ultime {label} ({wikidata_parent.identifier})")
    if parent:
        notes.append(f"société mère ultime retenue : {parent_source}")
    if unavailable:
        notes.append(f"{', '.join(sorted(set(unavailable)))} indisponible pendant le job, société mère à rechercher")
    elif not parent:
        notes.append("aucune société mère trouvée")
    return parent, notes


def propose_row(
    brand: str,
    holder: Holder | None,
    wikidata_parent: Company | None,
    gleif_lei: Company | None,
    gleif_parent: Company | None,
    known: KnownCompanies,
    today: date,
    unavailable: list[str] | None = None,
) -> dict[str, str]:
    """Row of the tab Marques: the company (company_name, or the company already in the tab with its
    identity and ultimate parent) with its country and SIREN, the ultimate parent company (GLEIF, else
    Wikidata) with its country and SIREN, and in commentaires all that explains them. The departments of
    the head offices are only the ones already in the tab: the job looks the others up afterwards.
    Statuses: e_statut 'non vérifié', smu_statut 'à vérifier' for an ultimate parent found by the job; both
    left empty when nothing was found."""
    job = f"job {today.isoformat()} : "
    row = {"marque": brand, **identity_cells(Identity()), **identity_cells(Identity(), PARENT_COLUMNS)}
    if holder is None:
        # written anyway, so that the brand is not searched again (and its INPI quota spent) on every run
        return {
            **row, "entreprise": "", "e_statut": "", "societe_mere_ultime": "", "smu_statut": "",
            "commentaires": job + "aucune marque en vigueur à ce nom à l'INPI (bases FR, EU, WO)",
        }

    identity = f"SIREN {holder.siren}" if holder.siren else f"société étrangère, pays {holder.country or '?'}"
    notes = [f"entreprise : titulaire INPI {holder.name} ({identity}), marque {holder.trademark}"]
    if not holder.in_sector_classes:
        notes.append("aucune marque lue dans les classes de Nice du secteur, titulaire à vérifier")
    if holder.other_holders:
        notes.append("autres titulaires : " + ", ".join(holder.other_holders))

    existing = known.find(holder, brand)
    row.update(identity_cells(company_identity(holder, existing)))
    if existing and not existing.identity.siren and holder.siren:
        notes.append("SIREN de l'entreprise à compléter (celui du titulaire peut être une autre société)")
    if existing and existing.parent:
        notes.append(f"entreprise et société mère ultime reprises de la marque {existing.brand}")
        return {
            **row, "entreprise": existing.name, "e_statut": UNVERIFIED,
            "societe_mere_ultime": existing.parent,
            # the ultimate parent of this company, with its status on the other brand
            "smu_statut": existing.parent_status,
            **identity_cells(existing.parent_identity.or_else(known.parent_identity(existing.parent)), PARENT_COLUMNS),
            "commentaires": job + " ; ".join(notes),
        }

    if existing:
        notes.append(f"entreprise reprise de la marque {existing.brand}")
    parent, more_notes = parent_notes(bool(holder.siren), wikidata_parent, gleif_lei, gleif_parent, unavailable)
    return {
        **row,
        "entreprise": existing.name if existing else company_name(holder, brand),
        "e_statut": UNVERIFIED,
        "societe_mere_ultime": parent.name if parent else "",
        "smu_statut": TO_CHECK if parent else "",
        **identity_cells(
            Identity.of_company(parent).or_else(known.parent_identity(parent.name)) if parent else Identity(),
            PARENT_COLUMNS,
        ),
        "commentaires": job + " ; ".join(notes + more_notes),
    }
