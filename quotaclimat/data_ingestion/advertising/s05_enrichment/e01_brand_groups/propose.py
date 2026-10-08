"""Company proposal for a brand: the INPI trademark holder (SIREN), the company that sells under the
brand (Free -> FREE), never a parent company. The ultimate parent company (GLEIF, Wikidata) is only
proposed in the new row of the tab Entreprises.

The proposals have statut 'non vérifié': a human checks them afterwards, and corrects the company when
the holder is a holding (Auchan -> ELO), e.g. with an alias in the tab Entreprises.
"""

from collections import Counter
from dataclasses import dataclass, field
from datetime import date

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    Notice,
    name_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.registries import (
    Company,
)


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


@dataclass
class KnownCompanies:
    """Companies of the tab Entreprises (and the ones proposed during the run): official name by name_key
    and by SIREN (optional column siren)."""

    by_key: dict[str, str] = field(default_factory=dict)
    by_siren: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows: list[dict[str, str]]) -> "KnownCompanies":
        known = cls()
        for row in rows:
            if row.get("entreprise"):
                known.add(row["entreprise"], row.get("siren", ""))
        return known

    def add(self, name: str, siren: str = "") -> None:
        self.by_key.setdefault(name_key(name), name)
        if siren:
            self.by_siren.setdefault(siren.replace(" ", ""), name)

    def find(self, holder: Holder) -> str | None:
        """Official name of the holder's company in the tab, by SIREN, else by name."""
        return (holder.siren and self.by_siren.get(holder.siren)) or self.by_key.get(name_key(holder.name))


def _parent(wikidata_parent: Company | None, gleif_parent: Company | None) -> tuple[Company | None, str]:
    """Ultimate parent company found and its source: GLEIF, whose ultimate parent is the consolidating one
    (the definition of the tab Entreprises), else Wikidata (top of the parent organizations, when it has a
    label)."""
    if gleif_parent:
        return gleif_parent, "gleif"
    if wikidata_parent and wikidata_parent.name:
        return wikidata_parent, "wikidata"
    return None, ""


def propose_row(
    brand: str,
    holder: Holder | None,
    wikidata_parent: Company | None,
    gleif_lei: Company | None,
    gleif_parent: Company | None,
    known: KnownCompanies,
    today: date,
) -> tuple[dict[str, str], dict[str, str] | None]:
    """Row of the tab Marques, and row of the tab Entreprises when the company is not in it yet. The
    company is the holder, under its official name in the tab Entreprises when it is there. The ultimate
    parent company (GLEIF, else Wikidata) is only proposed in the row of the new company."""
    job = f"job {today.isoformat()} : "
    if holder is None:
        return {
            "marque": brand,
            "entreprise": "",
            "source": "inpi",
            "statut": "non vérifié",
            "commentaire": job + "aucune marque en vigueur à ce nom à l'INPI (bases FR, EU, WO)",
        }, None

    known_name = known.find(holder)
    company = known_name or holder.name
    identity = f"SIREN {holder.siren}" if holder.siren else f"société étrangère, pays {holder.country or '?'}"
    notes = [f"titulaire {holder.name} ({identity}), marque {holder.trademark}"]
    if not holder.in_sector_classes:
        notes.append("aucune marque lue dans les classes de Nice du secteur, titulaire à vérifier")
    if known_name:
        notes.append("entreprise déjà dans l'onglet Entreprises")
    if holder.other_holders:
        notes.append("autres titulaires : " + ", ".join(holder.other_holders))
    brand_row = {
        "marque": brand,
        "entreprise": company,
        "source": "inpi",
        "statut": "non vérifié",
        "commentaire": job + " ; ".join(notes),
        # traceability, written only when the tab has these columns
        "titulaire": holder.name,
        "siren": holder.siren,
        "lei": gleif_lei.identifier if gleif_lei else "",
        "numero_marque": holder.trademark,
    }
    if known_name:
        return brand_row, None

    parent, parent_source = _parent(wikidata_parent, gleif_parent)
    notes = [f"titulaire de la marque {brand} ({holder.trademark}), {identity}"]
    if gleif_lei and not holder.siren:
        notes.append(f"LEI trouvé par son nom : {gleif_lei.identifier}")
    if gleif_parent:
        notes.append(f"GLEIF : société mère ultime {gleif_parent.name} (LEI {gleif_parent.identifier})")
    if wikidata_parent:
        label = wikidata_parent.name or "sans libellé"
        notes.append(f"Wikidata : société mère ultime {label} ({wikidata_parent.identifier})")
    if not parent:
        notes.append("aucune société mère trouvée")
    company_row = {
        "entreprise": company,
        "alias": "",
        "entreprise_id": gleif_lei.identifier if gleif_lei else "",
        "siren": holder.siren,
        "societe_mere_ultime": parent.name if parent else "",
        "source": f"inpi+{parent_source}" if parent else "inpi",
        "statut": "non vérifié",
        "commentaire": job + " ; ".join(notes),
    }
    return brand_row, company_row
