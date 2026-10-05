"""Group proposal for a brand: INPI trademark holder (SIREN), then its parent company (Wikidata, GLEIF).

The proposal is a row of the tab Marques with statut 'non vérifié': a human checks it afterwards.
"""

from collections import Counter
from dataclasses import dataclass
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
    """The holder of the most trademarks covering the sector's Nice classes (all trademarks when the
    sector has no classes), most recent trademark first on a tie."""
    relevant = [n for n in notices if not sector_classes or n.classes & sector_classes]
    if not relevant:
        return None
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
    )


def propose_row(
    brand: str,
    holder: Holder | None,
    wikidata_parent: Company | None,
    gleif_lei: Company | None,
    gleif_parent: Company | None,
    known_groups: dict[str, str],
    today: date,
) -> dict[str, str]:
    """Row of the tab Marques. The group is, in this order: a candidate already in the tab Groupes
    (its canonical name), the Wikidata parent, the GLEIF direct parent, the holder itself."""
    if holder is None:
        return {
            "marque": brand,
            "groupe": "",
            "source": "inpi",
            "statut": "non vérifié",
            "commentaire": f"job {today.isoformat()} : aucune marque française en vigueur à ce nom (INPI)",
        }

    candidates = [
        (wikidata_parent.name, "wikidata") if wikidata_parent else None,
        (gleif_parent.name, "gleif") if gleif_parent else None,
        (holder.name, "inpi"),
    ]
    candidates = [c for c in candidates if c]
    known = next(
        (
            (known_groups[name_key(n)], s)
            for n, s in candidates
            if name_key(n) in known_groups
        ),
        None,
    )
    group, source = known or candidates[0]

    identity = f"SIREN {holder.siren}" if holder.siren else f"société étrangère, pays {holder.country or '?'}"
    notes = [f"titulaire {holder.name} ({identity}), marque FR{holder.application_number}"]
    if gleif_lei and not holder.siren:
        notes.append(f"LEI du titulaire trouvé par son nom : {gleif_lei.identifier}")
    if wikidata_parent:
        notes.append(
            f"Wikidata : {wikidata_parent.name} ({wikidata_parent.identifier})"
        )
    if gleif_parent:
        notes.append(f"GLEIF : {gleif_parent.name} (LEI {gleif_parent.identifier})")
    if source == "inpi":
        notes.append("groupe = titulaire, aucune société mère trouvée")
    if holder.other_holders:
        notes.append("autres titulaires : " + ", ".join(holder.other_holders))
    return {
        "marque": brand,
        "groupe": group,
        # the holder always comes from the INPI
        "source": source if source == "inpi" else f"inpi+{source}",
        "statut": "non vérifié",
        "commentaire": f"job {today.isoformat()} : " + " ; ".join(notes),
        # traceability, written only when the tab has these columns
        "titulaire": holder.name,
        "siren": holder.siren,
        "lei": gleif_lei.identifier if gleif_lei else "",
        "numero_marque": f"FR{holder.application_number}",
    }
