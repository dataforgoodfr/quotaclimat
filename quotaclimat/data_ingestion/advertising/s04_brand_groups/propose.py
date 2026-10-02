"""Group proposal for a brand: INPI trademark holder (SIREN), then its parent company (Wikidata, GLEIF).

The proposal is a row of the tab Marques with statut 'non vérifié': a human checks it afterwards.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import date

from quotaclimat.data_ingestion.advertising.s04_brand_groups.inpi import Notice, name_key
from quotaclimat.data_ingestion.advertising.s04_brand_groups.registries import Company


@dataclass
class Holder:
    """Trademark holder chosen for a brand, among the notices of its trademarks."""
    name: str
    siren: str
    application_number: str
    other_holders: list[str]


def parse_nice_classes(rows: list[dict[str, str]]) -> dict[str, set[int]]:
    """Tab Classes_Nice: sector_code -> Nice classes ("3; 35")."""
    out = {}
    for row in rows:
        code = row.get("sector_code", "").strip()
        classes = {int(c) for c in row.get("classes", "").replace(",", ";").split(";") if c.strip().isdigit()}
        if code and classes:
            out[code] = classes
    return out


def choose_holder(notices: list[Notice], sector_classes: set[int] | None) -> Holder | None:
    """The holder of the most trademarks covering the sector's Nice classes (all trademarks when the
    sector has no classes), most recent trademark first on a tie."""
    relevant = [n for n in notices if not sector_classes or n.classes & sector_classes]
    if not relevant:
        return None
    counts = Counter(n.holder_siren for n in relevant)
    # Counter keeps the first-seen order on ties, notices are the most recent first
    siren, _ = counts.most_common(1)[0]
    chosen = next(n for n in relevant if n.holder_siren == siren)
    others = sorted({f"{n.holder_name} ({n.holder_siren})" for n in relevant if n.holder_siren != siren})
    return Holder(name=chosen.holder_name or siren, siren=siren, application_number=chosen.application_number, other_holders=others)


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
            "marque": brand, "groupe": "", "source": "inpi", "statut": "non vérifié",
            "commentaire": f"job {today.isoformat()} : aucune marque française en vigueur à ce nom (INPI)",
        }

    candidates = [
        (wikidata_parent.name, "wikidata") if wikidata_parent else None,
        (gleif_parent.name, "gleif") if gleif_parent else None,
        (holder.name, "inpi"),
    ]
    candidates = [c for c in candidates if c]
    known = next(((known_groups[name_key(n)], s) for n, s in candidates if name_key(n) in known_groups), None)
    group, source = known or candidates[0]

    notes = [f"titulaire {holder.name} (SIREN {holder.siren}), marque FR{holder.application_number}"]
    if wikidata_parent:
        notes.append(f"Wikidata : {wikidata_parent.name} ({wikidata_parent.identifier})")
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
