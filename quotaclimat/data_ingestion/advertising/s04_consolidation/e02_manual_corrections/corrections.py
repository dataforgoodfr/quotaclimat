"""Database operations of the manual corrections: they only add to the session, the caller commits.

- correct_ad: brand, sector, category or content type of an ad;
- trash_occurrence: an occurrence that is not an ad, soft deleted (deleted_at);
- create_ad_from_occurrences: a new ad for occurrences attributed to the wrong ad;
- reattribute_occurrence: an occurrence attributed to the wrong ad, moved to an existing ad;
- merge_ad_into: every occurrence of an ad moved to another existing ad (the same spot stored twice), the
  ad being soft deleted (deleted_at: kept, but left out of every step).

Each one returns an Outcome: the result and the messages written in the row of the spreadsheet. An error
changes nothing, a warning (inconsistency with the reference data) does not stop the correction.
"""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import get_args

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Occurrence,
)
from quotaclimat.data_ingestion.advertising.s03_classification.ad_classification_schema import (
    ContentType,
)
from quotaclimat.data_ingestion.advertising.s03_classification.prediction_status import (
    AD_CONTENT_TYPE,
    FINAL_PREDICTION_STATUSES,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.references import (
    References,
    brand_key,
)
from quotaclimat.data_ingestion.advertising.tools.ad_merge import merged_occurrence_id

RESULT_APPLIED = "appliqué"
RESULT_WARNING = "avertissement"
RESULT_ERROR = "erreur"
# prediction_method of an ad corrected by hand
MANUAL_METHOD = "manual"
# entry of advertising.ad.prediction keeping the values before the correction
MANUAL_STAGE = "manual_correction"
# entry of advertising.ad.prediction of an ad merged by hand into another (soft deleted)
MANUAL_FUSION_STAGE = "manual_fusion"
CONTENT_TYPES = get_args(ContentType)
# input status of the dictionary stage (s03_classification/pipeline/e03_dict_match.py)
DICT_MATCH_INPUT_STATUS = "ad_type_done"
# fragment_type of a new ad, the same as the detection: exported and classified by the next steps
NEW_AD_FRAGMENT_TYPE = "advertising"
# occurrences of ads of durations further apart than this get a warning when they move to one ad
MAX_DURATION_SPREAD_SEC = 1.0
# the video of the ad is missing or unreadable: a classification needing it will fail
MISSING_VIDEO_STATUSES = ("no_data", "video_decode_error")
PREVIOUS_COLUMNS = (
    "fragment_type",
    "prediction_status",
    "prediction_method",
    "predicted_brand",
    "predicted_sector",
    "predicted_product_category",
)


@dataclass
class Outcome:
    result: str = RESULT_APPLIED
    messages: list[str] = field(default_factory=list)

    def warn(self, message: str) -> None:
        self.messages.append(message)
        if self.result == RESULT_APPLIED:
            self.result = RESULT_WARNING

    @classmethod
    def error(cls, message: str) -> "Outcome":
        return cls(result=RESULT_ERROR, messages=[message])


@dataclass
class AdCorrection:
    """Values of a row of a correction tab, None when empty (unchanged)."""

    brand: str | None = None
    sector: str | None = None
    category: str | None = None
    content_type: str | None = None

    def given(self) -> dict[str, str]:
        return {
            name: value
            for name, value in (
                ("marque", self.brand),
                ("secteur", self.sector),
                ("categorie", self.category),
                ("type_contenu", self.content_type),
            )
            if value
        }


def _utcnow() -> datetime:
    # dates are stored as naive UTC timestamps
    return datetime.now(UTC).replace(tzinfo=None)


def previous_values(ad: Ad) -> dict[str, str | None]:
    return {column: getattr(ad, column) for column in PREVIOUS_COLUMNS}


def _ad_type_entry(prediction: list[dict]) -> dict | None:
    return next((e for e in prediction if e.get("stage") == "ad_type"), None)


def _check_references(
    correction: AdCorrection, ad: Ad, references: References, outcome: Outcome
) -> None:
    """Warnings when the corrected values do not match the reference data."""
    sector = correction.sector or ad.predicted_sector
    if correction.category and references.category_sector:
        category_sector = references.category_sector.get(correction.category)
        if sector and category_sector != sector:
            outcome.warn(
                f"la catégorie {correction.category} est du secteur {category_sector}, pas de {sector}"
            )
    brand = correction.brand or ad.predicted_brand
    if not brand:
        return
    dictionary_sectors = references.dictionary_sectors.get(brand_key(brand))
    if dictionary_sectors and sector and sector not in dictionary_sectors:
        outcome.warn(
            f"le dictionnaire classe {brand} dans {', '.join(sorted(dictionary_sectors))}, pas dans {sector}"
        )
    if (
        correction.brand
        and references.known_brands
        and brand_key(correction.brand) not in references.known_brands
    ):
        similar = references.similar_brands(correction.brand)
        outcome.warn(
            f"marque {correction.brand} inconnue (dictionnaire, inventaire, marques prédites)"
            + (f", proches : {', '.join(similar)}" if similar else "")
        )


def _validation_error(correction: AdCorrection, references: References) -> str | None:
    if not correction.given():
        return "rien à corriger : marque, secteur, categorie et type_contenu vides"
    if correction.content_type and correction.content_type not in CONTENT_TYPES:
        return f"type_contenu {correction.content_type} inconnu, valeurs possibles : {', '.join(CONTENT_TYPES)}"
    if (
        correction.sector
        and references.sectors
        and correction.sector not in references.sectors
    ):
        return (
            f"secteur {correction.sector} inconnu (codes de ref_ome_secteurs, ex. FOD)"
        )
    if (
        correction.category
        and references.category_sector
        and correction.category not in references.category_sector
    ):
        return f"catégorie {correction.category} inconnue (codes de ref_ome_categories, ex. FOD2)"
    return None


def correct_ad(
    session: Session,
    ad_id: str,
    correction: AdCorrection,
    references: References,
    source: dict,
) -> tuple[Outcome, dict[str, str | None] | None]:
    """Correct an ad. Returns the outcome and the values before the correction (None on an error).

    - an advertising (content type AD) already classified (final status): the given values replace the
      predicted ones, prediction_method becomes "manual";
    - an advertising not classified (another content type corrected to AD, no sector found...): it goes back
      to the classification at the dictionary stage (status ad_type_done) with the corrected brand and sector
      in its ad_type prediction; the dictionary, then the sub-category stage, give its sector and category;
    - another content type (BUMPER, OTHER...): the ad becomes like a content the classification found not to
      be an advertising: content type in fragment_type and in its ad_type prediction, status ad_type_done (the
      dictionary stage skips it), no brand, sector nor category. It leaves the analyses, even the queries
      reading advertising.ad without filtering on fragment_type, and the screenshots of new occurrences.
      Corrected back to AD, it goes back to the classification (case above).
    The values before the correction are kept in advertising.ad.prediction (stage "manual_correction").
    """
    error = _validation_error(correction, references)
    if error:
        return Outcome.error(error), None
    ad = session.get(Ad, ad_id)
    if ad is None:
        return Outcome.error(f"ad {ad_id} inconnue"), None
    if deleted_error(ad):
        return Outcome.error(deleted_error(ad)), None
    if not correction.content_type and ad.fragment_type not in CONTENT_TYPES:
        # not classified yet (fragment_type of the detection: advertising, jingle, no_data)
        return (
            Outcome.error(
                f"type de contenu de l'ad inconnu ({ad.fragment_type}) : renseigner type_contenu"
            ),
            None,
        )

    outcome = Outcome()
    previous = previous_values(ad)
    content_type = correction.content_type or ad.fragment_type
    prediction = list(ad.prediction or [])

    if content_type != AD_CONTENT_TYPE:
        if correction.brand or correction.sector or correction.category:
            outcome.warn(
                f"marque, secteur et catégorie ignorés : le contenu n'est pas une publicité ({AD_CONTENT_TYPE})"
            )
        entry = _ad_type_entry(prediction)
        if entry is None:
            entry = {"stage": "ad_type", "model": MANUAL_METHOD, "raw_response": {}}
            prediction.insert(0, entry)
        # the brand_name of the classification stays: the dictionary uses it if the ad is corrected back to AD
        entry["raw_response"] = {
            **(entry.get("raw_response") or {}),
            "content_type": content_type,
        }
        ad.fragment_type = content_type
        ad.prediction_status = DICT_MATCH_INPUT_STATUS
        ad.predicted_brand = None
        ad.predicted_sector = None
        ad.predicted_product_category = None
        ad.prediction_method = MANUAL_METHOD
    elif ad.prediction_status in FINAL_PREDICTION_STATUSES:
        _check_references(correction, ad, references, outcome)
        ad.fragment_type = AD_CONTENT_TYPE
        if correction.brand:
            ad.predicted_brand = correction.brand
        if correction.sector:
            ad.predicted_sector = correction.sector
        if correction.category:
            ad.predicted_product_category = correction.category
        ad.prediction_method = MANUAL_METHOD
    else:
        _check_references(correction, ad, references, outcome)
        entry = _ad_type_entry(prediction)
        if entry is None:
            entry = {"stage": "ad_type", "model": MANUAL_METHOD, "raw_response": {}}
            prediction.insert(0, entry)
        raw = dict(entry.get("raw_response") or {})
        raw["content_type"] = AD_CONTENT_TYPE
        if correction.brand:
            raw["brand_name"] = correction.brand
        if correction.sector:
            raw["sector_code"] = correction.sector
        entry["raw_response"] = raw
        if correction.category:
            outcome.warn(
                "catégorie ignorée : l'ad repasse par la classification, qui la choisit"
            )
        has_dictionary_brand = (
            brand_key(raw.get("brand_name")) in references.dictionary_sectors
        )
        if not raw.get("sector_code") and not has_dictionary_brand:
            outcome.warn(
                "ni secteur ni marque du dictionnaire : l'étape des sous-catégories n'aura pas de secteur"
            )
        if (
            ad.fragment_type == "no_data"
            or ad.prediction_status in MISSING_VIDEO_STATUSES
        ):
            outcome.warn(
                "vidéo de l'ad manquante ou illisible : la classification risque d'échouer"
            )
        ad.fragment_type = AD_CONTENT_TYPE
        ad.prediction_status = DICT_MATCH_INPUT_STATUS
        ad.predicted_sector = raw.get("sector_code")
        ad.predicted_brand = None
        ad.predicted_product_category = None
        ad.prediction_method = None
        outcome.messages.append("l'ad repasse par la classification (dictionnaire)")

    prediction.append(
        {
            "stage": MANUAL_STAGE,
            "at": _utcnow().isoformat(timespec="seconds"),
            "source": source,
            "previous": previous,
            "corrected": correction.given(),
        }
    )
    ad.prediction = prediction
    session.flush()
    return outcome, previous


def trash_occurrence(session: Session, occurrence_id: str) -> Outcome:
    """An occurrence that is not an ad leaves the analyses: soft deleted, like the occurrences replaced by a
    merge (every reader of advertising.ad_occurrence ignores the rows with a deleted_at)."""
    occurrence = session.get(Ad_Occurrence, occurrence_id)
    if occurrence is None:
        return Outcome.error(f"occurrence {occurrence_id} inconnue")
    if occurrence.deleted_at is not None:
        outcome = Outcome()
        outcome.warn(f"occurrence déjà supprimée le {occurrence.deleted_at:%Y-%m-%d}")
        return outcome
    occurrence.deleted_at = _utcnow()
    session.flush()
    return Outcome()


def new_ad_id(spreadsheet: str, tab: str, ad_indicator: str) -> str:
    """Stable id of the ad created from a tab (and an ad_indicator): rows added later go to the same ad."""
    raw = f"manual_ad|{spreadsheet}|{tab}|{ad_indicator}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def live_occurrences(
    session: Session, occurrence_ids: Sequence[str]
) -> tuple[dict[str, Ad_Occurrence], dict[str, Outcome]]:
    """Occurrences that can move to a new ad, and the errors of the others."""
    occurrences = {
        occ.id: occ
        for occ in session.scalars(
            select(Ad_Occurrence).where(Ad_Occurrence.id.in_(list(occurrence_ids)))
        ).all()
    }
    errors = {}
    for occurrence_id in occurrence_ids:
        occurrence = occurrences.get(occurrence_id)
        if occurrence is None:
            errors[occurrence_id] = Outcome.error(
                f"occurrence {occurrence_id} inconnue"
            )
        elif occurrence.deleted_at is not None:
            errors[occurrence_id] = Outcome.error(
                f"occurrence supprimée le {occurrence.deleted_at:%Y-%m-%d}"
            )
    return {
        occurrence_id: occurrence
        for occurrence_id, occurrence in occurrences.items()
        if occurrence_id not in errors
    }, errors


def create_ad_from_occurrences(
    session: Session,
    ad_id: str,
    occurrences: Sequence[Ad_Occurrence],
    duration_sec: float,
) -> dict[str, str]:
    """Create the ad (when it does not exist) and move the occurrences to it.

    The ad has no fingerprint and no prediction: the classification processes it like a new ad, the
    detection never attributes broadcasts to it. Returns {old occurrence id: new id} (see move_occurrences).
    """
    if session.get(Ad, ad_id) is None:
        session.add(
            Ad(
                id=ad_id,
                first_detection_date=min(occ.occurrence_date for occ in occurrences),
                duration_sec=duration_sec,
                chunks=[],
                fragment_type=NEW_AD_FRAGMENT_TYPE,
            )
        )
        session.flush()
    return move_occurrences(session, ad_id, occurrences)


def move_occurrences(
    session: Session, ad_id: str, occurrences: Sequence[Ad_Occurrence]
) -> dict[str, str]:
    """Move the occurrences to the existing ad ad_id: each one is replaced by an occurrence of the ad (id of
    the detection formula), the old one is soft deleted. Returns {old occurrence id: new id}."""
    now = _utcnow()
    new_ids = {}
    for occurrence in occurrences:
        new_id = merged_occurrence_id(
            ad_id, occurrence.occurrence_date, occurrence.channel_name
        )
        session.execute(
            insert(Ad_Occurrence)
            .values(
                id=new_id,
                occurrence_date=occurrence.occurrence_date,
                channel_name=occurrence.channel_name,
                ad_id=ad_id,
            )
            .on_conflict_do_update(
                index_elements=[Ad_Occurrence.id], set_={"deleted_at": None}
            )
        )
        new_ids[occurrence.id] = new_id
    session.execute(
        update(Ad_Occurrence)
        .where(Ad_Occurrence.id.in_(list(new_ids)))
        .values(deleted_at=now)
    )
    session.flush()
    return new_ids


def _target_warnings(outcome: Outcome, source_ad: Ad | None, ad: Ad) -> None:
    """Warnings when occurrences of source_ad move to the ad: another duration, not a classified advertising."""
    if (
        source_ad is not None
        and abs(source_ad.duration_sec - ad.duration_sec) > MAX_DURATION_SPREAD_SEC
    ):
        outcome.warn(
            f"durée de l'ad ({ad.duration_sec:g}s) différente de celle de l'ad d'origine"
            f" ({source_ad.duration_sec:g}s)"
        )
    if (
        ad.fragment_type != AD_CONTENT_TYPE
        or ad.prediction_status not in FINAL_PREDICTION_STATUSES
    ):
        outcome.warn(
            "l'ad n'est pas une publicité classée : ses occurrences n'apparaissent pas dans les analyses"
        )


def reattribute_occurrence(
    session: Session, occurrence_id: str, ad_id: str
) -> tuple[Outcome, str | None]:
    """Move an occurrence attributed to the wrong ad to the existing ad ad_id.

    Warnings: an ad of another duration than the ad of the occurrence, an ad that is not a classified
    advertising (the occurrence is then left out of the analyses). Returns the outcome and the id of the new
    occurrence (None on an error).
    """
    if not ad_id:
        return Outcome.error("ad_id vide"), None
    ad = session.get(Ad, ad_id)
    if ad is None:
        return Outcome.error(f"ad {ad_id} inconnue"), None
    if deleted_error(ad):
        return Outcome.error(deleted_error(ad)), None
    occurrence = session.get(Ad_Occurrence, occurrence_id)
    if occurrence is None:
        return Outcome.error(f"occurrence {occurrence_id} inconnue"), None
    if occurrence.deleted_at is not None:
        return (
            Outcome.error(f"occurrence supprimée le {occurrence.deleted_at:%Y-%m-%d}"),
            None,
        )
    if occurrence.ad_id == ad_id:
        return Outcome.error("occurrence déjà dans cette ad"), None

    outcome = Outcome()
    source_ad = session.get(Ad, occurrence.ad_id) if occurrence.ad_id else None
    _target_warnings(outcome, source_ad, ad)
    new_id = move_occurrences(session, ad_id, [occurrence])[occurrence_id]
    return outcome, new_id


def deleted_error(ad: Ad, role: str = "ad") -> str | None:
    """Error message for a soft deleted ad (merged by hand into another), None for a live ad."""
    if ad.deleted_at is None:
        return None
    return f"{role} supprimée le {ad.deleted_at:%Y-%m-%d}, fusionnée dans {ad.replaced_by_ad_id}"


def copy_fingerprints(source_ad: Ad, target_ad: Ad, outcome: Outcome) -> int:
    """Copy the chunk entries (fingerprints) of source_ad to target_ad, so that the detection matches the spot
    with both: another fingerprinter configuration (hash), or the same one on a slightly different recording.
    Each copied entry keeps its hash and fingerprints, with "from_ad_id". An entry already in the target and an
    entry without fingerprints are left. Not done when the durations differ by more than
    MAX_DURATION_SPREAD_SEC (warning): the detection gives a matched broadcast the duration of the ad, the
    fingerprints of another duration would cut it wrong. Returns the number of entries copied.
    """
    if abs(source_ad.duration_sec - target_ad.duration_sec) > MAX_DURATION_SPREAD_SEC:
        outcome.warn(
            "empreintes non copiées : durées des deux ads différentes"
            f" ({source_ad.duration_sec:g}s et {target_ad.duration_sec:g}s)"
        )
        return 0
    chunks = list(target_ad.chunks or [])
    known = [(e.get("hash"), e.get("fingerprints")) for e in chunks]
    copied = 0
    for entry in source_ad.chunks or []:
        if not entry.get("fingerprints"):
            continue
        if (entry.get("hash"), entry.get("fingerprints")) in known:
            continue
        chunks.append({**entry, "from_ad_id": source_ad.id})
        known.append((entry.get("hash"), entry.get("fingerprints")))
        copied += 1
    if copied:
        target_ad.chunks = chunks
    return copied


def merge_ad_into(
    session: Session, ad_id: str, target_ad_id: str, source: dict
) -> tuple[Outcome, int]:
    """Merge the ad ad_id into the existing ad target_ad_id (the same spot stored as two ads, the data of ad_id
    being wrong).

    1. Every live occurrence of ad_id is moved to target_ad_id like a reattribution: replaced by an occurrence
       of the target ad (id of the detection formula), the old one soft deleted.
    2. Its fingerprints (chunk entries) are copied to target_ad_id (copy_fingerprints): the detection matches the
       spot with both.
    3. The ad ad_id is soft deleted: deleted_at and replaced_by_ad_id are set, and an entry
       {"stage": "manual_fusion", ...} is added to its prediction. It stays in the database with its old
       occurrences (history), but every step leaves it out: the detection no longer matches its fingerprints.

    The same id in both columns changes nothing (applied). Errors: empty target, unknown or deleted ad, an ad of
    a merge of split pieces (advertising.ad_merge, redone on its pieces after a reprocessing of the detection).
    Warnings: the warnings of a reattribution. Returns the outcome and the number of occurrences moved.
    """
    if not target_ad_id:
        return Outcome.error("target_ad_id vide"), 0
    if ad_id == target_ad_id:
        outcome = Outcome()
        outcome.messages.append("ad_id et target_ad_id identiques : rien à faire")
        return outcome, 0
    source_ad = session.get(Ad, ad_id)
    if source_ad is None:
        return Outcome.error(f"ad {ad_id} inconnue"), 0
    target_ad = session.get(Ad, target_ad_id)
    if target_ad is None:
        return Outcome.error(f"ad cible {target_ad_id} inconnue"), 0
    error = deleted_error(source_ad) or deleted_error(target_ad, "ad cible")
    if error:
        return Outcome.error(error), 0
    in_merge = session.scalar(
        select(Ad_Merge.merged_ad_id)
        .where(
            or_(
                Ad_Merge.merged_ad_id == ad_id,
                Ad_Merge.source_ad_ids.any(ad_id),
            )
        )
        .limit(1)
    )
    if in_merge is not None:
        return (
            Outcome.error(
                f"ad liée à la fusion de morceaux {in_merge} (advertising.ad_merge) : à traiter à la main"
            ),
            0,
        )

    occurrences = session.scalars(
        select(Ad_Occurrence).where(
            Ad_Occurrence.ad_id == ad_id, Ad_Occurrence.deleted_at.is_(None)
        )
    ).all()
    outcome = Outcome()
    if occurrences:
        _target_warnings(outcome, source_ad, target_ad)
        move_occurrences(session, target_ad_id, occurrences)
    nb_copied = copy_fingerprints(source_ad, target_ad, outcome)
    now = _utcnow()
    source_ad.deleted_at = now
    source_ad.replaced_by_ad_id = target_ad_id
    source_ad.prediction = list(source_ad.prediction or []) + [
        {
            "stage": MANUAL_FUSION_STAGE,
            "at": now.isoformat(timespec="seconds"),
            "source": source,
            "replaced_by": target_ad_id,
            "nb_occurrences": len(occurrences),
            "nb_fingerprint_entries_copied": nb_copied,
        }
    ]
    session.flush()
    outcome.messages.append(
        f"{len(occurrences)} occurrences déplacées, {nb_copied} empreintes copiées,"
        " ad supprimée (deleted_at)"
    )
    return outcome, len(occurrences)
