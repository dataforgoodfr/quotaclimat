"""Response model for the review prompt, and applying its verdicts"""

import copy
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.content_schema import (
    OTHER_MARKERS,
    Channel,
    ClaimType,
    EnvironmentalContent,
    Impression,
    Subject,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils.schema_base import (
    MODEL_CONFIG,
    SHORT_TEXT,
)

OtherMarker = Literal[
    "environmental_label",
    "ecofriendly_material_or_technology",
    "recyclability_end_of_life",
    "natural_or_sustainable",
    "sans_x",
]
ClaimReason = Literal[
    "valid",
    "mandatory_message",  # exclusion 1
    "regulatory_display",  # exclusion 1
    "technology_or_material",  # exclusion 2
    "performance_spec",  # exclusion 3
    "price_or_promotion",  # exclusion 4
    "not_environmental",
    "no_benefit_asserted",
    "other_advertiser",
    "not_in_ad",
    "duplicate",
    "wrong_fields",
]
EvidenceReason = Literal[
    "valid",
    "wrong_marker",
    "not_environmental",
    "other_advertiser",
    "not_in_ad",
    "duplicate",
]


class ClaimFix(BaseModel):
    """Only the fields to overwrite; null leaves stage 1's value."""

    assertion_words: SHORT_TEXT | None = None
    environmental_object: SHORT_TEXT | None = None
    benefit_predicate: SHORT_TEXT | None = None
    claim_types: (
        Annotated[list[ClaimType], Field(min_length=1, max_length=4)] | None
    ) = None
    channel: Channel | None = None
    subject: Subject | None = None

    model_config = MODEL_CONFIG


class ClaimReview(BaseModel):
    id: int
    verdict: Literal["keep", "fix", "drop"]
    reason: ClaimReason
    note: SHORT_TEXT = ""
    fix: ClaimFix | None = None

    model_config = MODEL_CONFIG


class EvidenceReview(BaseModel):
    id: int
    verdict: Literal["keep", "move", "drop"]
    reason: EvidenceReason
    move_to: OtherMarker | None = None  # the right marker, when verdict is "move"
    note: SHORT_TEXT = ""

    model_config = MODEL_CONFIG


class ContentReview(BaseModel):
    claims: list[ClaimReview] = Field(default_factory=list)
    evidence: list[EvidenceReview] = Field(default_factory=list)
    impression: Impression = Field(default_factory=Impression)

    model_config = MODEL_CONFIG


def evidence_items(record: dict) -> list[tuple[str, dict]]:
    """Every I2 evidence item as (marker, item), in the order the prompt numbers them."""
    markers = record.get("I2_other_markers") or {}
    return [
        (kind, item)
        for kind in OTHER_MARKERS
        for item in (markers.get(kind) or {}).get("evidence") or []
    ]


def apply_review(record: dict, review: dict) -> dict:
    """Stage 1's record with the verdicts applied. An item without a verdict is kept."""
    out = copy.deepcopy(record)

    verdicts = {v["id"]: v for v in review.get("claims") or []}
    claims = []
    for i, claim in enumerate(out.get("I3_claims") or [], 1):
        verdict = verdicts.get(i, {})
        if verdict.get("verdict") == "drop":
            continue
        if verdict.get("verdict") == "fix":
            fix = verdict.get("fix") or {}
            claim.update({k: v for k, v in fix.items() if v is not None})
            if claim.get("environmental_object"):
                claim["object_stated"] = True
        claims.append(claim)
    out["I3_claims"] = claims

    verdicts = {v["id"]: v for v in review.get("evidence") or []}
    regrouped: dict[str, list] = {kind: [] for kind in OTHER_MARKERS}
    seen = set()
    for i, (kind, item) in enumerate(evidence_items(record), 1):
        verdict = verdicts.get(i, {})
        if verdict.get("verdict") == "drop":
            continue
        moved = verdict.get("move_to") if verdict.get("verdict") == "move" else None
        target = moved or kind
        # A move can land on an item the target marker already holds.
        key = (target, item.get("text") or item.get("note"), item.get("channel"))
        if key in seen:
            continue
        seen.add(key)
        regrouped[target].append(copy.deepcopy(item))
    markers = out.setdefault("I2_other_markers", {})
    for kind, items in regrouped.items():
        marker = markers.setdefault(kind, {})
        marker["evidence"] = items
        marker["present"] = bool(items)
        if kind == "recyclability_end_of_life" and not items:
            marker["applies_to"] = None

    if review.get("impression"):
        out["I4_overall_impression"] = review["impression"]

    # Stage 1's coercions again, as a fix can leave flags inconsistent.
    return EnvironmentalContent.model_validate(out).model_dump(mode="json")
