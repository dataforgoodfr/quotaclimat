"""Response model for the environmental content prompt"""

from typing import Annotated, Literal, get_args

from pydantic import BaseModel, BeforeValidator, Field, model_validator

from quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.exclusions import (
    exclusion_reason,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils.schema_base import (
    MODEL_CONFIG,
    SHORT_TEXT,
    blank_to,
    note,
)

Channel = Literal["transcript", "superimposed", "fine_print", "on_product", "in_scene"]
Subject = Literal["product", "packaging", "service", "brand_or_advertiser"]
Confidence = Literal["high", "medium", "low"]
ClaimType = Literal["generic", "specified", "future", "neutrality_claim", "offsetting"]
AppliesTo = Literal["product", "packaging", "both"]
ImpressionStrength = Literal["none", "weak", "moderate", "strong"]
NatureMarkerKind = Literal[
    "wild_animal_subject",
    "animal_addresses_viewer",
    "growth_or_planting",
    "hands_with_soil_or_plant",
    "earth_from_space",
    "nature_responds_to_product",
    "nature_reclaims_built",
    "child_with_landscape",
    "nature_damage_shown",
]

CLAIM_TYPES: tuple[str, ...] = get_args(ClaimType)
OTHER_MARKERS: tuple[str, ...] = (
    "environmental_label",
    "ecofriendly_material_or_technology",
    "recyclability_end_of_life",
    "natural_or_sustainable",
    "sans_x",
)

SHOT_COUNT = Annotated[int, Field(ge=0)]


# --------------------------------------------------------------------------
# I1. Setting and nature markers


class NatureMarker(BaseModel):
    kind: NatureMarkerKind
    note: SHORT_TEXT = ""  # the shot that makes the marker checkable

    model_config = MODEL_CONFIG

    @model_validator(mode="after")
    def _check_note(self):
        if not self.note:
            note("nature_marker_without_note")
        return self


class Setting(BaseModel):
    n_shots_total: SHOT_COUNT = 0
    n_shots_natural_background: SHOT_COUNT = 0
    n_shots_built_background: SHOT_COUNT = 0
    n_shots_indeterminate: SHOT_COUNT = 0
    setting_notes: Annotated[list[SHORT_TEXT], Field(max_length=3)] = Field(
        default_factory=list
    )
    nature_markers: Annotated[list[NatureMarker], Field(max_length=9)] = Field(
        default_factory=list
    )
    green_dominant_in_typography: bool = False
    green_dominant_in_graphic_elements: bool = False
    green_dominant_in_product_or_packaging: bool = False

    model_config = MODEL_CONFIG

    @model_validator(mode="after")
    def _check_setting(self):
        parts = (
            self.n_shots_natural_background
            + self.n_shots_built_background
            + self.n_shots_indeterminate
        )
        if parts != self.n_shots_total:
            note("shot_counts_do_not_sum")
        # One entry per marker kind: a duplicate is the same marker recorded twice.
        seen: set[str] = set()
        unique = []
        for marker in self.nature_markers:
            if marker.kind in seen:
                note("duplicate_nature_marker_kind")
                continue
            seen.add(marker.kind)
            unique.append(marker)
        self.nature_markers = unique
        return self


# --------------------------------------------------------------------------
# I2. Other environmental markers


class Evidence(BaseModel):
    channel: Channel
    text: SHORT_TEXT = ""  # verbatim French, "" when there is no legible text
    legible: bool = True
    note: SHORT_TEXT = ""  # the shot and the element, in English

    model_config = MODEL_CONFIG

    @model_validator(mode="after")
    def _check_evidence(self):
        if not self.text and not self.note:
            note("evidence_without_text_or_note")
        return self


class Marker(BaseModel):
    present: bool = False
    evidence: Annotated[list[Evidence], Field(max_length=20)] = Field(
        default_factory=list
    )

    model_config = MODEL_CONFIG

    @model_validator(mode="after")
    def _check_marker(self):
        # The evidence is the record, the flag the summary: the evidence wins.
        if self.evidence and not self.present:
            note("evidence_without_present")
            self.present = True
        if self.present and not self.evidence:
            note("present_without_evidence")
        return self


class RecyclabilityMarker(Marker):
    applies_to: AppliesTo | None = None  # null when not present or not stated

    @model_validator(mode="after")
    def _check_applies_to(self):
        if self.applies_to and not self.present and not self.evidence:
            note("applies_to_without_present")
            self.applies_to = None
        return self


class OtherMarkers(BaseModel):
    environmental_label: Marker = Field(default_factory=Marker)
    ecofriendly_material_or_technology: Marker = Field(default_factory=Marker)
    recyclability_end_of_life: RecyclabilityMarker = Field(
        default_factory=RecyclabilityMarker
    )
    natural_or_sustainable: Marker = Field(default_factory=Marker)
    sans_x: Marker = Field(default_factory=Marker)

    model_config = MODEL_CONFIG

    @property
    def present_kinds(self) -> list[str]:
        return [kind for kind in OTHER_MARKERS if getattr(self, kind).present]


# --------------------------------------------------------------------------
# I3. Claims


class Claim(BaseModel):
    quote: SHORT_TEXT
    assertion_words: SHORT_TEXT = ""  # minimal span of the quote carrying the assertion
    object_stated: bool = False
    environmental_object: SHORT_TEXT | None = None  # set if and only if object_stated
    benefit_predicate: SHORT_TEXT = ""
    # generic or specified, then any of future, neutrality_claim, offsetting
    claim_types: Annotated[list[ClaimType], Field(min_length=1, max_length=4)]
    channel: Channel
    subject: Subject
    confidence: Confidence

    model_config = MODEL_CONFIG

    @model_validator(mode="after")
    def _check_claim(self):
        if self.assertion_words and self.assertion_words not in self.quote:
            note("assertion_words_not_substring")

        # The object text is the evidence, the flag the summary.
        if self.environmental_object and not self.object_stated:
            note("object_without_object_stated")
            self.object_stated = True
        if self.object_stated and not self.environmental_object:
            note("object_stated_without_object")
            self.object_stated = False

        # One entry per type: a repeat is the same type recorded twice.
        seen: set[str] = set()
        types = []
        for claim_type in self.claim_types:
            if claim_type in seen:
                note("duplicate_claim_type")
                continue
            seen.add(claim_type)
            types.append(claim_type)
        self.claim_types = types

        if len(seen & {"generic", "specified"}) != 1:
            note("claim_types_without_one_specificity")
        return self


# --------------------------------------------------------------------------
# I4. Overall impression


class Impression(BaseModel):
    impression_strength: Annotated[
        ImpressionStrength, BeforeValidator(blank_to("none", "impression_blank"))
    ] = "none"
    impression_exceeds_claims: bool = False  # the impression rests on more than I3
    reasons: Annotated[list[SHORT_TEXT], Field(max_length=3)] = Field(
        default_factory=list
    )

    model_config = MODEL_CONFIG


# --------------------------------------------------------------------------
# The record


class EnvironmentalContent(BaseModel):
    I1_setting: Setting = Field(default_factory=Setting)
    I2_other_markers: OtherMarkers = Field(default_factory=OtherMarkers)
    I3_claims: Annotated[list[Claim], Field(max_length=40)] = Field(
        default_factory=list
    )
    I4_overall_impression: Impression = Field(default_factory=Impression)

    model_config = MODEL_CONFIG

    @model_validator(mode="before")
    @classmethod
    def _check_payload_shape(cls, data):
        """An empty or truncated answer validates to the same all-defaults record as an
        ad with nothing in it, so the raw payload is the only place to tell them apart."""
        if isinstance(data, dict):
            expected = cls.model_fields.keys()
            present = expected & data.keys()
            if not present:
                note("payload_empty")
            else:
                for key in sorted(expected - present):
                    note(f"missing_key:{key}")
            for key in sorted(data.keys() - expected):
                note(f"unexpected_key:{key}")
        return data

    @model_validator(mode="after")
    def _drop_excluded_claims(self):
        kept = []
        for claim in self.I3_claims:
            reason = exclusion_reason(claim.quote)
            if reason:
                note(f"claim_excluded:{reason}")
            else:
                kept.append(claim)
        self.I3_claims = kept
        return self

    @model_validator(mode="after")
    def _check_impression(self):
        strength = self.I4_overall_impression.impression_strength
        cues = self.I1_setting.nature_markers or self.I2_other_markers.present_kinds
        if strength == "none" and (self.I3_claims or cues):
            note("impression_none_with_content")
        if strength in ("moderate", "strong") and not self.I3_claims and not cues:
            note("impression_without_content")
        return self
