from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

ContentType = Literal["AD", "BUMPER", "CHANNEL_PROMO", "SPONSORSHIP", "OTHER"]
Completeness = Literal["FULL", "CUT_OFF", "UNCERTAIN"]
CutOffPoint = Literal["CUT_START", "CUT_END", "CUT_BOTH", "UNCERTAIN"]
Confidence = Literal["HIGH", "MEDIUM", "LOW"]
AdSubject = Literal["PRODUCT", "GENERAL"]

SHORT_TEXT = Annotated[str, Field(max_length=500)]


def build_ad_classification_model(sector_codes) -> type[BaseModel]:
    """Pydantic schema with sector_code constrained to the given codes (for guided JSON)"""
    sector_codes = list(sector_codes)
    if not sector_codes:
        raise ValueError("sector_codes must not be empty")
    SectorCode = Literal[*tuple(sector_codes)]  # type: ignore[valid-type]

    class AdClassification(BaseModel):
        # Part A: always populated
        content_type: ContentType
        completeness: Completeness
        cut_off_point: CutOffPoint | None = None
        ad_type_evidence_for: SHORT_TEXT
        ad_type_evidence_against: SHORT_TEXT
        ad_type_confidence: Confidence
        ad_type_confidence_notes: SHORT_TEXT

        # Part B: populated only if content_type == "AD"
        ad_subject: AdSubject | None = None
        sector_code: SectorCode | None = None  # type: ignore[valid-type]
        sector_label: str | None = None
        brand_name: str | None = None
        product_name: str | None = None
        sector_confidence: Confidence | None = None
        sector_confidence_notes: SHORT_TEXT | None = None

        model_config = {"extra": "forbid", "str_strip_whitespace": True}

        @model_validator(mode="after")
        def _enforce_part_b_nulls(self):
            if self.content_type != "AD":
                part_b = (
                    self.ad_subject,
                    self.sector_code,
                    self.sector_label,
                    self.brand_name,
                    self.product_name,
                    self.sector_confidence,
                    self.sector_confidence_notes,
                )
                if any(v is not None for v in part_b):
                    raise ValueError(
                        "Part B fields must be null when content_type != AD"
                    )
            else:
                if self.sector_confidence is None:
                    raise ValueError("sector_confidence is required when content_type == AD")
            return self

        @model_validator(mode="after")
        def _cut_off_point_consistency(self):
            if self.completeness != "CUT_OFF" and self.cut_off_point is not None:
                raise ValueError(
                    "cut_off_point must be null unless completeness == CUT_OFF"
                )
            return self

    AdClassification.__name__ = "AdClassification"
    return AdClassification
