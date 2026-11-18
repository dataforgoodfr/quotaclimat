from typing import Literal

from pydantic import BaseModel, model_validator

from quotaclimat.data_ingestion.advertising.s03_classification.utils.schema_base import (
    MODEL_CONFIG,
    SHORT_TEXT,
)
from quotaclimat.data_ingestion.advertising.tools.prediction_status import (
    CONTENT_TYPES,
)

ContentType = Literal[CONTENT_TYPES]
Confidence = Literal["HIGH", "MEDIUM", "LOW"]
AdSubject = Literal["PRODUCT", "GENERAL"]

# Part B describes the advertised product, so it only has a meaning when the fragment is an ad.
PART_B_FIELDS = (
    "ad_subject",
    "sector_code",
    "sector_label",
    "brand_name",
    "product_name",
    "sector_confidence",
    "sector_confidence_notes",
)


def build_ad_classification_model(sector_codes) -> type[BaseModel]:
    sector_codes = list(sector_codes)
    if not sector_codes:
        raise ValueError("sector_codes must not be empty")
    SectorCode = Literal[*tuple(sector_codes)]  # type: ignore[valid-type]

    class AdClassification(BaseModel):
        # Part A: always populated
        content_type: ContentType
        ad_type_evidence_for: SHORT_TEXT
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

        model_config = MODEL_CONFIG

        @model_validator(mode="after")
        def _clear_part_b_when_not_ad(self):
            if self.content_type != "AD":
                for field in PART_B_FIELDS:
                    setattr(self, field, None)
            return self

    AdClassification.__name__ = "AdClassification"
    return AdClassification


def build_subcategory_model(sub_category_codes) -> type[BaseModel]:
    sub_category_codes = list(sub_category_codes)
    if not sub_category_codes:
        raise ValueError("sub_category_codes must not be empty")
    SubCategoryCode = Literal[*tuple(sub_category_codes)]  # type: ignore[valid-type]

    class SubCategoryClassification(BaseModel):
        sub_category_code: SubCategoryCode | None = None  # type: ignore[valid-type]
        sub_category_label: str | None = None
        reasoning: SHORT_TEXT | None = None
        confidence: Confidence | None = None
        confidence_notes: SHORT_TEXT | None = None

        model_config = MODEL_CONFIG

        @model_validator(mode="after")
        def _clear_label_without_code(self):
            # The code is what gets stored; a label without one is an undecided answer.
            if self.sub_category_code is None:
                self.sub_category_label = None
            return self

    SubCategoryClassification.__name__ = "SubCategoryClassification"
    return SubCategoryClassification
