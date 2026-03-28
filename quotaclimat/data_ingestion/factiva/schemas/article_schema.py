"""Pydantic schema for Factiva-format articles written to S3.

Used by all converters (Factiva stream, LeMonde FTP, OuestFrance API)
to validate the JSON structure before uploading to S3. This ensures
format errors are caught at write time rather than during S3→PostgreSQL
processing.

The S3 JSON format is:
    {"data": [{"id": "...", "type": "article", "attributes": {...}}, ...]}
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, field_validator


class FactivaArticleAttributes(BaseModel):
    """Validated schema for article attributes in the Factiva S3 JSON format.

    Required fields: an, source_code, source_name.
    All other fields are optional with sensible defaults for non-Factiva sources.
    """

    model_config = ConfigDict(extra="allow")

    # --- Required fields ---
    an: str  # Unique article ID (primary key in factiva_articles)
    source_code: str  # e.g. "LEMOND", "OUESTFR", "LEMFR"
    source_name: str  # e.g. "Le Monde", "Ouest-France"

    # --- Content ---
    title: Optional[str] = None
    body: Optional[str] = None
    snippet: Optional[str] = None
    art: Optional[str] = None  # Photo captions / image descriptions

    # --- Event metadata ---
    action: str = "add"  # add, rep, del
    document_type: Optional[str] = "article"
    event_type: Optional[str] = None

    # --- Author / attribution ---
    byline: Optional[str] = None
    credit: Optional[str] = None
    dateline: Optional[str] = None

    # --- Publication info ---
    publisher_name: Optional[str] = None
    section: Optional[str] = None
    copyright: Optional[str] = None

    # --- Dates (ISO 8601 strings) ---
    publication_datetime: Optional[str] = None
    publication_date: Optional[str] = None
    modification_datetime: Optional[str] = None
    modification_date: Optional[str] = None
    ingestion_datetime: Optional[str] = None
    availability_datetime: Optional[str] = None

    # --- Language / geography ---
    language_code: Optional[str] = "fr"
    region_of_origin: Optional[str] = None

    # --- Metrics ---
    word_count: Optional[int] = None

    # --- Factiva taxonomy codes (empty strings for non-Factiva sources) ---
    company_codes: Optional[Any] = ""
    company_codes_about: Optional[Any] = ""
    company_codes_association: Optional[Any] = ""
    company_codes_lineage: Optional[Any] = ""
    company_codes_occur: Optional[Any] = ""
    company_codes_relevance: Optional[Any] = ""
    subject_codes: Optional[Any] = ""
    region_codes: Optional[Any] = ""
    industry_codes: Optional[Any] = ""
    person_codes: Optional[Any] = ""
    currency_codes: Optional[Any] = ""
    market_index_codes: Optional[Any] = ""

    # --- Additional Factiva metadata (v2.44+) ---
    allow_translation: Optional[bool] = None
    attrib_code: Optional[str] = None
    authors: Optional[Any] = None  # JSON array of author names/IDs
    clusters: Optional[Any] = None  # JSON array of similar article IDs
    content_type_codes: Optional[Any] = None
    footprint_company_codes: Optional[Any] = None
    footprint_person_codes: Optional[Any] = None
    industry_classification_benchmark_codes: Optional[Any] = None
    newswires_codes: Optional[Any] = None
    org_type_codes: Optional[Any] = None
    pub_page: Optional[str] = None
    restrictor_codes: Optional[Any] = None

    # --- Optional extensions for non-Factiva sources ---
    article_url: Optional[str] = None  # Article URL (OuestFrance, LeMonde, etc.)
    tags: Optional[List[str]] = None  # Category tags

    @field_validator("an")
    @classmethod
    def an_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Article ID (an) must not be empty")
        return v

    @field_validator("source_code")
    @classmethod
    def source_code_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_code must not be empty")
        return v

    @field_validator("word_count", mode="before")
    @classmethod
    def coerce_word_count(cls, v: Any) -> Optional[int]:
        if v is None:
            return None
        return int(v)


class FactivaArticleEnvelope(BaseModel):
    """Single article in the Factiva S3 JSON format."""

    id: str
    type: str = "article"
    attributes: FactivaArticleAttributes


class FactivaS3Document(BaseModel):
    """Top-level S3 JSON document containing multiple articles.

    Usage:
        doc = FactivaS3Document(data=[envelope1, envelope2, ...])
        json_str = doc.model_dump_json()  # Validated JSON string
        json_dict = doc.model_dump()      # Validated dict for json.dump()
    """

    data: List[FactivaArticleEnvelope]
