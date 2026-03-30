"""Dataclass schema for Factiva-format articles written to S3.

Used by all converters (Factiva stream, LeMonde FTP, OuestFrance API)
to validate the JSON structure before uploading to S3. This ensures
format errors are caught at write time rather than during S3→PostgreSQL
processing.

The S3 JSON format is:
    {"data": [{"id": "...", "type": "article", "attributes": {...}}, ...]}
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FactivaArticleAttributes:
    """Validated schema for article attributes in the Factiva S3 JSON format.

    Required fields: an, source_code, source_name.
    All other fields are optional with sensible defaults for non-Factiva sources.
    """

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

    def __post_init__(self) -> None:
        if not self.an or not self.an.strip():
            raise ValueError("Article ID (an) must not be empty")
        if not self.source_code or not self.source_code.strip():
            raise ValueError("source_code must not be empty")
        if self.word_count is not None:
            self.word_count = int(self.word_count)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FactivaArticleEnvelope:
    """Single article in the Factiva S3 JSON format."""

    id: str
    type: str = "article"
    attributes: FactivaArticleAttributes = field(default_factory=FactivaArticleAttributes)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FactivaS3Document:
    """Top-level S3 JSON document containing multiple articles.

    Usage:
        doc = FactivaS3Document(data=[envelope1, envelope2, ...])
        json_dict = doc.to_dict()  # Validated dict for json.dump()
    """

    data: List[FactivaArticleEnvelope] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
