from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Double,
    ForeignKey,
    Index,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from postgres.schemas.advertising.base import AdvertisingBase


class Ad(AdvertisingBase):
    __tablename__ = "ad"
    __table_args__ = (Index("ix_ad_first_detection_date", "first_detection_date"),)
    id = Column(Text, primary_key=True)
    first_detection_date = Column(DateTime(), nullable=False)
    duration_sec = Column(Double, nullable=False)

    # Format: list of audio descriptions of the Ad. Each item of this list describe an audio (list of chunk) of the Ad.
    # Each description is a dict containing the "hash" describing the extraction of the fingerprints, and a list of audio "fingerprints" (list of dict).
    # [
    #     {
    #         "hash": chunk_hash,
    #         "fingerprints": [c.fingerprint.to_dict() for c in canonical_chunks],
    #     }
    # ]
    chunks = Column(JSON, nullable=False)

    fragment_type = Column(String, nullable=False)  # "advertising" or "jingle" ...
    transcript = Column(Text, nullable=True)
    prediction = Column(JSON, nullable=True)
    prediction_status = Column(String, nullable=True)
    prediction_confidence = Column(Double, nullable=True)
    predicted_sector = Column(String, nullable=True)
    predicted_product_category = Column(String, nullable=True)

    predicted_brand = Column(String, nullable=True)
    prediction_method = Column(String, nullable=True)

    def attributes(self):
        return {
            "id": self.id,
            "first_detection_date": self.first_detection_date,
            "duration_sec": self.duration_sec,
            "chunks": self.chunks,
            "fragment_type": self.fragment_type,
        }


class Ad_Occurrence(AdvertisingBase):
    __tablename__ = "ad_occurrence"
    __table_args__ = (
        Index("ix_ad_occurrence_ad_id", "ad_id"),
        Index("ix_ad_occurrence_occurrence_date", "occurrence_date"),
    )
    id = Column(Text, primary_key=True)
    deleted_at = Column(DateTime(), nullable=True)

    occurrence_date = Column(DateTime(), nullable=False)
    channel_name = Column(String, nullable=False)

    ad_id = Column(Text, ForeignKey("ad.id"), nullable=True)
    ad = relationship("Ad", foreign_keys=[ad_id])

    def attributes(self):
        return {
            "id": self.id,
            "occurrence_date": self.occurrence_date,
            "channel_name": self.channel_name,
            "ad_id": self.ad_id,
            "deleted_at": self.deleted_at,
        }
