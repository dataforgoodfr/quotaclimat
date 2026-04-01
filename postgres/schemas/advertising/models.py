from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Double,
    ForeignKey,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from postgres.schemas.advertising.base import AdvertisingBase


class Ad(AdvertisingBase):
    __tablename__ = "ad"
    id = Column(Text, primary_key=True)
    first_detection_date = Column(DateTime(), nullable=False)
    duration_sec = Column(Double, nullable=False)
    chunks_fingerprint = Column(JSON, nullable=False)
    fragment_type = Column(String, nullable=False)  # "advertising" or "jingle" ...


class Ad_Occurrence(AdvertisingBase):
    __tablename__ = "ad_occurrence"
    id = Column(Text, primary_key=True)
    deleted_at = Column(DateTime(), nullable=True)

    occurrence_date = Column(DateTime(), nullable=False)
    channel_name = Column(String, nullable=False)

    ad_id = Column(Text, ForeignKey("ad.id"), nullable=True)
    ad = relationship("Ad", foreign_keys=[ad_id])
