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
    chunks = Column(JSON, nullable=False)
    fragment_type = Column(String, nullable=False)  # "advertising" or "jingle" ...
    transcript = Column(Text, nullable=True)
    prediction = Column(JSON, nullable=True)
    prediction_status = Column(String, nullable=True)
    prediction_confidence = Column(Double, nullable=True)
    predicted_sector = Column(String, nullable=True)
    predicted_product_category = Column(String, nullable=True)


class Ad_Occurrence(AdvertisingBase):
    __tablename__ = "ad_occurrence"
    id = Column(Text, primary_key=True)
    deleted_at = Column(DateTime(), nullable=True)

    occurrence_date = Column(DateTime(), nullable=False)
    channel_name = Column(String, nullable=False)

    ad_id = Column(Text, ForeignKey("ad.id"), nullable=True)
    ad = relationship("Ad", foreign_keys=[ad_id])
