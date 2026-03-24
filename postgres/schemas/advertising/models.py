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

from postgres.schemas.base import Base


class Advertising(Base):
    __tablename__ = "advertising"
    id = Column(Text, primary_key=True)
    first_detection_date = Column(DateTime(), nullable=False)
    duration_sec = Column(Double, nullable=False)
    chunks = Column(JSON, nullable=False)
    fragment_type = Column(String, nullable=False)  # "advertising" or "jingle" ...


class Advertising_Occurrence(Base):
    __tablename__ = "advertising_occurrence"
    id = Column(Text, primary_key=True)
    deleted_at = Column(DateTime(), nullable=True)

    occurrence_date = Column(DateTime(), nullable=False)
    channel_name = Column(String, nullable=False)

    advertising_id = Column(Text, ForeignKey("advertising.id"), nullable=True)
    advertising = relationship("Advertising", foreign_keys=[advertising_id])
