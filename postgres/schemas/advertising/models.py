from sqlalchemy import (
    ARRAY,
    JSON,
    Column,
    DateTime,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import relationship

from postgres.schemas.advertising.base import AdvertisingBase
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.fingerprint import (
    Fingerprint,
)


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

    @classmethod
    def generate_chunk_dict(
        cls, fingerprint_hash: str, fingerprints: list[Fingerprint]
    ):
        return {
            "hash": fingerprint_hash,
            "fingerprints": [f.to_dict() for f in fingerprints],
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


class Ad_Merge(AdvertisingBase):
    """Journal of the ads built by merging several ads (pieces of one ad split by the detection).

    One row per merged ad: the merge rule, kept when the detection reprocesses a window (it deletes
    the occurrences of the window). See tools/ad_merge.py.
    """

    __tablename__ = "ad_merge"
    merged_ad_id = Column(Text, primary_key=True)
    # ids of the merged ads, in broadcast order
    source_ad_ids = Column(ARRAY(Text), nullable=False)
    # what created the merge, e.g. "auto_split_pieces" or "manual"
    method = Column(String, nullable=False)
    # "active" or "reverted"
    status = Column(String, nullable=False)
    # free context: thresholds, number of occurrences, comment...
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime(), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(), nullable=False, server_default=func.now())


class Ad_Merge_Occurrence(AdvertisingBase):
    """Journal of the occurrences replaced by an occurrence of a merged ad.

    One row per (merged occurrence, source occurrence). Deleted with these occurrences (a reprocessing
    of the detection deletes the occurrences of its window): the merge is redone on the new ones.
    """

    __tablename__ = "ad_merge_occurrence"
    __table_args__ = (
        Index("ix_ad_merge_occurrence_merged_ad_id", "merged_ad_id"),
        Index("ix_ad_merge_occurrence_source_occurrence_id", "source_occurrence_id"),
    )
    merged_occurrence_id = Column(
        Text,
        ForeignKey("ad_occurrence.id", ondelete="CASCADE"),
        primary_key=True,
    )
    source_occurrence_id = Column(
        Text,
        ForeignKey("ad_occurrence.id", ondelete="CASCADE"),
        primary_key=True,
    )
    merged_ad_id = Column(Text, nullable=False)
    source_ad_id = Column(Text, nullable=True)
    # "piece": a part of the merged ad, "absorbed": a short segment between two pieces
    role = Column(String, nullable=False)
    # position of the source occurrence in the merged occurrence, in broadcast order
    position = Column(Integer, nullable=False)
    created_at = Column(DateTime(), nullable=False, server_default=func.now())
    reverted_at = Column(DateTime(), nullable=True)
