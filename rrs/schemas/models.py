from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.sql import text
from sqlalchemy.orm import relationship

from rrs.schemas.base import RRSBase


class Subject(RRSBase):
    __tablename__ = "subjects"

    subject_id = Column(Text, primary_key=True)
    name = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)

    segments = relationship("Segment", back_populates="subject")
    cases = relationship("Case", back_populates="subject")
    clusters = relationship("Cluster", back_populates="subject")
    dictionary_entries = relationship("DictionaryEntry", back_populates="subject")


class DictionaryEntry(RRSBase):
    __tablename__ = "dictionary"

    keyword_id = Column(String, primary_key=True)
    subject_id = Column(String, ForeignKey("subjects.subject_id"), nullable=True)
    keyword = Column(String, nullable=True)
    high_risk_false_positive = Column(Boolean, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)

    subject = relationship("Subject", back_populates="dictionary_entries")


class Segment(RRSBase):
    __tablename__ = "segments"

    segment_id = Column(String, primary_key=True)
    subject_id = Column(String, ForeignKey("subjects.subject_id"), nullable=True)
    s3_uri = Column(String, nullable=True)
    n_keywords = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)

    subject = relationship("Subject", back_populates="segments")
    cases = relationship("Case", back_populates="segment")


class Case(RRSBase):
    __tablename__ = "cases"

    case_id = Column(String, primary_key=True)
    segment_id = Column(String, ForeignKey("segments.segment_id"), nullable=True)
    subject_id = Column(String, ForeignKey("subjects.subject_id"), nullable=True)
    model_score = Column(String, nullable=True)
    model_reason = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)

    segment = relationship("Segment", back_populates="cases")
    subject = relationship("Subject", back_populates="cases")
    case_to_clusters = relationship("CaseToCluster", back_populates="case")


class Cluster(RRSBase):
    __tablename__ = "clusters"

    cluster_id = Column(String, primary_key=True)
    subject_id = Column(String, ForeignKey("subjects.subject_id"), nullable=True)
    cluster_text = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)

    subject = relationship("Subject", back_populates="clusters")
    case_to_clusters = relationship("CaseToCluster", back_populates="cluster")


class CaseToCluster(RRSBase):
    __tablename__ = "case_to_clusters"

    case_id = Column(String, ForeignKey("cases.case_id"), primary_key=True)
    cluster_id = Column(String, ForeignKey("clusters.cluster_id"), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))

    case = relationship("Case", back_populates="case_to_clusters")
    cluster = relationship("Cluster", back_populates="case_to_clusters")
