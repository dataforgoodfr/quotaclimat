import logging
import os
import json

from datetime import datetime
from json import JSONDecodeError

import pandas as pd
from postgres.database_connection import connect_to_db, get_db_session


from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Double,
    String,
    Text,
    Boolean,
    JSON,
    Integer,
    ForeignKey,
    Uuid,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

SourceBase = declarative_base()
TargetBase = declarative_base()

labelstudio_task_source_table = "task"
labelstudio_task_completion_source_table = "task_completion"
labelstudio_task_aggregate_table = "labelstudio_task_aggregate"
labelstudio_task_completion_aggregate_table = "labelstudio_task_completion_aggregate"

class LabelStudioTaskSource(SourceBase):
    __tablename__ = labelstudio_task_source_table
    # column_name,data_type,character_maximum_length,column_default,is_nullable
    id = Column(Integer, nullable=False, primary_key=True)
    data = Column(JSON, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    is_labeled = Column(Boolean, nullable=False)
    project_id = Column(Integer, nullable=True)
    meta = Column(JSON, nullable=True)
    overlap = Column(Integer, nullable=False)
    file_upload_id = Column(Integer, nullable=True)
    updated_by_id = Column(Integer, nullable=True)
    inner_id = Column(BigInteger, nullable=True)
    total_annotations = Column(Integer, nullable=False)
    cancelled_annotations = Column(Integer, nullable=False)
    total_predictions = Column(Integer, nullable=False)
    comment_count = Column(Integer, nullable=False)
    last_comment_updated_at = Column(DateTime, nullable=True)
    unresolved_comment_count = Column(Integer, nullable=False)


class LabelStudioTaskCompletionSource(SourceBase):
    __tablename__ = labelstudio_task_completion_source_table
    # column_name,data_type,character_maximum_length,column_default,is_nullable
    id = Column(Integer, nullable=False, primary_key=True)
    result = Column(JSON, nullable=True)
    was_cancelled = Column(Boolean, nullable=False)
    ground_truth = Column(Boolean, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    task_id = Column(Integer, nullable=True)
    prediction = Column(JSON, nullable=True)
    lead_time = Column(Double, nullable=True)
    result_count = Column(Integer, nullable=False)
    completed_by_id = Column(Integer, nullable=True)
    parent_prediction_id = Column(Integer, nullable=True)
    parent_annotation_id = Column(Integer, nullable=True)
    last_action = Column(Text, nullable=True)
    last_created_by_id = Column(Integer, nullable=True)
    project_id = Column(Integer, nullable=True)
    updated_by_id = Column(Integer, nullable=True)
    unique_id = Column(Uuid, nullable=True)
    draft_created_at = Column((DateTime()), nullable=True)
    import_id = Column(BigInteger, nullable=True)
    bulk_created = Column(Boolean, nullable=True, default=False)


class LabelStudioTaskAggregate(TargetBase):
    __tablename__ = labelstudio_task_aggregate_table
    task_aggregate_id = Column(String, nullable=False, primary_key=True)
    id = Column(Integer, nullable=False)
    data = Column(JSON, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    is_labeled = Column(Boolean, nullable=False)
    project_id = Column(Integer, nullable=True)
    meta = Column(JSON, nullable=True)
    overlap = Column(Integer, nullable=False)
    file_upload_id = Column(Integer, nullable=True)
    updated_by_id = Column(Integer, nullable=True)
    inner_id = Column(BigInteger, nullable=True)
    total_annotations = Column(Integer, nullable=False)
    cancelled_annotations = Column(Integer, nullable=False)
    total_predictions = Column(Integer, nullable=False)
    comment_count = Column(Integer, nullable=False)
    last_comment_updated_at = Column(DateTime, nullable=True)
    unresolved_comment_count = Column(Integer, nullable=False)
    country = Column(String, nullable=False)


class LabelStudioTaskCompletionAggregate(TargetBase):
    __tablename__ = labelstudio_task_completion_aggregate_table
    task_completion_aggregate_id = Column(String, nullable=False, primary_key=True)
    task_aggregate_id = Column(
        String, ForeignKey(f"{labelstudio_task_aggregate_table}.task_aggregate_id"), nullable=False
    )
    labelstudio_task_aggregate = relationship(
        "LabelStudioTaskAggregate", foreign_keys=[task_aggregate_id]
    )
    
    id = Column(Integer, nullable=False)
    result = Column(JSON, nullable=True)
    was_cancelled = Column(Boolean, nullable=False)
    ground_truth = Column(Boolean, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    task_id = Column(Integer, nullable=True)
    prediction = Column(JSON, nullable=True)
    lead_time = Column(Double, nullable=True)
    result_count = Column(Integer, nullable=False)
    completed_by_id = Column(Integer, nullable=True)
    parent_prediction_id = Column(Integer, nullable=True)
    parent_annotation_id = Column(Integer, nullable=True)
    last_action = Column(Text, nullable=True)
    last_created_by_id = Column(Integer, nullable=True)
    project_id = Column(Integer, nullable=True)
    updated_by_id = Column(Integer, nullable=True)
    unique_id = Column(Uuid, nullable=True)
    draft_created_at = Column((DateTime()), nullable=True)
    import_id = Column(BigInteger, nullable=True)
    bulk_created = Column(Boolean, nullable=True, default=False)
    country = Column(String, nullable=False)


def create_tables(conn=None):
    """Create tables in the PostgreSQL database"""
    logging.info("create labelstudio_task_aggregate, labelstudio_task_completion_aggregate")
    try:
        if conn is None :
            engine = connect_to_db()
        else:
            engine = conn
        TargetBase.metadata.create_all(engine, checkfirst=True)

        logging.info("Table creation done, if not already done.")
    except (Exception) as error:
        logging.error(error)
    finally:
        if engine is not None:
            engine.dispose()

