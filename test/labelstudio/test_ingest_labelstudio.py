import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Import your module
import quotaclimat.data_ingestion.labelstudio.ingest_labelstudio as ls
from quotaclimat.data_ingestion.labelstudio import models



def test_corlab_perimeter_ingests_no_label_studio_project(monkeypatch):
    # The Corlab database must never receive the annotations of the other countries (db_config):
    # with no Corlab project configured, nothing is read from Label Studio.
    from quotaclimat.data_ingestion.labelstudio import ingest_labelstudio
    from quotaclimat.data_ingestion.labelstudio.configs import (
        db_config_corlab_perimeter,
    )

    assert db_config_corlab_perimeter == []

    def fail(*args, **kwargs):
        raise AssertionError("Label Studio must not be read")

    monkeypatch.setattr(ingest_labelstudio, "collect_all_data", fail)
    monkeypatch.setattr(ingest_labelstudio, "upsert_data_to_target", fail)
    ingest_labelstudio.main({}, {}, db_config=db_config_corlab_perimeter)
