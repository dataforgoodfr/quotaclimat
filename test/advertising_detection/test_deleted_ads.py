"""The detection no longer matches the fingerprints of a soft deleted ad (merged by hand into another)."""

import asyncio
from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad, AdvertisingBase
from quotaclimat.data_ingestion.advertising.s01_detection.e03_already_identified_advertising import (
    run_chunk_identification,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.common_objects import (
    Chunk,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.fingerprint import (
    Fingerprint,
)

T0 = datetime(2026, 4, 13, 20, 0, 0)
HASH = "current-hash"
FINGERPRINT = Fingerprint(
    duration_sec=10.0, energy_mean=1.0, spectral_centroid=1.0, zcr_mean=1.0
)


class EverythingMatches:
    """Comparison of fingerprints replaced: every fingerprint of the database matches the chunk."""

    def build_similarity_index(self, fingerprints):
        return self

    def get_similar_indices(self, fingerprint):
        return [0]

    def is_similar(self, a, b):
        return True


@pytest.fixture
def engine():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    yield engine
    engine.dispose()


def add_ad(engine, ad_id, deleted=False):
    with Session(engine) as session:
        session.add(
            Ad(
                id=ad_id,
                first_detection_date=T0,
                duration_sec=10.0,
                chunks=[{"hash": HASH, "fingerprints": [FINGERPRINT.to_dict()]}],
                fragment_type="AD",
                deleted_at=T0 if deleted else None,
                replaced_by_ad_id="live" if deleted else None,
            )
        )
        session.commit()


def identify():
    chunk = Chunk(start_sec=0.0, end_sec=10.0, channel="tf1", fingerprint=FINGERPRINT)
    return asyncio.run(run_chunk_identification([chunk], HASH, EverythingMatches()))


def test_deleted_ad_is_not_matched(engine):
    add_ad(engine, "deleted", deleted=True)
    known, unknown = identify()
    assert known == [] and len(unknown) == 1

    add_ad(engine, "live")
    known, unknown = identify()
    assert [f.group_id for f in known] == ["live"] and unknown == []
