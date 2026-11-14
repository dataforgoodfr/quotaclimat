"""The detection no longer matches the fingerprints of a soft deleted ad (merged by hand into another),
nor of an ad not detected for more than KNOWN_AD_MAX_AGE."""

import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad, Ad_Occurrence, AdvertisingBase
from quotaclimat.data_ingestion.advertising.s01_detection.e03_already_identified_advertising import (
    KNOWN_AD_MAX_AGE,
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


def add_ad(engine, ad_id, deleted=False, first_detection_date=T0, occurrences=()):
    with Session(engine) as session:
        session.add(
            Ad(
                id=ad_id,
                first_detection_date=first_detection_date,
                duration_sec=10.0,
                chunks=[{"hash": HASH, "fingerprints": [FINGERPRINT.to_dict()]}],
                fragment_type="AD",
                deleted_at=T0 if deleted else None,
                replaced_by_ad_id="live" if deleted else None,
            )
        )
        session.flush()
        for i, (occurrence_date, occurrence_deleted) in enumerate(occurrences):
            session.add(
                Ad_Occurrence(
                    id=f"{ad_id}-{i}",
                    ad_id=ad_id,
                    occurrence_date=occurrence_date,
                    channel_name="france2",
                    deleted_at=occurrence_date if occurrence_deleted else None,
                )
            )
        session.commit()


def identify():
    chunk = Chunk(start_sec=0.0, end_sec=10.0, channel="tf1", fingerprint=FINGERPRINT)
    return asyncio.run(
        run_chunk_identification([chunk], HASH, EverythingMatches(), reference_date=T0)
    )


def test_deleted_ad_is_not_matched(engine):
    add_ad(engine, "deleted", deleted=True)
    known, unknown = identify()
    assert known == [] and len(unknown) == 1

    add_ad(engine, "live")
    known, unknown = identify()
    assert [f.group_id for f in known] == ["live"] and unknown == []


def test_ad_not_detected_recently_is_not_matched(engine):
    old = T0 - KNOWN_AD_MAX_AGE - timedelta(days=1)
    recent = T0 - KNOWN_AD_MAX_AGE + timedelta(days=1)
    add_ad(engine, "never_seen_again", first_detection_date=old)
    add_ad(
        engine,
        "only_junk_recently",
        first_detection_date=old,
        occurrences=[(recent, True)],
    )
    known, unknown = identify()
    assert known == [] and len(unknown) == 1

    # A recent occurrence on any channel keeps an old ad searched
    add_ad(
        engine,
        "seen_recently",
        first_detection_date=old,
        occurrences=[(old, False), (recent, False)],
    )
    known, unknown = identify()
    assert [f.group_id for f in known] == ["seen_recently"] and unknown == []


def test_ad_first_detected_recently_is_matched(engine):
    add_ad(engine, "new", first_detection_date=T0 - timedelta(days=1))
    known, _ = identify()
    assert [f.group_id for f in known] == ["new"]
