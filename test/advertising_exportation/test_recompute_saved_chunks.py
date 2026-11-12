from datetime import datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad, Ad_Occurrence, AdvertisingBase
from quotaclimat.data_ingestion.advertising.s02_exportation import (
    recompute_saved_chunks as recompute,
)

T0 = datetime(2026, 4, 13, 20, 0, 0)
HASH = "current-hash"


@pytest.fixture
def session():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _add_ad(
    session,
    ad_id,
    nb_occurrences,
    chunk_hashes=(),
    nb_deleted=0,
    fragment_type="AD",
    obsolete=False,
):
    session.add(
        Ad(
            id=ad_id,
            first_detection_date=T0,
            duration_sec=12.0,
            chunks=[{"hash": h, "fingerprints": []} for h in chunk_hashes],
            fragment_type=fragment_type,
            obsolete_at=T0 if obsolete else None,
        )
    )
    session.flush()
    for i in range(nb_occurrences + nb_deleted):
        session.add(
            Ad_Occurrence(
                id=f"{ad_id}-occ{i}",
                occurrence_date=T0 + timedelta(minutes=10 * i),
                channel_name="tf1",
                ad_id=ad_id,
                deleted_at=T0 if i >= nb_occurrences else None,
            )
        )
    session.commit()


def _ids(session, clean_other_chunks=False, limit=None):
    query = recompute.ads_to_process_query(HASH, clean_other_chunks, limit)
    return [ad.id for ad in session.scalars(query)]


def test_skip_ads_with_current_hash_most_occurrences_first(session):
    _add_ad(session, "ad-few", 1)
    _add_ad(session, "ad-many", 5, chunk_hashes=["old-hash"])
    _add_ad(session, "ad-done", 9, chunk_hashes=[HASH])
    _add_ad(session, "ad-done-and-old", 7, chunk_hashes=["old-hash", HASH])
    _add_ad(session, "ad-none", 0)
    # deleted occurrences are not counted: 2 live occurrences only
    _add_ad(session, "ad-deleted", 2, nb_deleted=10)
    # no media to download: never taken
    _add_ad(session, "ad-no-data", 20, fragment_type="no_data")
    # obsolete (merged by hand into another): no longer matched by the detection, never taken
    _add_ad(session, "ad-obsolete", 0, nb_deleted=30, obsolete=True)

    assert _ids(session) == ["ad-many", "ad-deleted", "ad-few", "ad-none"]
    assert _ids(session, limit=2) == ["ad-many", "ad-deleted"]
    assert recompute.count_ads(session, HASH, False) == {
        "total": 8,
        "already_had_chunk": 2,
        "no_data": 1,
        "to_process": 4,
    }


def test_clean_other_chunks_also_takes_ads_with_several_entries(session):
    _add_ad(session, "ad-done", 9, chunk_hashes=[HASH])
    _add_ad(session, "ad-done-and-old", 7, chunk_hashes=["old-hash", HASH])
    _add_ad(session, "ad-old", 1, chunk_hashes=["old-hash"])
    _add_ad(
        session,
        "ad-no-data",
        5,
        chunk_hashes=["old-hash", HASH],
        fragment_type="no_data",
    )

    assert _ids(session, clean_other_chunks=True) == ["ad-done-and-old", "ad-old"]
    assert recompute.count_ads(session, HASH, True) == {
        "total": 4,
        "already_had_chunk": 3,
        "no_data": 1,
        "to_process": 2,
    }
