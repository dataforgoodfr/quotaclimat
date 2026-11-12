from datetime import datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Occurrence,
    AdvertisingBase,
)
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
    deleted=False,
):
    session.add(
        Ad(
            id=ad_id,
            first_detection_date=T0,
            duration_sec=12.0,
            chunks=[{"hash": h, "fingerprints": []} for h in chunk_hashes],
            fragment_type=fragment_type,
            deleted_at=T0 if deleted else None,
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
    # soft deleted (merged by hand into another): no longer matched by the detection, never taken
    _add_ad(session, "ad-deleted-ad", 0, nb_deleted=30, deleted=True)

    assert _ids(session) == ["ad-many", "ad-deleted", "ad-few", "ad-none"]
    assert _ids(session, limit=2) == ["ad-many", "ad-deleted"]
    assert recompute.count_ads(session, HASH, False) == {
        "total": 8,
        "already_had_chunk": 2,
        "no_data": 1,
        "merged": 0,
        "merges_to_concatenate": 0,
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

    # two entries of the current hash (fingerprints copied by a manual fusion): nothing to clean
    _add_ad(session, "ad-done-twice", 3, chunk_hashes=[HASH, HASH])

    assert _ids(session, clean_other_chunks=True) == ["ad-done-and-old", "ad-old"]
    assert recompute.count_ads(session, HASH, True) == {
        "total": 5,
        "already_had_chunk": 4,
        "no_data": 1,
        "merged": 0,
        "merges_to_concatenate": 0,
        "to_process": 2,
    }


def test_undecodable_audio_does_not_stop_the_run(tmp_path):
    # One-frame / truncated mp3: libsndfile refuses to open it
    bad = tmp_path / "raw.mp3"
    bad.write_bytes(b"\xff\xfb\x90\x64" + b"\x00" * 10)
    fingerprints, error = recompute.compute_fingerprints(str(bad), T0, 5.0)
    assert fingerprints is None
    assert "LibsndfileError" in error


def _add_merge(session, merged_id, source_ids, nb_occurrences, status="active"):
    _add_ad(session, merged_id, nb_occurrences)
    session.add(
        Ad_Merge(
            merged_ad_id=merged_id,
            source_ad_ids=source_ids,
            method="auto_split_pieces",
            status=status,
        )
    )
    session.commit()


def test_merged_ads_not_downloaded_and_sources_ranked_with_their_merges(session):
    # The occurrences of the sources were moved to the merged ad when it was merged
    _add_ad(session, "src-1", 0)
    _add_ad(session, "src-2", 1)
    _add_ad(session, "alone", 5)
    _add_merge(session, "merged", ["src-1", "src-2"], 10)
    _add_ad(session, "reverted-src", 0)
    _add_merge(session, "reverted", ["reverted-src"], 0, status="reverted")

    # merged ads (active or reverted) are never downloaded; src-2: 1 + 10, src-1: 0 + 10
    assert _ids(session) == ["src-2", "src-1", "alone", "reverted-src"]
    counts = recompute.count_ads(session, HASH, False)
    assert counts["merged"] == 2
    assert counts["merges_to_concatenate"] == 1
    assert counts["to_process"] == 4


def _chunks(session, ad_id):
    session.expire_all()
    return session.get(Ad, ad_id).chunks


def test_concatenate_merges_once_all_sources_have_the_hash(session):
    _add_ad(session, "src-1", 0)
    _add_ad(session, "src-2", 0)
    _add_merge(session, "merged", ["src-1", "src-2"], 10)
    # chunks cut from the merged ad's own audio by a previous run, and an old hash
    session.get(Ad, "merged").chunks = [
        {"hash": "old-hash", "fingerprints": [{"f": "old"}]},
        {"hash": HASH, "fingerprints": [{"f": "own-audio"}]},
    ]
    session.get(Ad, "src-1").chunks = [{"hash": HASH, "fingerprints": [{"f": 1}]}]
    session.commit()

    # src-2 has no chunk with the hash yet: the merge waits
    assert recompute.concatenate_merges(session, session, HASH, False) == {
        "merges_concatenated": 0,
        "merges_waiting_for_sources": 1,
    }

    session.get(Ad, "src-2").chunks = [
        {"hash": HASH, "fingerprints": [{"f": 2}, {"f": 3}]}
    ]
    session.commit()
    assert recompute.concatenate_merges(session, session, HASH, False) == {
        "merges_concatenated": 1,
        "merges_waiting_for_sources": 0,
    }
    assert _chunks(session, "merged") == [
        {"hash": "old-hash", "fingerprints": [{"f": "old"}]},
        {
            "hash": HASH,
            "fingerprints": [{"f": 1}, {"f": 2}, {"f": 3}],
            "from_sources": True,
        },
    ]
    # done: not taken again
    assert recompute.count_ads(session, HASH, False)["merges_to_concatenate"] == 0
    assert recompute.concatenate_merges(session, session, HASH, False) == {
        "merges_concatenated": 0,
        "merges_waiting_for_sources": 0,
    }


def test_concatenate_merges_clean_other_chunks(session):
    _add_ad(session, "src-1", 0, chunk_hashes=[HASH])
    _add_merge(session, "merged", ["src-1"], 3)
    session.get(Ad, "merged").chunks = [{"hash": "old-hash", "fingerprints": []}]
    session.commit()

    recompute.concatenate_merges(session, session, HASH, True)
    assert _chunks(session, "merged") == [
        {"hash": HASH, "fingerprints": [], "from_sources": True}
    ]


def test_concatenate_merges_with_fingerprints_copied_by_a_manual_fusion(session):
    _add_ad(session, "src-1", 0)
    _add_merge(session, "merged", ["src-1"], 3)
    copied = {"hash": HASH, "fingerprints": [{"f": "other"}], "from_ad_id": "other"}
    # src-1 got the fingerprints of an ad merged into it by hand, after its own ones
    session.get(Ad, "src-1").chunks = [
        {"hash": HASH, "fingerprints": [{"f": 1}]},
        copied,
    ]
    # merged ad: an old hash, its own entry and an entry copied by a manual fusion
    session.get(Ad, "merged").chunks = [
        {"hash": "old-hash", "fingerprints": []},
        {"hash": HASH, "fingerprints": [{"f": "own-audio"}]},
        copied,
    ]
    session.commit()

    recompute.concatenate_merges(session, session, HASH, True)
    assert _chunks(session, "merged") == [
        copied,
        {"hash": HASH, "fingerprints": [{"f": 1}], "from_sources": True},
    ]
