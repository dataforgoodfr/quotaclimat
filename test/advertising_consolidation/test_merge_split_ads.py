"""Merge of the ads split by the detection: database operations (tools/ad_merge.py) and unsupervised
script (s04_consolidation/e01_merge_split_ads), on a PostgreSQL database."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Merge_Occurrence,
    Ad_Occurrence,
    AdvertisingBase,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e03_already_identified_advertising import (
    AdChunkMatch,
    _sort_chunk_matches_by_importance,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e06_export_classification import (
    clean_pre_existing_detections,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e01_merge_split_ads import (
    run as merge_run,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e01_merge_split_ads.detect import (
    PairType,
    build_sequences,
    fetch_pairs,
    group_by_source_ads,
)
from quotaclimat.data_ingestion.advertising.tools.ad_merge import (
    MERGE_STATUS_ACTIVE,
    MERGE_STATUS_REVERTED,
    MergeRevertedError,
    concatenate_chunks,
    create_merged_ad,
    merge_occurrences,
    merged_ad_id,
    revert_merge,
)

T0 = datetime(2026, 4, 13, 20, 0, 0)
HASH = "params-hash"


@pytest.fixture
def engine():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    yield engine
    engine.dispose()


def fingerprint(n: int) -> dict:
    return {
        "duration_sec": 1.0,
        "energy_mean": float(n),
        "spectral_centroid": 0.0,
        "zcr_mean": 0.0,
        "peaks": None,
        "pairs": [[n, n]],
    }


def make_ad(ad_id, duration_sec, brand=None, llm_brand=None, content_type="AD"):
    prediction = [
        {
            "stage": "ad_type",
            "raw_response": {"content_type": content_type, "brand_name": llm_brand},
        }
    ]
    return Ad(
        id=ad_id,
        first_detection_date=T0,
        duration_sec=duration_sec,
        chunks=[{"hash": HASH, "fingerprints": [fingerprint(len(ad_id))]}],
        fragment_type=content_type,
        prediction=prediction,
        prediction_status="dict_tier1" if brand else "dict_miss",
        predicted_brand=brand,
    )


def make_occ(occ_id, ad_id, seconds, channel="tf1"):
    return Ad_Occurrence(
        id=occ_id,
        occurrence_date=T0 + timedelta(seconds=seconds),
        channel_name=channel,
        ad_id=ad_id,
    )


def insert(engine, *rows):
    with Session(engine) as session:
        session.add_all(rows)
        session.commit()


def active_occurrences(engine) -> dict[str, str]:
    with Session(engine) as session:
        return {
            o.id: o.ad_id
            for o in session.scalars(
                select(Ad_Occurrence).where(Ad_Occurrence.deleted_at.is_(None))
            )
        }


# ---------- tools/ad_merge.py ----------


def test_concatenate_chunks_keeps_the_hashes_shared_by_all_ads():
    a = SimpleNamespace(
        chunks=[
            {"hash": "h1", "fingerprints": [1, 2]},
            {"hash": "h2", "fingerprints": [9]},
        ]
    )
    b = SimpleNamespace(chunks=[{"hash": "h1", "fingerprints": [3]}])
    assert concatenate_chunks([a, b]) == [{"hash": "h1", "fingerprints": [1, 2, 3]}]


def test_create_merged_ad_is_idempotent(engine):
    insert(engine, make_ad("a1", 8, "Renault"), make_ad("a2", 12, "Renault"))
    with Session(engine) as session:
        first = create_merged_ad(session, ["a1", "a2"], method="test", details={"k": 1})
        second = create_merged_ad(session, ["a1", "a2"], method="test")
        session.commit()
        assert first == second == merged_ad_id(["a1", "a2"])
        ad = session.get(Ad, first)
        assert ad.duration_sec == 20
        assert ad.fragment_type == "advertising"
        assert ad.prediction_status is None
        assert ad.chunks == [
            {"hash": HASH, "fingerprints": [fingerprint(2), fingerprint(2)]}
        ]
        merge = session.get(Ad_Merge, first)
        assert merge.source_ad_ids == ["a1", "a2"]
        assert merge.status == MERGE_STATUS_ACTIVE
        assert merge.details == {"k": 1}


def test_create_merged_ad_rejects_unknown_ads(engine):
    insert(engine, make_ad("a1", 8, "Renault"))
    with Session(engine) as session:
        with pytest.raises(ValueError):
            create_merged_ad(session, ["a1", "unknown"], method="test")
        with pytest.raises(ValueError):
            create_merged_ad(session, ["a1"], method="test")


def test_merge_occurrences_soft_deletes_and_logs(engine):
    insert(
        engine,
        make_ad("a1", 8, "Renault"),
        make_ad("a2", 12, "Renault"),
        make_ad("junk", 0.6, content_type="OTHER"),
        make_occ("o1", "a1", 0),
        make_occ("o2", "junk", 8.3),
        make_occ("o3", "a2", 9),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        new_id = merge_occurrences(session, ad_id, ["o1", "o3"], ["o2"])
        session.commit()
        journal = session.scalars(
            select(Ad_Merge_Occurrence).order_by(Ad_Merge_Occurrence.position)
        ).all()
        assert [(j.source_occurrence_id, j.role, j.position) for j in journal] == [
            ("o1", "piece", 0),
            ("o2", "absorbed", 1),
            ("o3", "piece", 2),
        ]
        assert {j.merged_occurrence_id for j in journal} == {new_id}
    assert active_occurrences(engine) == {new_id: ad_id}
    with Session(engine) as session:
        assert session.get(Ad_Occurrence, new_id).occurrence_date == T0


def test_merge_occurrences_checks_the_pieces(engine):
    insert(
        engine,
        make_ad("a1", 8, "Renault"),
        make_ad("a2", 12, "Renault"),
        make_occ("o1", "a1", 0),
        make_occ("o3", "a2", 9),
        make_occ("o4", "a2", 100),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        with pytest.raises(ValueError):  # wrong order
            merge_occurrences(session, ad_id, ["o3", "o1"])
        with pytest.raises(ValueError):  # not the source ads
            merge_occurrences(session, ad_id, ["o3", "o4"])
        with pytest.raises(ValueError):  # unknown occurrence
            merge_occurrences(session, ad_id, ["o1", "unknown"])
        with pytest.raises(ValueError):  # no merge
            merge_occurrences(session, "not-a-merge", ["o1", "o3"])


def test_revert_merge_restores_the_occurrences(engine):
    insert(
        engine,
        make_ad("a1", 8, "Renault"),
        make_ad("a2", 12, "Renault"),
        make_occ("o1", "a1", 0),
        make_occ("o3", "a2", 9),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        merge_occurrences(session, ad_id, ["o1", "o3"])
        session.commit()
        assert revert_merge(session, ad_id) == 2
        session.commit()
        assert session.get(Ad_Merge, ad_id).status == MERGE_STATUS_REVERTED
        assert session.get(Ad, ad_id).chunks == []
    assert active_occurrences(engine) == {"o1": "a1", "o3": "a2"}

    # merged again only on explicit request: same ids, reactivated
    with Session(engine) as session:
        with pytest.raises(MergeRevertedError):
            create_merged_ad(session, ["a1", "a2"], method="test")
        create_merged_ad(session, ["a1", "a2"], method="test", reactivate_reverted=True)
        new_id = merge_occurrences(session, ad_id, ["o1", "o3"])
        session.commit()
        assert session.get(Ad_Merge, ad_id).status == MERGE_STATUS_ACTIVE
        assert session.get(Ad, ad_id).chunks != []
    assert active_occurrences(engine) == {new_id: ad_id}


def test_reprocessing_deletes_the_journal_and_the_merge_is_redone(engine, monkeypatch):
    insert(
        engine,
        make_ad("a1", 8, "Renault"),
        make_ad("a2", 12, "Renault"),
        make_occ("o1", "a1", 0),
        make_occ("o3", "a2", 9),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        merged_occ_id = merge_occurrences(session, ad_id, ["o1", "o3"])
        session.commit()

    # the detection reprocesses the window: it deletes all its occurrences, then inserts the new ones
    clean_pre_existing_detections(T0, T0 + timedelta(days=1), "tf1")
    with Session(engine) as session:
        assert session.scalars(select(Ad_Merge_Occurrence)).all() == []
        assert session.get(Ad_Merge, ad_id).status == MERGE_STATUS_ACTIVE
    insert(engine, make_occ("o1", "a1", 0), make_occ("o3", "a2", 9))

    monkeypatch.setenv("AD_MERGE_START_DATE", T0.date().isoformat())
    monkeypatch.setenv("AD_MERGE_END_DATE", (T0 + timedelta(days=1)).date().isoformat())
    monkeypatch.setenv("AD_MERGE_DRY_RUN", "false")
    assert merge_run.run() == 1
    assert active_occurrences(engine) == {merged_occ_id: ad_id}
    with Session(engine) as session:
        assert len(session.scalars(select(Ad_Merge_Occurrence)).all()) == 2


# ---------- s04_consolidation/e01_merge_split_ads ----------


def insert_broadcasts(engine):
    insert(
        engine,
        make_ad("a1", 8, "Renault"),
        make_ad("a2", 12, "Renault"),
        make_ad("junk", 0.6, content_type="OTHER"),
        make_ad("b", 20, "Orange"),
        make_ad("c", 10, "Orange"),
        make_ad("lidl", 15, "Lidl"),
        make_ad("bumper", 1.5, content_type="BUMPER"),
        make_ad("leclerc", 15, "E.Leclerc"),
        # brand of the LLM only (dict_miss), spelled differently
        make_ad("f", 10, llm_brand="Free"),
        make_ad("g", 10, llm_brand="free"),
        # short piece with a brand
        make_ad("s1", 1.5, "Peugeot"),
        make_ad("s2", 18.5, "Peugeot"),
        # short segment of another brand between two pieces
        make_ad("o15", 15, "SFR"),
        make_ad("bouygues", 1, "Bouygues Telecom"),
        make_ad("o5", 5, "SFR"),
        # ad split in three pieces
        make_ad("t1", 5, "Total"),
        make_ad("t2", 5, "Total"),
        make_ad("t3", 10, "Total"),
        # detection errors, not merged
        make_ad("same", 10, "Same"),
        make_ad("k30", 30, "Included"),
        make_ad("k10", 10, "Included"),
        make_ad("m20", 20, "Overlap"),
        make_ad("m15", 15, "Overlap"),
        make_ad("n10", 10, "Gap"),
        make_ad("n12", 12, "Gap"),
        # 2 broadcasts of the same split ad
        make_occ("r1a", "a1", 0),
        make_occ("r1j", "junk", 8.3),
        make_occ("r1b", "a2", 9),
        make_occ("r2a", "a1", 86400),
        make_occ("r2b", "a2", 86408),
        make_occ("ob", "b", 25),
        make_occ("oc", "c", 45),
        make_occ("olidl", "lidl", 60),
        make_occ("obumper", "bumper", 76),
        make_occ("oleclerc", "leclerc", 78),
        make_occ("of", "f", 100),
        make_occ("ox", "junk", 110),
        make_occ("og", "g", 110.5),
        make_occ("os1", "s1", 200),
        make_occ("os2", "s2", 201.5),
        make_occ("oo15", "o15", 300),
        make_occ("obouygues", "bouygues", 315.2),
        make_occ("ox2", "junk", 316.2),
        make_occ("oo5", "o5", 317),
        make_occ("ot1", "t1", 400),
        make_occ("ot2", "t2", 405),
        make_occ("ot3", "t3", 410),
        make_occ("osame1", "same", 500),
        make_occ("osame2", "same", 510),
        make_occ("ok30", "k30", 600),
        make_occ("ok10", "k10", 610),
        make_occ("om20", "m20", 700),
        make_occ("om15", "m15", 715),
        make_occ("on10", "n10", 800),
        make_occ("on12", "n12", 812),
    )


def test_pair_types(engine):
    insert_broadcasts(engine)
    with engine.connect() as connection:
        pairs = fetch_pairs(connection, T0, T0 + timedelta(days=2), 2.0, 3.0)
    types = {(p.ad_id, p.next_ad_id): p.pair_type for p in pairs}
    assert types == {
        ("a1", "a2"): PairType.CONTIGUOUS,  # 2nd broadcast; the 1st one is below
        ("b", "c"): PairType.CONTIGUOUS,
        ("f", "g"): PairType.SHORT_UNBRANDED,
        ("s1", "s2"): PairType.CONTIGUOUS,
        ("o15", "o5"): PairType.SHORT_OTHER_BRAND,
        ("t1", "t2"): PairType.CONTIGUOUS,
        ("t2", "t3"): PairType.CONTIGUOUS,
        ("same", "same"): PairType.SAME_AD,
        ("k30", "k10"): PairType.INCLUDED,
        ("m20", "m15"): PairType.OVERLAP,
        ("n10", "n12"): PairType.GAP,
    }
    first_broadcast = next(p for p in pairs if p.occurrence_id == "r1a")
    assert first_broadcast.pair_type == PairType.SHORT_UNBRANDED
    assert first_broadcast.between_occurrence_ids == ("r1j",)


def test_sequences(engine):
    insert_broadcasts(engine)
    with engine.connect() as connection:
        pairs = fetch_pairs(connection, T0, T0 + timedelta(days=2), 2.0, 3.0)
    groups = group_by_source_ads(build_sequences(pairs, T0 + timedelta(days=2)))
    assert {k: len(v) for k, v in groups.items()} == {
        ("a1", "a2"): 2,
        ("b", "c"): 1,
        ("f", "g"): 1,
        ("s1", "s2"): 1,
        ("o15", "o5"): 1,
        ("t1", "t2", "t3"): 1,
    }
    (sfr,) = groups[("o15", "o5")]
    assert sfr.absorbed_occurrence_ids == ["obouygues", "ox2"]
    (total,) = groups[("t1", "t2", "t3")]
    assert total.piece_occurrence_ids == ["ot1", "ot2", "ot3"]
    assert total.duration_sec == 20


def test_run_dry_run_changes_nothing(engine, monkeypatch):
    insert_broadcasts(engine)
    before = active_occurrences(engine)
    monkeypatch.setenv("AD_MERGE_START_DATE", T0.date().isoformat())
    monkeypatch.setenv("AD_MERGE_END_DATE", (T0 + timedelta(days=2)).date().isoformat())
    monkeypatch.delenv("AD_MERGE_DRY_RUN", raising=False)
    assert merge_run.run() == 0
    assert active_occurrences(engine) == before


def test_run_merges_the_split_ads(engine, monkeypatch):
    insert_broadcasts(engine)
    monkeypatch.setenv("AD_MERGE_START_DATE", T0.date().isoformat())
    monkeypatch.setenv("AD_MERGE_END_DATE", (T0 + timedelta(days=2)).date().isoformat())
    monkeypatch.setenv("AD_MERGE_DRY_RUN", "false")
    assert merge_run.run() == 7

    active = active_occurrences(engine)
    for source_ad_ids, count in [
        (["a1", "a2"], 2),
        (["b", "c"], 1),
        (["f", "g"], 1),
        (["s1", "s2"], 1),
        (["o15", "o5"], 1),
        (["t1", "t2", "t3"], 1),
    ]:
        assert list(active.values()).count(merged_ad_id(source_ad_ids)) == count
    for occ_id in ["r1a", "r1j", "r1b", "obouygues", "ox2", "ot2"]:
        assert occ_id not in active
    for occ_id in ["olidl", "obumper", "oleclerc", "osame1", "osame2", "ok30", "ok10"]:
        assert occ_id in active
    with Session(engine) as session:
        merge = session.get(Ad_Merge, merged_ad_id(["a1", "a2"]))
        assert merge.method == merge_run.MERGE_METHOD
        # created on the first day, with its first broadcast
        assert merge.details["broadcasts_at_creation"] == 1

    # a second run finds nothing more to merge
    assert merge_run.run() == 0

    # a merge reverted by a human is not redone
    with Session(engine) as session:
        revert_merge(session, merged_ad_id(["b", "c"]))
        session.commit()
    assert merge_run.run() == 0
    active = active_occurrences(engine)
    assert active["ob"] == "b" and active["oc"] == "c"


# ---------- s01_detection/e03_already_identified_advertising.py ----------


def test_classified_ads_are_tried_before_other_content():
    def match(fragment_type, duration_sec, chunk_index=0):
        ad = SimpleNamespace(fragment_type=fragment_type, duration_sec=duration_sec)
        return AdChunkMatch(ad=ad, chunk_entry_index=0, chunk_index=chunk_index)

    jingle = match("jingle", 30)
    bumper = match("BUMPER", 30)
    classified = match("AD", 10)
    merged = match("advertising", 20)
    middle = match("AD", 40, chunk_index=1)
    ordered = _sort_chunk_matches_by_importance(
        [jingle, bumper, classified, merged, middle]
    )
    assert ordered == [merged, classified, middle, jingle, bumper]
