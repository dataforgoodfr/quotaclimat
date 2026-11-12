"""Search of the duplicate ads from their fingerprints (s04_consolidation/duplicate_ads): matching on
synthetic fingerprints, and the SQL query on a PostgreSQL database."""

import random
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import (
    Ad,
    Ad_Occurrence,
    Ad_Screenshot,
    AdvertisingBase,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.duplicate_ads import (
    find_duplicates as fd,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.compare import (
    FingerprintsCompare,
)

FPS = 16000 / 1024
HASH = "current-hash"
T0 = datetime(2026, 4, 13, 20, 0, 0)

# same tolerances as the detection (s01_detection/processor.py)
COMPARE = FingerprintsCompare(
    min_matching_pairs=10,
    similarity_threshold=0.05,
    freq_tol=2,
    dt_tol=1,
    offset_tol=2,
    duration_tol=1.0,
    rms_tol=0.1,
    centroid_tol=0.05,
    zcr_tol=0.1,
)


def random_audio(seed: int, duration_sec: float, pairs_per_sec: int = 20) -> list:
    """Pairs of a whole audio: (f1, f2, dt, t in frames, amp)."""
    rng = random.Random(seed)
    n_frames = int(duration_sec * FPS)
    return [
        (
            rng.randrange(0, 512),
            rng.randrange(0, 512),
            rng.randrange(1, 30),
            rng.randrange(0, n_frames),
            rng.randrange(0, 2000),
        )
        for _ in range(int(duration_sec * pairs_per_sec))
    ]


def split_in_chunks(audio_pairs: list, boundaries_sec: list[float]) -> list[dict]:
    """Chunk fingerprints (as stored in ad.chunks) of an audio split at these boundaries."""
    chunks = []
    for start, end in zip(boundaries_sec, boundaries_sec[1:]):
        shift = round(start * FPS)
        chunks.append(
            {
                "duration_sec": round(end - start, 2),
                "energy_mean": 0.1,
                "spectral_centroid": 2000.0,
                "zcr_mean": 0.1,
                "peaks": [],
                "pairs": [
                    [p[0], p[1], p[2], p[3] - shift, p[4]]
                    for p in audio_pairs
                    if start * FPS <= p[3] < end * FPS
                ],
            }
        )
    return chunks


def ad_print(ad_id: str, chunks: list[dict], brand: str = "Renault", block=None):
    fingerprint = fd.whole_ad_fingerprint(chunks, FPS)
    return fd.AdPrint(
        id=ad_id,
        duration_sec=fingerprint.duration_sec,
        block=block if block is not None else fd.brand_key(brand),
        fingerprint=fingerprint,
        nb_chunks=len(chunks),
        index_keys=fd.sample_index_keys(fingerprint.pairs, 60, 2, 1, seed=ad_id),
        meta={"brand": brand},
    )


def test_whole_ad_fingerprint_shifts_pairs_by_chunk_start():
    audio = random_audio(1, 10)
    chunks = split_in_chunks(audio, [0, 3.2, 7.5, 10])
    fingerprint = fd.whole_ad_fingerprint(chunks, FPS)
    assert fingerprint.duration_sec == 10
    assert sorted(fingerprint.pairs) == sorted(tuple(p) for p in audio)


def test_same_audio_split_differently_is_a_duplicate():
    audio = random_audio(1, 20)
    finder = fd.DuplicateFinder(COMPARE)
    assert finder.add(ad_print("a", split_in_chunks(audio, [0, 2.1, 5, 11, 20]))) == []
    pairs = finder.add(ad_print("b", split_in_chunks(audio, [0, 3, 8.4, 15, 20])))
    assert [(p.ad_a.id, p.ad_b.id) for p in pairs] == [("a", "b")]
    assert pairs[0].score_min > 0.9
    assert pairs[0].features_compatible


def test_different_audio_or_brand_or_duration_is_not_a_duplicate():
    audio = random_audio(1, 20)
    finder = fd.DuplicateFinder(COMPARE)
    finder.add(ad_print("a", split_in_chunks(audio, [0, 10, 20])))
    # another audio of the same duration and brand
    assert (
        finder.add(ad_print("other", split_in_chunks(random_audio(2, 20), [0, 20])))
        == []
    )
    # the same audio, but another brand: not compared with --blocking brand
    assert (
        finder.add(ad_print("c", split_in_chunks(audio, [0, 20]), brand="Peugeot"))
        == []
    )
    # the same audio, without blocking: compared
    finder_all = fd.DuplicateFinder(COMPARE)
    finder_all.add(ad_print("a", split_in_chunks(audio, [0, 10, 20]), block="*"))
    assert (
        len(finder_all.add(ad_print("c", split_in_chunks(audio, [0, 20]), block="*")))
        == 1
    )


def test_ads_out_of_the_duration_window_leave_the_index():
    short = random_audio(1, 10)
    finder = fd.DuplicateFinder(COMPARE, duration_tol=1.0)
    finder.add(ad_print("short", split_in_chunks(short, [0, 10])))
    # 10 s of the same audio followed by 5 s of something else: out of the window
    longer = short + [
        (p[0], p[1], p[2], p[3] + round(10 * FPS), p[4]) for p in random_audio(3, 5)
    ]
    assert finder.add(ad_print("longer", split_in_chunks(longer, [0, 15]))) == []
    assert "short" not in finder._ads
    assert finder._index.get("renault", {}) and all(
        "short" not in ids for ids in finder._index["renault"].values()
    )


def test_contained_ad_has_low_score_max():
    audio = random_audio(1, 20)
    finder = fd.DuplicateFinder(COMPARE, duration_tol=5.0)
    finder.add(ad_print("part", split_in_chunks(audio, [0, 16])))
    pairs = finder.add(ad_print("whole", split_in_chunks(audio, [0, 20])))
    assert len(pairs) == 1
    assert pairs[0].score_min > 0.9
    assert pairs[0].score_max < pairs[0].score_min


def test_output_s3_path():
    assert (
        fd.output_s3_path("test-advertising-detection", "/duplicate_ads/", "/tmp/a.csv")
        == "test-advertising-detection/duplicate_ads/a.csv"
    )
    assert fd.output_s3_path("bucket", "", "a.csv") == "bucket/a.csv"


def test_group_rows_one_row_per_ad():
    a, b, c = (ad_print(x, []) for x in "abc")
    pairs = [
        fd.DuplicatePair(a, b, 10, 0.9, 0.8, True),
        fd.DuplicatePair(b, c, 12, 0.5, 0.5, True),
    ]
    rows = {row["ad_id"]: row for row in fd.group_rows(pairs)}
    assert set(rows) == {"a", "b", "c"}
    assert {row["group_id"] for row in rows.values()} == {"a"}
    assert rows["b"]["nb_matches"] == 2
    assert rows["b"]["best_match_ad_id"] == "a" and rows["b"]["best_score_min"] == 0.9
    assert rows["b"]["matches"] == "a:0.9 c:0.5"
    assert rows["c"]["best_match_ad_id"] == "b" and rows["c"]["best_shared_keys"] == 12


def test_metabase_query_path():
    assert fd.metabase_query_path("out/dups.csv") == "out/dups_metabase.txt"


def test_connected_groups():
    a, b, c, d = (ad_print(x, []) for x in "abcd")
    pair = lambda x, y: fd.DuplicatePair(x, y, 10, 1.0, 1.0, True)  # noqa: E731
    groups = fd.connected_groups([pair(b, c), pair(a, b), pair(d, d)])
    assert groups == {"a": ("a", 3), "b": ("a", 3), "c": ("a", 3), "d": ("d", 1)}


@pytest.fixture
def connection():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    with Session(engine) as session:
        audio = random_audio(1, 20)
        ads = [
            # id, brand, chunks, entries hash, nb occurrences
            ("dup-1", "Renault", split_in_chunks(audio, [0, 5, 20]), HASH, 2),
            ("dup-2", "RENAULT", split_in_chunks(audio, [0, 12, 20]), HASH, 1),
            ("old-hash", "Renault", split_in_chunks(audio, [0, 20]), "old", 1),
            ("deleted", "Renault", split_in_chunks(audio, [0, 20]), HASH, 0),
            (
                "other",
                "Renault",
                split_in_chunks(random_audio(2, 20), [0, 20]),
                HASH,
                1,
            ),
        ]
        for ad_id, brand, chunks, chunk_hash, nb_occurrences in ads:
            session.add(
                Ad(
                    id=ad_id,
                    first_detection_date=T0,
                    duration_sec=20.0,
                    chunks=[
                        {"hash": "older", "fingerprints": []},
                        {"hash": chunk_hash, "fingerprints": chunks},
                    ],
                    fragment_type="AD",
                    predicted_brand=brand,
                )
            )
            session.flush()
            session.add(
                Ad_Occurrence(
                    id=f"{ad_id}-occ",
                    occurrence_date=T0,
                    channel_name="tf1",
                    ad_id=ad_id,
                    deleted_at=T0 if nb_occurrences == 0 else None,
                )
            )
            for i in range(1, nb_occurrences):
                session.add(
                    Ad_Occurrence(
                        id=f"{ad_id}-occ{i}",
                        occurrence_date=T0 + timedelta(minutes=i),
                        channel_name="tf1",
                        ad_id=ad_id,
                    )
                )
        session.add(
            Ad_Screenshot(
                target_type="ad",
                target_id="dup-1",
                ad_id="dup-1",
                status="done",
                image_url="https://bucket/screenshots/ads/dup-1.jpg",
            )
        )
        session.commit()
    with engine.connect() as connection:
        yield connection
    engine.dispose()


def args(**kwargs):
    defaults = dict(
        hash=HASH,
        blocking="brand",
        include_unbranded=False,
        brand=None,
        min_duration=None,
        max_duration=None,
        date_from=None,
        date_to=None,
        fragment_types=["AD", "advertising"],
        min_occurrences=1,
        max_ads=None,
        duration_tol=1.0,
        index_pairs=60,
        min_shared_keys=10,
        max_posting=5000,
        min_score=0.2,
        compare=COMPARE,
        freq_tol=2,
        dt_tol=1,
        frames_per_sec=FPS,
    )
    return SimpleNamespace(**{**defaults, **kwargs})


def test_run_on_database(connection, tmp_path):
    ads = list(fd.stream_ads(connection, args(), FPS))
    # the ad without the hash and the ad without live occurrence are not read
    assert [ad.id for ad in ads] == ["dup-1", "dup-2", "other"]
    assert ads[0].nb_chunks == 2 and ads[0].meta["nb_occurrences"] == 2
    assert len(ads[0].fingerprint.pairs) == 400

    pairs = fd.run(connection, args())
    assert [(p.ad_a.id, p.ad_b.id) for p in pairs] == [("dup-1", "dup-2")]

    assert fd.run(connection, args(min_duration=21)) == []
    assert [ad.id for ad in fd.stream_ads(connection, args(brand="peugeot"), FPS)] == []

    rows = fd.group_rows(pairs)
    output = tmp_path / "duplicates.csv"
    fd.write_csv(str(output), rows)
    lines = output.read_text().splitlines()
    # one line per ad of the group, each with its own data
    assert len(lines) == 3
    assert lines[1].startswith("dup-1,2,dup-1,1,dup-2,")
    assert lines[2].startswith("dup-1,2,dup-2,1,dup-1,")

    # the Metabase query runs on the database and loads the data of the ads
    result = (
        connection.execute(text(fd.metabase_query(rows, "advertising-detection")))
        .mappings()
        .all()
    )
    assert [(r["groupe_id"], r["ad_id"], r["meilleur_match_id"]) for r in result] == [
        ("dup-1", "dup-1", "dup-2"),
        ("dup-1", "dup-2", "dup-1"),
    ]
    assert result[0]["mosaique"] == "https://bucket/screenshots/ads/dup-1.jpg"
    assert result[1]["mosaique"] is None
    assert result[0]["nb_diffusions"] == 2 and result[0]["marque"] == "Renault"
    assert result[0]["lien_sauvegarde"] == (
        "https://console.scaleway.com/object-storage/buckets/fr-par/advertising-detection"
        "/files/ads/dup-1/"
    )
    assert result[0]["lien_mediatree"].startswith(
        "https://keywords.mediatree.fr/player/?fifo=tf1&start_cts="
    )
    # without any duplicate, the query still runs
    assert connection.execute(text(fd.metabase_query([], "b"))).all() == []
