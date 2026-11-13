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
    Ad_Merge,
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


def test_multibrand_columns_only_for_other_brands():
    a, b, c, d = (ad_print(x, []) for x in "abcd")
    b.meta["brand"] = "Peugeot"  # a, c: Renault, d: no brand
    d.meta["brand"] = None
    pairs = [
        fd.DuplicatePair(a, b, 10, 0.9, 0.8, True),
        fd.DuplicatePair(a, c, 12, 0.95, 0.9, True),
        fd.DuplicatePair(c, d, 11, 0.7, 0.7, False),
    ]
    rows = {row["ad_id"]: row for row in fd.group_rows(pairs)}
    assert all(row["is_multibrand_group"] for row in rows.values())
    assert rows["a"]["group_brands"] == "Peugeot | Renault"
    # a: its best match is c (Renault), but only b is another brand
    assert rows["a"]["best_match_ad_id"] == "c"
    assert rows["a"]["nb_matches_other_brand"] == 1
    assert rows["a"]["other_brand_match_ad_id"] == "b"
    assert rows["a"]["other_brand_matches"] == "b(Peugeot):0.9"
    assert rows["b"]["other_brand_match_brand"] == "Renault"
    # c: linked to Peugeot only through a, d: no brand
    assert rows["c"]["nb_matches_other_brand"] == 0
    assert rows["c"]["other_brand_match_ad_id"] is None
    assert rows["d"]["nb_matches_other_brand"] == 0


def test_sql_literal():
    assert fd._sql_literal(True) == "true"
    assert fd._sql_literal(None) == "NULL"
    assert fd._sql_literal("l'ad") == "'l''ad'"
    assert fd._sql_literal(0.5) == "0.5"


def _ad(ad_id, brand, nb_occurrences, is_merged=False):
    ad = ad_print(ad_id, [], brand=brand)
    ad.meta.update(nb_occurrences=nb_occurrences, is_merged=is_merged)
    return ad


def test_choose_target():
    # the most frequent brand (normalised) wins, even with fewer occurrences
    ads = [
        _ad("a", "Peugeot", 100),
        _ad("b", "Renault", 5),
        _ad("c", "RENAULT", 3),
    ]
    assert fd.choose_target(ads).id == "b"
    # brands tied: the most broadcast of both, but not a merged ad
    ads = [
        _ad("a", "Peugeot", 100, is_merged=True),
        _ad("b", "Renault", 5),
        _ad("c", "Peugeot", 1),
    ]
    assert fd.choose_target(ads[:2]).id == "b"
    # merged ads only in the biggest brand: the most broadcast of them
    ads = [
        _ad("a", "Peugeot", 1, is_merged=True),
        _ad("b", "Peugeot", 9, is_merged=True),
        _ad("c", "Renault", 50),
    ]
    assert fd.choose_target(ads).id == "b"
    # ads without brand are candidates only when no ad has a brand
    assert fd.choose_target([_ad("a", None, 9), _ad("b", "Renault", 1)]).id == "b"
    assert fd.choose_target([_ad("a", None, 9), _ad("b", None, 1)]).id == "a"


def test_csv_separator_between_groups(tmp_path):
    a, b, c, d = (_ad(x, "Renault", 1) for x in "abcd")
    pairs = [
        fd.DuplicatePair(a, b, 10, 0.9, 0.8, True),
        fd.DuplicatePair(c, d, 10, 0.9, 0.8, True),
    ]
    output = tmp_path / "out.csv"
    fd.write_csv(str(output), fd.group_rows(pairs))
    lines = output.read_text().splitlines()
    assert len(lines) == 6
    assert lines[3] == ",".join(["-"] * len(fd.CSV_COLUMNS))
    assert [line.split(",")[4] for line in lines[1:3] + lines[4:]] == list("abcd")


def test_chunks_coherent():
    audio = random_audio(1, 20)
    a = ad_print("a", split_in_chunks(audio, [0, 5, 12, 20]))
    b = ad_print("b", split_in_chunks(audio, [0, 5, 12, 20]))
    a.chunks = [
        fd.Fingerprint.from_dict(c) for c in split_in_chunks(audio, [0, 5, 12, 20])
    ]
    b.chunks = list(a.chunks)
    assert fd.chunks_coherent(COMPARE, a, b)
    # the same audio split elsewhere: chunk 1 of 7 s against chunk 1 of 3 s
    b.chunks = [
        fd.Fingerprint.from_dict(c) for c in split_in_chunks(audio, [0, 5, 8, 20])
    ]
    assert not fd.chunks_coherent(COMPARE, a, b)
    # another number of chunks
    b.chunks = b.chunks[:2]
    assert not fd.chunks_coherent(COMPARE, a, b)
    # another audio, split at the same places
    other = split_in_chunks(random_audio(2, 20), [0, 5, 12, 20])
    b.chunks = [fd.Fingerprint.from_dict(c) for c in other]
    assert not fd.chunks_coherent(COMPARE, a, b)


def test_mediatree_link():
    assert fd.mediatree_link("tf1", datetime(2026, 4, 13, 20, 0, 0, 500000), 10.2) == (
        "https://keywords.mediatree.fr/player/?fifo=tf1&start_cts=1776110400"
        "&end_cts=1776110411&position_cts=1776110400"
    )


def test_metabase_query_path():
    assert fd.metabase_query_path("out/dups.csv") == "out/dups_metabase.txt"
    assert (
        fd.metabase_query_path("out/dups.csv", "metabase_multimarques")
        == "out/dups_metabase_multimarques.txt"
    )


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
        # dup-2 is a merged ad: the mosaics of its pieces (piece-b has none)
        session.add(
            Ad_Merge(
                merged_ad_id="dup-2",
                source_ad_ids=["piece-a", "piece-b", "piece-c"],
                method="manual",
                status="active",
            )
        )
        for piece in ("piece-a", "piece-c"):
            session.add(
                Ad_Screenshot(
                    target_type="ad",
                    target_id=piece,
                    ad_id=piece,
                    status="done",
                    image_url=f"https://bucket/screenshots/ads/{piece}.jpg",
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

    rows = fd.group_rows(pairs, COMPARE)
    fd.enrich_rows(connection, rows, 365, now=T0 + timedelta(days=1))
    output = tmp_path / "duplicates.csv"
    fd.write_csv(str(output), rows)
    lines = output.read_text().splitlines()
    # one line per ad of the group, the target (most broadcast) first; dup-1 and dup-2 are split
    # in chunks at different places: not chunk-coherent
    link_1 = (
        "https://keywords.mediatree.fr/player/?fifo=tf1&start_cts=1776110460"
        "&end_cts=1776110480&position_cts=1776110460"
    )
    link_2 = (
        "https://keywords.mediatree.fr/player/?fifo=tf1&start_cts=1776110400"
        "&end_cts=1776110420&position_cts=1776110400"
    )
    sources_header = ",".join(
        f"mosaique_source_{i},mosaique_source_{i}_image" for i in range(1, 6)
    )
    assert lines == [
        "group_id,group_size,mosaique,mosaique_image,ad_id,target_ad_id,brand,sector,"
        "category,nb_occurrences,duration_sec,lien_mediatree,nb_sources_fusion,"
        f"{sources_header},chunk_coherent,chunk_coherent_target",
        "dup-1,2,https://bucket/screenshots/ads/dup-1.jpg,,dup-1,dup-1,Renault,,,2,20.0,"
        f"{link_1},{',' * 10},False,",
        # dup-2 is merged from 3 pieces, piece-b has no mosaic
        f"dup-1,2,,,dup-2,dup-1,RENAULT,,,1,20.0,{link_2},3,"
        "https://bucket/screenshots/ads/piece-a.jpg,,"
        "https://bucket/screenshots/ads/piece-c.jpg,"
        f"{',' * 6},False,False",
    ]
    # no Mediatree link for an occurrence older than the limit
    old_rows = fd.group_rows(pairs)
    fd.enrich_rows(connection, old_rows, 365, now=T0 + timedelta(days=400))
    assert [row["mediatree_link"] for row in old_rows] == [None, None]
    assert old_rows[0]["chunk_coherent"] is None

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

    # groups with several brands: none here (Renault twice)
    assert not any(row["is_multibrand_group"] for row in rows)
    assert connection.execute(text(fd.metabase_multibrand_query(rows, "b"))).all() == []
    # the same pair with another brand on dup-2
    pairs[0].ad_b.meta["brand"] = "Peugeot"
    rows = fd.group_rows(pairs)
    result = (
        # raw SQL, as Metabase runs it (text() would read ":0" of "matches" as a parameter)
        connection.exec_driver_sql(
            fd.metabase_multibrand_query(rows, "advertising-detection")
        )
        .mappings()
        .all()
    )
    assert [(r["ad_id"], r["match_id"], r["marque_match"]) for r in result] == [
        ("dup-1", "dup-2", "Peugeot"),
        ("dup-2", "dup-1", "Renault"),
    ]
    assert result[0]["marques_groupe"] == "Peugeot | Renault"
    assert result[0]["mosaique"] == "https://bucket/screenshots/ads/dup-1.jpg"
    assert result[1]["mosaique_match"] == "https://bucket/screenshots/ads/dup-1.jpg"
    assert result[0]["match_score_min"] > 0.9
    assert result[0]["match_acoustique_compatible"] is True
    assert result[1]["lien_sauvegarde_match"].endswith("/files/ads/dup-1/")
