"""Merge of the ads split by the detection: database operations (tools/ad_merge.py) and unsupervised
script (s04_consolidation/e01_merge_split_ads), on a PostgreSQL database."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import select, text
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
from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.matcher import (
    BrandDictionary,
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
    DICTIONARY_TABLES,
    INHERITED,
    INHERITED_FROM_DICTIONARY,
    INHERITED_FROM_MAJORITY,
    MERGE_STATUS_ACTIVE,
    MERGE_STATUS_REVERTED,
    NOT_CLASSIFIED,
    OTHER_BRAND,
    OTHER_SECTOR,
    SUBCATEGORY_PENDING,
    MergeRevertedError,
    concatenate_chunks,
    create_merged_ad,
    inherit_classification,
    inherit_classification_of_merges,
    inherited_classification,
    load_brand_dictionary,
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


def make_ad(
    ad_id, duration_sec, brand=None, llm_brand=None, content_type="AD", **columns
):
    prediction = [
        {
            "stage": "ad_type",
            "raw_response": {"content_type": content_type, "brand_name": llm_brand},
        }
    ]
    return Ad(
        **{
            "id": ad_id,
            "first_detection_date": T0,
            "duration_sec": duration_sec,
            "chunks": [{"hash": HASH, "fingerprints": [fingerprint(len(ad_id))]}],
            "fragment_type": content_type,
            "prediction": prediction,
            "prediction_status": "dict_tier1" if brand else "dict_miss",
            "predicted_brand": brand,
        }
        | columns
    )


def make_classified_ad(ad_id, duration_sec, brand="Renault", **columns):
    """An advertising classified by the dictionary, sector AUTO, category AUTO_EV."""
    return make_ad(
        ad_id,
        duration_sec,
        brand,
        llm_brand=brand,
        **{
            "predicted_sector": "AUTO",
            "predicted_product_category": "AUTO_EV",
            "prediction_confidence": 0.9,
            "prediction_method": "dict_tier1",
            "transcript": f"transcript of {ad_id}",
        }
        | columns,
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


def piece(**columns):
    """A source ad as read by inherited_classification: a classified advertising, overridden by columns."""
    return SimpleNamespace(
        **{
            "id": "p",
            "duration_sec": 10.0,
            "fragment_type": "AD",
            "transcript": None,
            "prediction": None,
            "prediction_status": "subcat_done",
            "prediction_confidence": None,
            "prediction_method": "llm_subcat",
            "predicted_brand": "Renault",
            "predicted_sector": "AUTO",
            "predicted_product_category": "AUTO_EV",
        }
        | columns
    )


def test_inherited_classification_needs_classified_pieces_that_agree():
    assert (
        inherited_classification([piece(), piece(predicted_brand="RENAULT !")])[1]
        == INHERITED
    )
    for other, reason in [
        (piece(fragment_type="advertising", prediction_status=None), NOT_CLASSIFIED),
        (piece(fragment_type="SPONSORSHIP"), NOT_CLASSIFIED),
        (piece(prediction_status="ad_type_done"), NOT_CLASSIFIED),
        (piece(prediction_status="dict_miss"), SUBCATEGORY_PENDING),
        (piece(predicted_brand="Peugeot"), OTHER_BRAND),
    ]:
        assert inherited_classification([piece(), other]) == (None, reason)
    assert inherited_classification(
        [piece(predicted_brand=None), piece(predicted_brand=None)]
    ) == (None, OTHER_BRAND)
    assert inherited_classification(
        [piece(predicted_sector=None), piece(predicted_sector=None)]
    ) == (None, OTHER_SECTOR)
    # the same empty category on every piece is inherited, like a final status of a merged ad
    values, reason = inherited_classification(
        [
            piece(predicted_product_category=None),
            piece(predicted_product_category=None, prediction_status="merge_inherited"),
        ]
    )
    assert reason == INHERITED and values["predicted_product_category"] is None


def test_create_merged_ad_inherits_the_classification_of_the_pieces(engine):
    insert(
        engine,
        make_classified_ad("a1", 8, "RENAULT", prediction_confidence=0.7),
        make_classified_ad("a2", 12),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        session.commit()
        ad = session.get(Ad, ad_id)
        assert (ad.fragment_type, ad.prediction_status, ad.prediction_method) == (
            "AD",
            "merge_inherited",
            "merge_inherited",
        )
        # the brand as spelled for the longest piece, the lowest confidence
        assert (
            ad.predicted_brand,
            ad.predicted_sector,
            ad.predicted_product_category,
        ) == (
            "Renault",
            "AUTO",
            "AUTO_EV",
        )
        assert ad.prediction_confidence == 0.7
        # a transcript: the transcription stage leaves it alone
        assert ad.transcript == "transcript of a1 transcript of a2"
        assert ad.prediction == [
            {
                "stage": "ad_type",
                "raw_response": {"content_type": "AD", "brand_name": "Renault"},
            },
            {
                "stage": "merge_inherited",
                "source_ad_ids": ["a1", "a2"],
                "source_prediction_statuses": ["dict_tier1", "dict_tier1"],
                "source_prediction_methods": ["dict_tier1", "dict_tier1"],
                "source_classifications": [
                    {
                        "ad_id": "a1",
                        "duration_sec": 8.0,
                        "predicted_sector": "AUTO",
                        "predicted_product_category": "AUTO_EV",
                    },
                    {
                        "ad_id": "a2",
                        "duration_sec": 12.0,
                        "predicted_sector": "AUTO",
                        "predicted_product_category": "AUTO_EV",
                    },
                ],
            },
        ]


def test_create_merged_ad_without_agreement_takes_the_majority(engine):
    insert(
        engine,
        make_classified_ad("a1", 8),
        make_classified_ad(
            "a2", 12, predicted_sector="FOOD", prediction_confidence=0.6
        ),
        make_classified_ad("b1", 8, "Lidl"),
        make_classified_ad("b2", 12, "Lidl", fragment_type="SPONSORSHIP"),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        session.commit()
        ad = session.get(Ad, ad_id)
        assert (
            ad.fragment_type,
            ad.prediction_status,
            ad.prediction_method,
            ad.predicted_sector,
            ad.prediction_confidence,
            ad.transcript,
        ) == (
            "AD",
            "merge_inherited",
            "merge_inherited+majority",
            "FOOD",
            0.6,
            "transcript of a1 transcript of a2",
        )
        merge = ad.prediction[-1]
        assert (merge["rule"], merge["majority_duration_share"]) == (
            INHERITED_FROM_MAJORITY,
            0.6,
        )
        # a piece that is not a classified advertising: the merged ad goes to the classification
        ad_id = create_merged_ad(session, ["b1", "b2"], method="test")
        session.commit()
        ad = session.get(Ad, ad_id)
        assert (ad.fragment_type, ad.prediction_status, ad.transcript) == (
            "advertising",
            None,
            None,
        )


def test_inherited_classification_by_majority_of_the_duration():
    pieces = [
        piece(
            id="p1",
            duration_sec=10.0,
            predicted_product_category="AUTO_EV",
            prediction_confidence=0.9,
        ),
        piece(
            id="p2",
            duration_sec=15.0,
            predicted_product_category="AUTO_THERMAL",
            predicted_brand="RENAULT",
            prediction_confidence=0.5,
        ),
        piece(
            id="p3",
            duration_sec=10.0,
            predicted_product_category="AUTO_EV",
            prediction_confidence=0.7,
        ),
    ]
    # AUTO_EV covers 20 s of 35: brand of its longest piece (the first one), its lowest confidence
    values, reason = inherited_classification(pieces)
    assert reason == INHERITED_FROM_MAJORITY
    assert (
        values["prediction_status"],
        values["prediction_method"],
        values["predicted_brand"],
        values["predicted_product_category"],
        values["prediction_confidence"],
    ) == ("merge_inherited", "merge_inherited+majority", "Renault", "AUTO_EV", 0.7)
    assert values["prediction"][-1]["majority_duration_share"] == 0.571
    assert [
        c["predicted_product_category"]
        for c in values["prediction"][-1]["source_classifications"]
    ] == [
        "AUTO_EV",
        "AUTO_THERMAL",
        "AUTO_EV",
    ]
    # a tie: the classification of the last piece in broadcast order
    tie = [
        piece(duration_sec=15.0, predicted_product_category="AUTO_EV"),
        piece(duration_sec=15.0, predicted_product_category="AUTO_THERMAL"),
    ]
    assert (
        inherited_classification(tie)[0]["predicted_product_category"] == "AUTO_THERMAL"
    )
    # a sector disagreement too
    sectors = [piece(predicted_sector="FOOD", duration_sec=20.0), piece()]
    assert inherited_classification(sectors)[0]["predicted_sector"] == "FOOD"
    # the pieces covering the longest duration have no sector
    no_sector = [piece(predicted_sector=None, duration_sec=20.0), piece()]
    assert inherited_classification(no_sector) == (None, OTHER_SECTOR)


def test_inherit_classification_keeps_a_classification_and_a_transcript(engine):
    insert(engine, make_classified_ad("a1", 8), make_classified_ad("a2", 12))
    with Session(engine) as session:
        sources = [session.get(Ad, "a1"), session.get(Ad, "a2")]
        session.add(
            Ad(
                id="classified",
                first_detection_date=T0,
                duration_sec=20,
                chunks=[],
                fragment_type="AD",
                prediction_status="subcat_done",
                predicted_brand="Renault",
                predicted_sector="FOOD",
            )
        )
        session.add(
            Ad(
                id="transcribed",
                first_detection_date=T0,
                duration_sec=20,
                chunks=[],
                fragment_type="no_data",
                transcript="whole ad",
            )
        )
        session.flush()
        inherit_classification(session, "classified", sources)
        inherit_classification(session, "transcribed", sources)
        session.commit()
        assert session.get(Ad, "classified").predicted_sector == "FOOD"
        transcribed = session.get(Ad, "transcribed")
        assert (transcribed.fragment_type, transcribed.transcript) == ("AD", "whole ad")
        assert transcribed.prediction_status == "merge_inherited"


# tabs of the brand dictionary (Excel file of the classification, or its copy in the database), in their order
DICTIONARY_TIER1 = [
    {"nom_marque": "Lidl", "secteur": "DST", "catégorie": "DST1"},
]
DICTIONARY_TIER2 = [
    {
        "nom_marque": "Renault",
        "secteur": "AUT",
        "catégorie": "AUT1",
        "mots_clés_produit": "modèle électrique ; R5",
    },
    {
        "nom_marque": "Renault",
        "secteur": "AUT",
        "catégorie": "AUT2",
        "mots_clés_produit": "hybride rechargeable ; austral",
    },
    {
        "nom_marque": "Renault",
        "secteur": "AUT",
        "catégorie": "AUT5",
        "mots_clés_produit": "modèle ; SUV",
    },
]
DICTIONARY_TIER3 = [
    {
        "nom_marque": "Auchan",
        "secteur": "FOD",
        "catégorie": "FOD30",
        "mots_clés_produit": "fruits ; légumes",
    },
    {
        "nom_marque": "Auchan",
        "secteur": "DST",
        "catégorie": "DST1",
        "mots_clés_produit": "magasin",
    },
]


def brand_dictionary() -> BrandDictionary:
    return BrandDictionary.from_frames(
        pd.DataFrame(DICTIONARY_TIER1),
        pd.DataFrame(DICTIONARY_TIER2),
        pd.DataFrame(DICTIONARY_TIER3),
    )


def test_inherited_classification_uses_the_dictionary_on_the_whole_transcript():
    # each piece was classified on its own part of the transcript: "SUV" alone gave AUT5
    pieces = [
        piece(id="p1", predicted_product_category="AUT5", transcript="le nouveau SUV"),
        piece(
            id="p2",
            duration_sec=20.0,
            predicted_product_category="AUT2",
            prediction_method="dict_tier2",
            transcript="Austral, hybride rechargeable",
            prediction=[
                {
                    "stage": "ad_type",
                    "raw_response": {"brand_name": "Renault", "product_name": None},
                }
            ],
        ),
    ]
    # without the dictionary: the majority of the duration (p2, 20 s of 30)
    assert inherited_classification(pieces)[1] == INHERITED_FROM_MAJORITY
    values, reason = inherited_classification(pieces, brand_dictionary())
    assert reason == INHERITED_FROM_DICTIONARY
    # the first rule of the brand with a keyword hit in the whole transcript, like stage 3
    assert {
        k: values[k]
        for k in [
            "prediction_status",
            "prediction_method",
            "fragment_type",
            "predicted_brand",
            "predicted_sector",
            "predicted_product_category",
            "prediction_confidence",
            "transcript",
        ]
    } == {
        "prediction_status": "dict_tier2",
        "prediction_method": "merge_inherited+dict_tier2",
        "fragment_type": "AD",
        "predicted_brand": "Renault",
        "predicted_sector": "AUT",
        "predicted_product_category": "AUT2",
        "prediction_confidence": None,
        "transcript": "le nouveau SUV Austral, hybride rechargeable",
    }
    ad_type, dict_match, merge = values["prediction"]
    assert ad_type["stage"] == "ad_type"
    assert (
        dict_match["stage"],
        dict_match["method"],
        dict_match["matched_keyword"],
    ) == (
        "dict_match",
        "dict_tier2",
        "hybride rechargeable",
    )
    assert merge == {
        "stage": "merge_inherited",
        "source_ad_ids": ["p1", "p2"],
        "source_prediction_statuses": ["subcat_done", "subcat_done"],
        "source_prediction_methods": ["llm_subcat", "dict_tier2"],
        "source_classifications": [
            {
                "ad_id": "p1",
                "duration_sec": 10.0,
                "predicted_sector": "AUTO",
                "predicted_product_category": "AUT5",
            },
            {
                "ad_id": "p2",
                "duration_sec": 20.0,
                "predicted_sector": "AUTO",
                "predicted_product_category": "AUT2",
            },
        ],
        "rule": INHERITED_FROM_DICTIONARY,
    }


def test_inherited_classification_by_the_dictionary_needs_a_decision():
    dictionary = brand_dictionary()
    # tier 2 without keyword: the dictionary gives the sector only, the majority of the duration decides
    no_keyword = [
        piece(predicted_product_category="AUT1", transcript="la voiture"),
        piece(predicted_product_category="AUT2", transcript="de demain"),
    ]
    assert (
        inherited_classification(no_keyword, dictionary)[1] == INHERITED_FROM_MAJORITY
    )
    # tier 3: a keyword gives the sector and the category
    retail = [
        piece(
            predicted_brand="Auchan",
            predicted_sector="DST",
            predicted_product_category="DST1",
            transcript="chez Auchan",
        ),
        piece(
            predicted_brand="Auchan",
            predicted_sector="FOD",
            predicted_product_category="FOD30",
            transcript="les fruits de saison",
        ),
    ]
    values, reason = inherited_classification(retail, dictionary)
    assert reason == INHERITED_FROM_DICTIONARY
    assert (
        values["prediction_status"],
        values["predicted_sector"],
        values["predicted_product_category"],
    ) == (
        "dict_tier3",
        "FOD",
        "FOD30",
    )
    # tier 1: the brand alone
    lidl = [
        piece(
            predicted_brand="Lidl",
            predicted_sector="FOD",
            predicted_product_category="FOD30",
        ),
        piece(
            predicted_brand="Lidl",
            predicted_sector="DST",
            predicted_product_category="DST1",
        ),
    ]
    values, reason = inherited_classification(lidl, dictionary)
    assert (
        reason,
        values["prediction_status"],
        values["predicted_product_category"],
    ) == (
        INHERITED_FROM_DICTIONARY,
        "dict_tier1",
        "DST1",
    )
    # a brand out of the dictionary
    other = [
        piece(predicted_brand="Inconnue"),
        piece(predicted_brand="Inconnue", predicted_sector="FOD"),
    ]
    assert inherited_classification(other, dictionary)[1] == INHERITED_FROM_MAJORITY
    # pieces that agree: their classification, whatever the dictionary says
    assert inherited_classification([piece(), piece()], dictionary)[1] == INHERITED


@pytest.fixture
def dictionary_tables(engine):
    with engine.begin() as connection:
        for table, rows in zip(
            DICTIONARY_TABLES, [DICTIONARY_TIER1, DICTIONARY_TIER2, DICTIONARY_TIER3]
        ):
            connection.execute(text(f"DROP TABLE IF EXISTS advertising.{table}"))
            pd.DataFrame(rows).to_sql(
                table, connection, schema="advertising", index=False
            )
    yield
    with engine.begin() as connection:
        for table in DICTIONARY_TABLES:
            connection.execute(text(f"DROP TABLE IF EXISTS advertising.{table}"))


def test_load_brand_dictionary_reads_the_copy_in_the_database(
    engine, dictionary_tables
):
    with Session(engine) as session:
        dictionary = load_brand_dictionary(session)
    # rules in the order of the rows: "modèle électrique" (AUT1) comes before "modèle" (AUT5)
    match = dictionary.match("Renault", None, "un modèle électrique")
    assert (match.method, match.subcat_code) == ("dict_tier2", "AUT1")
    assert dictionary.match("Lidl").method == "dict_tier1"


def test_load_brand_dictionary_without_tables(engine):
    with engine.begin() as connection:
        for table in DICTIONARY_TABLES:
            connection.execute(text(f"DROP TABLE IF EXISTS advertising.{table}"))
    with Session(engine) as session:
        assert load_brand_dictionary(session) is None


def test_run_inherit_only_classifies_with_the_dictionary(
    engine, dictionary_tables, monkeypatch
):
    insert(
        engine,
        make_classified_ad(
            "a1", 8, predicted_product_category="AUT5", transcript="le nouveau SUV"
        ),
        make_classified_ad(
            "a2",
            12,
            predicted_product_category="AUT2",
            transcript="hybride rechargeable",
            prediction_status="ad_type_done",
        ),
    )
    with Session(engine) as session:
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        session.commit()
        assert session.get(Ad, ad_id).prediction_status is None
        # a2 classified after the merge
        session.get(Ad, "a2").prediction_status = "subcat_done"
        session.commit()
    monkeypatch.setenv("AD_MERGE_INHERIT_ONLY", "true")
    monkeypatch.setenv("AD_MERGE_DRY_RUN", "false")
    merge_run.run()
    with Session(engine) as session:
        ad = session.get(Ad, ad_id)
        assert (
            ad.prediction_status,
            ad.prediction_method,
            ad.predicted_product_category,
        ) == (
            "dict_tier2",
            "merge_inherited+dict_tier2",
            "AUT2",
        )
        assert ad.transcript == "le nouveau SUV hybride rechargeable"


def test_inherit_classification_of_merges_backfills_the_merged_ads(engine):
    insert(
        engine,
        make_classified_ad("a1", 8),
        make_classified_ad("a2", 12, prediction_status="ad_type_done"),
        make_classified_ad("b1", 8, "Lidl"),
        make_classified_ad("b2", 12, "Lidl", prediction_status="ad_type_done"),
    )
    with Session(engine) as session:
        a = create_merged_ad(session, ["a1", "a2"], method="test")
        b = create_merged_ad(session, ["b1", "b2"], method="test")
        session.commit()
        assert session.get(Ad, a).prediction_status is None
        # a2 classified after the merge
        session.get(Ad, "a2").prediction_status = "dict_tier1"
        session.commit()

        assert inherit_classification_of_merges(session, dry_run=True) == {
            INHERITED: 1,
            NOT_CLASSIFIED: 1,
        }
        session.rollback()
        assert session.get(Ad, a).prediction_status is None

        assert inherit_classification_of_merges(session, batch_size=1) == {
            INHERITED: 1,
            NOT_CLASSIFIED: 1,
        }
        session.commit()
        assert session.get(Ad, a).prediction_status == "merge_inherited"
        assert session.get(Ad, b).prediction_status is None
        # done once: only b is left
        assert inherit_classification_of_merges(session) == {NOT_CLASSIFIED: 1}


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


def test_run_inherit_only_classifies_the_merged_ads(engine, monkeypatch):
    insert(
        engine,
        make_classified_ad("a1", 8),
        make_classified_ad("a2", 12, transcript=None),
    )
    with Session(engine) as session:
        session.get(Ad, "a2").prediction_status = "ad_type_done"
        session.flush()
        ad_id = create_merged_ad(session, ["a1", "a2"], method="test")
        session.get(Ad, "a2").prediction_status = "dict_tier1"
        session.commit()
    monkeypatch.setenv("AD_MERGE_INHERIT_ONLY", "true")
    monkeypatch.setenv("AD_MERGE_DRY_RUN", "true")
    assert merge_run.run() == 0
    with Session(engine) as session:
        assert session.get(Ad, ad_id).prediction_status is None
    monkeypatch.setenv("AD_MERGE_DRY_RUN", "false")
    assert merge_run.run() == 0
    with Session(engine) as session:
        ad = session.get(Ad, ad_id)
        assert ad.prediction_status == "merge_inherited"
        assert ad.transcript == "transcript of a1"


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
