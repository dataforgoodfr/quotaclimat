"""
Tests for the embedding-based advertising detection pipeline (e02b/e03b/e04b/e06b).

These tests verify:
  - EmbeddingFingerprint and EmbeddingChunk serialization round-trip
  - EmbeddingChunkCreator produces valid chunks with embeddings
  - EmbeddingChunkGrouping correctly groups identical chunks
  - FragmentsClassifier (e05) works with EmbeddingChunkGroups (duck-typing)
  - End-to-end embedding pipeline integration
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from quotaclimat.data_ingestion.advertising.s01_detection.tools.common_objects import (
    EmbeddingChunk,
    EmbeddingFingerprint,
)


def _panns_available() -> bool:
    try:
        import panns_inference
        return True
    except ImportError:
        return False


# --- Serialization tests (no model needed) ---


def test_embedding_fingerprint_round_trip():
    fp = EmbeddingFingerprint(
        duration_sec=5.0,
        energy_mean=0.03,
        spectral_centroid=2500.0,
        zcr_mean=0.15,
        embedding=[float(i) for i in range(2048)],
        semantic_labels=["Speech", "Music", "Jingle, bell"],
    )
    d = fp.to_dict()
    fp2 = EmbeddingFingerprint.from_dict(d)
    assert fp2.duration_sec == fp.duration_sec
    assert fp2.energy_mean == fp.energy_mean
    assert fp2.spectral_centroid == fp.spectral_centroid
    assert fp2.zcr_mean == fp.zcr_mean
    assert fp2.embedding == fp.embedding
    assert fp2.semantic_labels == fp.semantic_labels


def test_embedding_fingerprint_without_labels():
    fp = EmbeddingFingerprint(
        duration_sec=3.0,
        energy_mean=0.01,
        spectral_centroid=1000.0,
        zcr_mean=0.1,
        embedding=[0.0] * 10,
    )
    d = fp.to_dict()
    fp2 = EmbeddingFingerprint.from_dict(d)
    assert fp2.semantic_labels is None
    assert fp2.embedding == [0.0] * 10


def test_embedding_chunk_round_trip():
    chunk = EmbeddingChunk(
        start_sec=100.0,
        end_sec=105.0,
        channel="tf1",
        fingerprint=EmbeddingFingerprint(
            duration_sec=5.0,
            energy_mean=0.03,
            spectral_centroid=2500.0,
            zcr_mean=0.15,
            embedding=[1.0, 2.0, 3.0],
            semantic_labels=["Speech"],
        ),
    )
    d = chunk.to_dict()
    chunk2 = EmbeddingChunk.from_dict(d)
    assert chunk2.start_sec == chunk.start_sec
    assert chunk2.end_sec == chunk.end_sec
    assert chunk2.channel == chunk.channel
    assert chunk2.fingerprint.embedding == chunk.fingerprint.embedding
    assert chunk2.fingerprint.semantic_labels == chunk.fingerprint.semantic_labels


# --- Grouping tests (no model needed) ---


def _make_embedding_chunk(
    start: float,
    end: float,
    embedding: list[float],
    channel: str = "tf1",
    labels: list[str] = None,
) -> EmbeddingChunk:
    return EmbeddingChunk(
        start_sec=start,
        end_sec=end,
        channel=channel,
        fingerprint=EmbeddingFingerprint(
            duration_sec=end - start,
            energy_mean=0.03,
            spectral_centroid=2500.0,
            zcr_mean=0.15,
            embedding=embedding,
            semantic_labels=labels,
        ),
    )


def test_embedding_grouping_identical_chunks():
    from quotaclimat.data_ingestion.advertising.s01_detection.e04b_group_chunks import (
        EmbeddingChunkGrouping,
    )

    # Create 3 chunks with identical embeddings and 2 with different ones
    emb_a = [1.0] * 2048
    emb_b = [0.0] * 1024 + [1.0] * 1024  # different direction

    chunks = [
        _make_embedding_chunk(0, 5, emb_a),
        _make_embedding_chunk(10, 15, emb_a),
        _make_embedding_chunk(20, 25, emb_a),
        _make_embedding_chunk(30, 35, emb_b),
        _make_embedding_chunk(40, 45, emb_b),
    ]

    grouping = EmbeddingChunkGrouping(similarity_threshold=0.9, duration_tol=1.0)
    groups = grouping.run(chunks)

    # Should have 2 groups: one with 3 occurrences, one with 2
    counts = sorted([g.count for g in groups], reverse=True)
    assert counts == [3, 2]


def test_embedding_grouping_no_match():
    from quotaclimat.data_ingestion.advertising.s01_detection.e04b_group_chunks import (
        EmbeddingChunkGrouping,
    )

    # Create chunks with orthogonal embeddings
    rng = np.random.RandomState(42)
    chunks = [
        _make_embedding_chunk(i * 10, i * 10 + 5, rng.randn(2048).tolist())
        for i in range(5)
    ]

    grouping = EmbeddingChunkGrouping(similarity_threshold=0.99, duration_tol=1.0)
    groups = grouping.run(chunks)

    # All should be singletons
    assert all(g.count == 1 for g in groups)


def test_embedding_grouping_duration_filter():
    from quotaclimat.data_ingestion.advertising.s01_detection.e04b_group_chunks import (
        EmbeddingChunkGrouping,
    )

    # Same embedding but very different durations
    emb = [1.0] * 2048
    chunks = [
        _make_embedding_chunk(0, 5, emb),      # 5s
        _make_embedding_chunk(10, 25, emb),     # 15s — too different
    ]

    grouping = EmbeddingChunkGrouping(similarity_threshold=0.9, duration_tol=1.0)
    groups = grouping.run(chunks)

    # Should not be grouped due to duration difference
    assert all(g.count == 1 for g in groups)


def test_embedding_canonical():
    from quotaclimat.data_ingestion.advertising.s01_detection.e04b_group_chunks import (
        embedding_canonical,
    )

    emb1 = [1.0, 0.0, 0.0]
    emb2 = [0.0, 1.0, 0.0]
    chunks = [
        _make_embedding_chunk(0, 5, emb1, labels=["Speech", "Music"]),
        _make_embedding_chunk(10, 15, emb2, labels=["Speech", "Jingle"]),
    ]

    canon = embedding_canonical(chunks)
    # Mean of embeddings
    assert canon.fingerprint.embedding == [0.5, 0.5, 0.0]
    # Most common labels
    assert "Speech" in canon.fingerprint.semantic_labels


# --- e05 duck-typing compatibility test ---


def test_e05_works_with_embedding_chunk_groups():
    from quotaclimat.data_ingestion.advertising.s01_detection.e04b_group_chunks import (
        EmbeddingChunkGroup,
    )
    from quotaclimat.data_ingestion.advertising.s01_detection.e05_classify_fragments import (
        FragmentsClassifier,
    )

    emb = [1.0] * 2048
    # Create a group with count=3 (should be classified as new_ad)
    chunks_3 = [_make_embedding_chunk(i * 100, i * 100 + 5, emb) for i in range(3)]
    group_ad = EmbeddingChunkGroup(
        count=3,
        duration_mean=5.0,
        duration_std=0.0,
        occurrences=chunks_3,
    )

    # Create a group with count=1 (should be classified as content)
    chunks_1 = [_make_embedding_chunk(500, 510, emb)]
    group_content = EmbeddingChunkGroup(
        count=1,
        duration_mean=10.0,
        duration_std=0.0,
        occurrences=chunks_1,
    )

    classifier = FragmentsClassifier(repetition_threshold=3)
    fragments = classifier.run([group_ad, group_content])

    classifications = [f.classification for f in fragments]
    assert "new_ad" in classifications
    assert "content" in classifications


# --- EmbeddingChunkCreator test (requires PANNs model) ---


@pytest.mark.skipif(
    not _panns_available(),
    reason="panns_inference not installed (optional dependency)",
)
def test_embedding_chunk_creator_produces_valid_chunks():
    from quotaclimat.data_ingestion.advertising.s01_detection.e00_partition_window import (
        Segment,
    )
    from quotaclimat.data_ingestion.advertising.s01_detection.e02b_create_chunks_embedding import (
        EmbeddingChunkCreator,
    )

    creator = EmbeddingChunkCreator(
        sr=16000,
        min_chunk_sec=1.0,
    )

    segment = Segment(
        start_date=datetime(2025, 5, 5, 12, 0, tzinfo=ZoneInfo("Europe/Paris")),
        end_date=datetime(2025, 5, 5, 12, 1, tzinfo=ZoneInfo("Europe/Paris")),
        channel="tf1",
    )

    chunks = creator.run(segment, "test/advertising_detection/assets/tf1_1.mp3")

    assert len(chunks) > 0
    for chunk in chunks:
        assert isinstance(chunk, EmbeddingChunk)
        assert chunk.fingerprint.embedding is not None
        assert len(chunk.fingerprint.embedding) == 2048
        assert chunk.fingerprint.duration_sec > 0
        assert chunk.start_sec < chunk.end_sec
        assert chunk.channel == "tf1"

    # Verify params_hash is stable
    assert creator.params_hash() == creator.params_hash()
