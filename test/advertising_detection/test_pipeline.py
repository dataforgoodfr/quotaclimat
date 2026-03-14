"""
Tests for the advertising detection offline pipeline (e02 + e03).

Requires test audio/video files in test/advertising_detection/test_files/.
No network calls — everything runs locally.
"""

import pytest
from pathlib import Path

from quotaclimat.data_ingestion.advertising_detection.e02_create_segments import (
    HashGenerator,
    Segment,
    SegmentCreator,
)
from quotaclimat.data_ingestion.advertising_detection.e03_group_segments import (
    Fingerprint,
    FingerprintMatcher,
    SegmentGroupingPipeline,
)
from quotaclimat.data_ingestion.advertising_detection.offline_pipeline import (
    create_segments,
    group_segments,
    run_offline_pipeline,
)

TEST_FILES_DIR = Path(__file__).parent / "test_files"


def _find_test_audio():
    """Find any audio/video file in test_files/ that librosa can load."""
    extensions = ("*.mp3", "*.wav", "*.ogg", "*.flac", "*.mp4", "*.m4a", "*.webm")
    for ext in extensions:
        files = list(TEST_FILES_DIR.glob(ext))
        if files:
            return files
    return []


@pytest.fixture(scope="module")
def test_audio_files():
    files = _find_test_audio()
    if not files:
        pytest.skip(
            f"No audio/video files found in {TEST_FILES_DIR}. "
            "Add test files (mp3, wav, mp4, etc.) to run these tests."
        )
    return [str(f) for f in files]


@pytest.fixture(scope="module")
def test_audio_file(test_audio_files):
    return test_audio_files[0]


# ─────────────────────────────────────────────
#  e02: Segmentation + hashing
# ─────────────────────────────────────────────


class TestSegmentCreator:
    def test_creates_segments(self, test_audio_file):
        segments = create_segments(test_audio_file)
        assert len(segments) > 0

    def test_segment_fields(self, test_audio_file):
        segments = create_segments(test_audio_file)
        for seg in segments:
            assert isinstance(seg, Segment)
            assert seg.duration_sec > 0
            assert seg.start_sec >= 0
            assert seg.end_sec > seg.start_sec
            assert seg.energy_mean >= 0
            assert seg.spectral_centroid >= 0
            assert seg.zcr_mean >= 0

    def test_peaks_computed(self, test_audio_file):
        segments = create_segments(test_audio_file)
        has_peaks = any(seg.peaks for seg in segments)
        assert has_peaks, "At least some segments should have constellation peaks"

    def test_hashes_precomputed(self, test_audio_file):
        """Hashes should be computed in e02, not deferred to e03."""
        segments = create_segments(test_audio_file)
        has_hashes = any(seg.hashes for seg in segments)
        assert has_hashes, "At least some segments should have pre-computed hashes"

    def test_hash_format(self, test_audio_file):
        segments = create_segments(test_audio_file)
        for seg in segments:
            if seg.hashes:
                for h, t in seg.hashes:
                    assert isinstance(h, str) and len(h) == 12
                    assert isinstance(t, int) and t >= 0

    def test_segment_serialization_roundtrip(self, test_audio_file):
        segments = create_segments(test_audio_file)
        for seg in segments:
            d = seg.to_dict()
            restored = Segment.from_dict(d)
            assert restored.start_sec == seg.start_sec
            assert restored.end_sec == seg.end_sec
            assert restored.hashes == seg.hashes
            assert restored.peaks == seg.peaks


# ─────────────────────────────────────────────
#  e03: Grouping (uses pre-computed hashes)
# ─────────────────────────────────────────────


class TestGrouping:
    def test_fingerprint_from_segment(self, test_audio_file):
        """e03 should build fingerprints from pre-computed hashes without recomputing."""
        segments = create_segments(test_audio_file)
        pipeline = SegmentGroupingPipeline()
        fingerprints = pipeline.fingerprint_source(segments)

        assert len(fingerprints) > 0
        for fp in fingerprints:
            assert isinstance(fp, Fingerprint)
            assert fp.duration_sec > 0

    def test_fingerprint_hashes_match_segment(self, test_audio_file):
        """Fingerprint hashes should come directly from the segment (no recomputation)."""
        segments = create_segments(test_audio_file)
        pipeline = SegmentGroupingPipeline()
        fingerprints = pipeline.fingerprint_source(segments)

        for fp in fingerprints:
            seg = segments[fp.segment_index]
            expected_hashes = seg.hashes if seg.hashes else []
            assert fp.hashes == expected_hashes

    def test_group_segments_returns_list(self, test_audio_file):
        segments = create_segments(test_audio_file)
        groups = group_segments([segments])
        assert isinstance(groups, list)

    def test_same_audio_twice_produces_matches(self, test_audio_files):
        """Feeding the same audio twice should produce groups with count > 1."""
        audio = test_audio_files[0]
        segments1 = create_segments(audio, start_epoch=0.0)
        segments2 = create_segments(audio, start_epoch=100000.0)
        groups = group_segments([segments1, segments2])

        multi_occurrence = [g for g in groups if g["count"] > 1]
        assert len(multi_occurrence) > 0, (
            "Same audio fed twice should produce at least one group with multiple occurrences"
        )


# ─────────────────────────────────────────────
#  Full offline pipeline
# ─────────────────────────────────────────────


class TestOfflinePipeline:
    def test_run_single_file(self, test_audio_file):
        result = run_offline_pipeline([test_audio_file])
        assert "segments_list" in result
        assert "groups" in result
        assert len(result["segments_list"]) == 1
        assert len(result["segments_list"][0]) > 0

    def test_run_multiple_files(self, test_audio_files):
        if len(test_audio_files) < 2:
            pytest.skip("Need at least 2 test files")
        result = run_offline_pipeline(test_audio_files[:2])
        assert len(result["segments_list"]) == 2

    def test_custom_start_epochs(self, test_audio_file):
        result = run_offline_pipeline(
            [test_audio_file],
            start_epochs=[1000.0],
        )
        for seg in result["segments_list"][0]:
            assert seg.start_sec >= 1000.0


# ─────────────────────────────────────────────
#  HashGenerator unit tests (no audio needed)
# ─────────────────────────────────────────────


class TestHashGenerator:
    def test_empty_peaks(self):
        import numpy as np

        hg = HashGenerator()
        assert hg.generate(np.empty((0, 2), dtype=np.int32)) == []

    def test_single_peak(self):
        import numpy as np

        hg = HashGenerator()
        assert hg.generate(np.array([[5, 100]], dtype=np.int32)) == []

    def test_deterministic(self):
        import numpy as np

        peaks = np.array([[0, 100], [5, 200], [10, 300]], dtype=np.int32)
        hg = HashGenerator()
        h1 = hg.generate(peaks)
        h2 = hg.generate(peaks)
        assert h1 == h2

    def test_hash_length(self):
        import numpy as np

        peaks = np.array([[0, 100], [5, 200]], dtype=np.int32)
        hg = HashGenerator()
        hashes = hg.generate(peaks)
        assert len(hashes) > 0
        for h, t in hashes:
            assert len(h) == 12
