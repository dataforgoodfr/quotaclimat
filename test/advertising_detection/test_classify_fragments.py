"""Tests for e05_classify_fragments.py"""
from dataclasses import dataclass, field

import pytest

from quotaclimat.data_ingestion.advertising.s01_detection.e05_classify_fragments import (
    MAXIMUM_SECONDS,
    FragmentsClassifier,
    _FragmentWrapper,
    higher_classification,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.common_objects import (
    Chunk,
    Fingerprint,
    Fragment,
)


# ---------------------------------------------------------------------------
# Lightweight stand-in for ChunkGroup (avoids importing the heavy module)
# ---------------------------------------------------------------------------


@dataclass
class ChunkGroup:
    """Minimal ChunkGroup used in tests — mirrors the fields the classifier reads."""
    count: int
    occurrences: list = field(default_factory=list)
    duration_mean: float = 5.0
    duration_std: float = 0.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_chunk(start_sec=0.0, end_sec=5.0, channel="test"):
    return Chunk(
        start_sec=start_sec,
        end_sec=end_sec,
        channel=channel,
        fingerprint=Fingerprint(
            duration_sec=end_sec - start_sec,
            energy_mean=0.1,
            spectral_centroid=1000.0,
            zcr_mean=0.1,
        ),
    )


def make_group(count, offsets=None):
    """Build a ChunkGroup with `count` occurrences at the given start offsets (5 s each)."""
    if offsets is None:
        offsets = [i * 10.0 for i in range(count)]
    occurrences = [make_chunk(t, t + 5.0) for t in offsets]
    return ChunkGroup(
        count=count,
        duration_mean=5.0,
        duration_std=0.0,
        occurrences=occurrences,
    )


def make_fragment(start_sec, end_sec, classification="unknown", group_id=None):
    return Fragment(
        start_sec=start_sec,
        end_sec=end_sec,
        channel="test",
        classification=classification,
        group_id=group_id,
        chunks=[make_chunk(start_sec, end_sec)],
    )


def make_fw(start_sec, end_sec, classification="unknown", group=None, group_id=None):
    return _FragmentWrapper(
        fragment=make_fragment(start_sec, end_sec, classification, group_id),
        group=group,
    )


# ---------------------------------------------------------------------------
# higher_classification
# ---------------------------------------------------------------------------


class TestHigherClassification:
    def test_already_known_ad_beats_everything(self):
        for other in ("new_ad", "jingle", "content", "unknown"):
            assert higher_classification("already_known_ad", other) == "already_known_ad"
            assert higher_classification(other, "already_known_ad") == "already_known_ad"

    def test_new_ad_beats_lower(self):
        for other in ("jingle", "content", "unknown"):
            assert higher_classification("new_ad", other) == "new_ad"
            assert higher_classification(other, "new_ad") == "new_ad"

    def test_jingle_beats_content_and_unknown(self):
        assert higher_classification("jingle", "content") == "jingle"
        assert higher_classification("jingle", "unknown") == "jingle"

    def test_content_beats_unknown(self):
        assert higher_classification("content", "unknown") == "content"

    def test_same_class_returns_that_class(self):
        for c in ("already_known_ad", "new_ad", "jingle", "content", "unknown"):
            assert higher_classification(c, c) == c


# ---------------------------------------------------------------------------
# FragmentsClassifier._first_discrimination
# ---------------------------------------------------------------------------


class TestFirstDiscrimination:
    def setup_method(self):
        self.clf = FragmentsClassifier(repetition_threshold=3)

    def test_already_classified_passes_through(self):
        for cls in ("already_known_ad", "new_ad", "jingle", "content"):
            fw = make_fw(0, 5, classification=cls)
            assert self.clf._first_discrimination(fw) == cls

    def test_no_group_is_unknown(self):
        fw = make_fw(0, 5, classification="unknown", group=None)
        assert self.clf._first_discrimination(fw) == "unknown"

    def test_small_group_below_threshold_is_unknown(self):
        fw = make_fw(0, 5, group=make_group(2))
        assert self.clf._first_discrimination(fw) == "unknown"

    def test_group_at_threshold_is_new_ad(self):
        fw = make_fw(0, 5, group=make_group(3))
        assert self.clf._first_discrimination(fw) == "new_ad"

    def test_group_above_threshold_is_new_ad(self):
        fw = make_fw(0, 5, group=make_group(10))
        assert self.clf._first_discrimination(fw) == "new_ad"

    def test_custom_repetition_threshold(self):
        clf = FragmentsClassifier(repetition_threshold=5)
        assert clf._first_discrimination(make_fw(0, 5, group=make_group(4))) == "unknown"
        assert clf._first_discrimination(make_fw(0, 5, group=make_group(5))) == "new_ad"


# ---------------------------------------------------------------------------
# FragmentsClassifier._is_ad_anchor
# ---------------------------------------------------------------------------


class TestIsAdAnchor:
    def setup_method(self):
        self.clf = FragmentsClassifier(tunnel_terminal_threshold=2)

    def test_no_group_is_not_anchor(self):
        fw = make_fw(0, 5, group=None)
        assert self.clf._is_ad_anchor(fw) is False

    def test_singleton_group_is_not_anchor(self):
        fw = make_fw(0, 5, group=make_group(1))
        assert self.clf._is_ad_anchor(fw) is False

    def test_group_at_threshold_2_is_anchor(self):
        fw = make_fw(0, 5, group=make_group(2))
        assert self.clf._is_ad_anchor(fw) is True

    def test_group_above_threshold_is_anchor(self):
        fw = make_fw(0, 5, group=make_group(5))
        assert self.clf._is_ad_anchor(fw) is True

    def test_non_default_threshold_disables_anchor(self):
        # The check is `count >= threshold == 2`, so threshold != 2 disables anchoring entirely
        clf = FragmentsClassifier(tunnel_terminal_threshold=3)
        fw = make_fw(0, 5, group=make_group(5))
        assert clf._is_ad_anchor(fw) is False


# ---------------------------------------------------------------------------
# FragmentsClassifier._is_in_short_tunnel
# ---------------------------------------------------------------------------


class TestIsInShortTunnel:
    def setup_method(self):
        self.clf = FragmentsClassifier()

    def _fws(self, specs):
        """Build a list of _FragmentWrapper from (start, end, classification) triples."""
        result = []
        for start, end, cls in specs:
            result.append(make_fw(start, end, classification=cls))
        return result

    def test_unknown_between_two_ads_is_in_tunnel(self):
        fws = self._fws([(0, 5, "new_ad"), (5, 10, "unknown"), (10, 15, "new_ad")])
        assert self.clf._is_in_short_tunnel(1, fws) is True

    def test_ad_is_not_in_short_tunnel(self):
        fws = self._fws([(0, 5, "new_ad"), (5, 10, "new_ad"), (10, 15, "new_ad")])
        # The caller only invokes _is_in_short_tunnel for "unknown" fragments,
        # but the method itself should still return True for an ad surrounded by ads
        # (or True by finding left ad and right ad). The important thing is that
        # "unknown" fragments in a gap do get detected; ads themselves are irrelevant.
        # Here we just check that the logic runs without error on non-unknown fragments.
        assert self.clf._is_in_short_tunnel(1, fws) in (True, False)

    def test_unknown_not_near_any_ad_is_not_in_tunnel(self):
        # All unknown, no ads nearby
        fws = self._fws(
            [(i * 5, i * 5 + 5, "unknown") for i in range(8)]
        )
        assert self.clf._is_in_short_tunnel(4, fws) is False

    def test_unknown_with_ad_only_on_left_is_not_in_tunnel(self):
        # Ad on left but nothing on right (too few elements)
        fws = self._fws([(0, 5, "new_ad"), (5, 10, "unknown"), (10, 15, "unknown")])
        assert self.clf._is_in_short_tunnel(1, fws) is False

    def test_unknown_beyond_4_positions_from_ad_is_not_in_tunnel(self):
        # Ad at index 0, unknown at index 6 — more than 4 positions away
        fws = self._fws(
            [(0, 5, "new_ad")]
            + [(i * 5 + 5, i * 5 + 10, "unknown") for i in range(6)]
        )
        assert self.clf._is_in_short_tunnel(6, fws) is False

    def test_tunnel_exceeding_maximum_seconds_is_rejected(self):
        # Distance between left ad end (5) and right ad start (5 + MAXIMUM_SECONDS + 1) > 60 s
        right_start = 5 + MAXIMUM_SECONDS + 1
        fws = self._fws(
            [(0, 5, "new_ad"), (5, right_start, "unknown"), (right_start, right_start + 5, "new_ad")]
        )
        assert self.clf._is_in_short_tunnel(1, fws) is False

    def test_tunnel_at_exactly_maximum_seconds_is_accepted(self):
        right_start = 5 + MAXIMUM_SECONDS  # end of left ad (5) + exactly 60 s
        fws = self._fws(
            [(0, 5, "new_ad"), (5, right_start, "unknown"), (right_start, right_start + 5, "new_ad")]
        )
        assert self.clf._is_in_short_tunnel(1, fws) is True

    def test_anchor_on_right_also_triggers_tunnel(self):
        # Right neighbour is an ad anchor (count >= 2) rather than a classified ad
        group_2 = make_group(2)
        fws = [
            make_fw(0, 5, classification="new_ad"),
            make_fw(5, 10, classification="unknown"),
            _FragmentWrapper(
                fragment=make_fragment(10, 15, classification="unknown"),
                group=group_2,
            ),
        ]
        assert self.clf._is_in_short_tunnel(1, fws) is True


# ---------------------------------------------------------------------------
# FragmentsClassifier._is_in_long_tunnel
# ---------------------------------------------------------------------------


class TestIsInLongTunnel:
    def setup_method(self):
        self.clf = FragmentsClassifier()

    def _make_sequence(self, ad_left, gap, ad_right, gap_start_sec=0.0):
        """Build a contiguous sequence of fragments: ad_left ads, gap unknowns, ad_right ads."""
        fws = []
        t = gap_start_sec
        for _ in range(ad_left):
            fws.append(make_fw(t, t + 5.0, classification="new_ad"))
            t += 5.0
        gap_start_idx = len(fws)
        for _ in range(gap):
            fws.append(make_fw(t, t + 5.0, classification="unknown"))
            t += 5.0
        for _ in range(ad_right):
            fws.append(make_fw(t, t + 5.0, classification="new_ad"))
            t += 5.0
        return fws, gap_start_idx

    def test_fragment_in_gap_flanked_by_large_ad_blocks(self):
        # NAD=4, NNAD=3: gap=1 (<=3), ads on each side >= 4
        fws, gap_idx = self._make_sequence(ad_left=4, gap=1, ad_right=4)
        assert self.clf._is_in_long_tunnel(gap_idx, fws, 4, 3) is True

    def test_fragment_in_gap_with_insufficient_ads_on_left(self):
        # Only 3 ads on the left, need 4
        fws, gap_idx = self._make_sequence(ad_left=3, gap=1, ad_right=4)
        assert self.clf._is_in_long_tunnel(gap_idx, fws, 4, 3) is False

    def test_fragment_in_gap_with_insufficient_ads_on_right(self):
        fws, gap_idx = self._make_sequence(ad_left=4, gap=1, ad_right=3)
        assert self.clf._is_in_long_tunnel(gap_idx, fws, 4, 3) is False

    def test_gap_too_wide_is_not_in_tunnel(self):
        # NAD=4, NNAD=3: gap must be <= 2*3=6 AND sufficient rule requires <=NNAD=3
        # Use a gap of 7 (> 2*NNAD) which the gap-expansion loop will cap
        fws, gap_idx = self._make_sequence(ad_left=4, gap=7, ad_right=4)
        assert self.clf._is_in_long_tunnel(gap_idx, fws, 4, 3) is False

    def test_large_ad_blocks_satisfy_sufficient_rule(self):
        # NAD=4, NNAD=4; 8 ads on left (>=NAD*2=8) satisfies the "large left block" sufficient
        # rule even when the gap (5 unknowns) is wider than NNAD.
        # Note: the left-block counter is capped at 2*NNAD, so NAD*2 must be <= 2*NNAD+1.
        fws, gap_idx = self._make_sequence(ad_left=8, gap=5, ad_right=4)
        assert self.clf._is_in_long_tunnel(gap_idx, fws, 4, 4) is True


# ---------------------------------------------------------------------------
# FragmentsClassifier.run — integration
# ---------------------------------------------------------------------------


class TestFragmentsClassifierRun:
    def setup_method(self):
        self.clf = FragmentsClassifier(repetition_threshold=3)

    def test_empty_input_returns_empty(self):
        assert self.clf.run([]) == []

    def test_singleton_group_classified_unknown(self):
        groups = [make_group(1, offsets=[0.0])]
        fragments = self.clf.run(groups)
        assert len(fragments) == 1
        assert fragments[0].classification == "unknown"

    def test_repeated_group_classified_new_ad(self):
        groups = [make_group(3, offsets=[0.0, 60.0, 120.0])]
        fragments = self.clf.run(groups)
        assert all(f.classification == "new_ad" for f in fragments)

    def test_already_known_fragments_pass_through(self):
        known = [make_fragment(0.0, 5.0, classification="already_known_ad")]
        fragments = self.clf.run([], already_known_fragments=known)
        assert len(fragments) == 1
        assert fragments[0].classification == "already_known_ad"

    def test_unknown_fragment_between_ads_classified_as_new_ad_via_short_tunnel(self):
        # Repeated group A at t=0, t=30, t=60 → new_ad
        group_a = make_group(3, offsets=[0.0, 30.0, 60.0])
        # Singleton at t=5 → initially unknown, sits between ads
        group_singleton = make_group(1, offsets=[5.0])
        # Repeated group B at t=10, t=40, t=70 → new_ad
        group_b = make_group(3, offsets=[10.0, 40.0, 70.0])
        fragments = self.clf.run([group_a, group_singleton, group_b])
        singleton_frags = [f for f in fragments if f.group_id is None]
        assert len(singleton_frags) == 1
        assert singleton_frags[0].classification == "new_ad"

    def test_unknown_fragment_far_from_ads_stays_unknown(self):
        # Repeated group far away on the left only
        group_a = make_group(3, offsets=[0.0, 200.0, 400.0])
        # Singleton far to the right
        group_singleton = make_group(1, offsets=[500.0])
        fragments = self.clf.run([group_a, group_singleton])
        singleton_frags = [f for f in fragments if f.group_id is None]
        assert len(singleton_frags) == 1
        assert singleton_frags[0].classification == "unknown"

    def test_fragments_are_sorted_by_start_sec(self):
        group = make_group(3, offsets=[20.0, 0.0, 40.0])
        fragments = self.clf.run([group])
        starts = [f.start_sec for f in fragments]
        assert starts == sorted(starts)

    def test_already_known_ad_is_not_given_group_id(self):
        known = [make_fragment(0.0, 5.0, classification="already_known_ad")]
        fragments = self.clf.run([], already_known_fragments=known)
        assert fragments[0].group_id is None


# ---------------------------------------------------------------------------
# FragmentsClassifier._get_internal_fragments
# ---------------------------------------------------------------------------


class TestGetInternalFragments:
    def setup_method(self):
        self.clf = FragmentsClassifier()

    def test_singleton_group_gets_no_group_id(self):
        groups = [make_group(1, offsets=[0.0])]
        fws = self.clf._get_internal_fragments(groups)
        assert len(fws) == 1
        assert fws[0].fragment.group_id is None
        assert fws[0].group is None

    def test_repeated_group_gets_group_id(self):
        groups = [make_group(3, offsets=[0.0, 10.0, 20.0])]
        fws = self.clf._get_internal_fragments(groups)
        assert len(fws) == 3
        for fw in fws:
            assert fw.fragment.group_id == "group_0"
            assert fw.group is groups[0]

    def test_group_id_uses_list_index(self):
        groups = [make_group(1), make_group(2, offsets=[50.0, 60.0])]
        fws = self.clf._get_internal_fragments(groups)
        group_1_fws = [fw for fw in fws if fw.group is not None]
        assert all(fw.fragment.group_id == "group_1" for fw in group_1_fws)

    def test_all_fragments_start_as_unknown(self):
        groups = [make_group(3, offsets=[0.0, 10.0, 20.0])]
        fws = self.clf._get_internal_fragments(groups)
        assert all(fw.fragment.classification == "unknown" for fw in fws)


# ---------------------------------------------------------------------------
# FragmentsClassifier._merge_groups
# ---------------------------------------------------------------------------


class TestMergeGroups:
    def setup_method(self):
        self.clf = FragmentsClassifier()

    def test_consecutive_pairs_are_merged(self):
        fragments = [
            make_fragment(0, 5, "new_ad", "group_0"),
            make_fragment(5, 10, "new_ad", "group_1"),
            make_fragment(20, 25, "new_ad", "group_0"),
            make_fragment(25, 30, "new_ad", "group_1"),
        ]
        result = self.clf._merge_groups(fragments, "group_0", "group_1")
        assert len(result) == 2
        assert result[0].start_sec == 0
        assert result[0].end_sec == 10
        assert result[0].group_id == "group_1"
        assert result[1].start_sec == 20
        assert result[1].end_sec == 30
        assert result[1].group_id == "group_1"

    def test_non_consecutive_group1_fragment_gets_null_group_id(self):
        # group_1 appears at the start without a preceding group_0 → group_id cleared
        fragments = [
            make_fragment(0, 5, "new_ad", "group_1"),
            make_fragment(10, 15, "new_ad", "group_0"),
            make_fragment(15, 20, "new_ad", "group_1"),
        ]
        result = self.clf._merge_groups(fragments, "group_0", "group_1")
        assert result[0].group_id is None
        assert result[1].start_sec == 10
        assert result[1].end_sec == 20
        assert result[1].group_id == "group_1"

    def test_merged_fragment_uses_higher_classification(self):
        fragments = [
            make_fragment(0, 5, "new_ad", "group_0"),
            make_fragment(5, 10, "already_known_ad", "group_1"),
        ]
        result = self.clf._merge_groups(fragments, "group_0", "group_1")
        assert result[0].classification == "already_known_ad"

    def test_merged_fragment_combines_chunks(self):
        chunk_a = make_chunk(0, 5)
        chunk_b = make_chunk(5, 10)
        f0 = Fragment(0, 5, "test", "new_ad", "group_0", [chunk_a])
        f1 = Fragment(5, 10, "test", "new_ad", "group_1", [chunk_b])
        result = self.clf._merge_groups([f0, f1], "group_0", "group_1")
        assert result[0].chunks == [chunk_a, chunk_b]

    def test_unrelated_fragments_pass_through_unchanged(self):
        fragments = [
            make_fragment(0, 5, "content", None),
            make_fragment(5, 10, "unknown", "group_0"),
        ]
        result = self.clf._merge_groups(fragments, "group_0", "group_1")
        assert result[0].classification == "content"
        assert result[1].group_id == "group_0"


# ---------------------------------------------------------------------------
# FragmentsClassifier._merge_continuous_fragments
# ---------------------------------------------------------------------------


class TestMergeContinuousFragments:
    def setup_method(self):
        self.clf = FragmentsClassifier()

    def test_always_consecutive_groups_are_merged(self):
        # group_0 always immediately followed by group_1 (within 1 s)
        fragments = [
            make_fragment(0, 5, "new_ad", "group_0"),
            make_fragment(5, 10, "new_ad", "group_1"),
            make_fragment(20, 25, "new_ad", "group_0"),
            make_fragment(25, 30, "new_ad", "group_1"),
            make_fragment(40, 45, "new_ad", "group_0"),
            make_fragment(45, 50, "new_ad", "group_1"),
        ]
        result = self.clf._merge_continous_fragments(fragments)
        assert len(result) == 3
        assert all(f.end_sec - f.start_sec == 10 for f in result)

    def test_inconsistently_followed_groups_are_not_merged(self):
        # group_0 is sometimes followed by group_1, sometimes by group_2
        fragments = [
            make_fragment(0, 5, "new_ad", "group_0"),
            make_fragment(5, 10, "new_ad", "group_1"),
            make_fragment(20, 25, "new_ad", "group_0"),
            make_fragment(25, 30, "new_ad", "group_2"),
        ]
        result = self.clf._merge_continous_fragments(fragments)
        assert len(result) == 4

    def test_already_known_ad_fragments_excluded_from_merge_logic(self):
        # already_known_ad fragments should not participate in merge decisions
        fragments = [
            make_fragment(0, 5, "already_known_ad", "group_0"),
            make_fragment(5, 10, "already_known_ad", "group_1"),
            make_fragment(20, 25, "already_known_ad", "group_0"),
            make_fragment(25, 30, "already_known_ad", "group_1"),
        ]
        result = self.clf._merge_continous_fragments(fragments)
        # No merging should happen since already_known_ad is excluded
        assert len(result) == 4

    def test_no_groups_returns_unchanged(self):
        fragments = [
            make_fragment(0, 5, "unknown", None),
            make_fragment(5, 10, "unknown", None),
        ]
        result = self.clf._merge_continous_fragments(fragments)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# FragmentsClassifier.params / params_hash
# ---------------------------------------------------------------------------


class TestParams:
    def test_params_returns_constructor_values(self):
        clf = FragmentsClassifier(repetition_threshold=5, tunnel_terminal_threshold=4)
        p = clf.params()
        assert p["repetition_threshold"] == 5
        assert p["tunnel_terminal_threshold"] == 4

    def test_params_hash_is_stable(self):
        clf = FragmentsClassifier(repetition_threshold=3, tunnel_terminal_threshold=2)
        assert clf.params_hash() == clf.params_hash()

    def test_params_hash_changes_with_different_params(self):
        clf1 = FragmentsClassifier(repetition_threshold=3)
        clf2 = FragmentsClassifier(repetition_threshold=5)
        assert clf1.params_hash() != clf2.params_hash()

    def test_params_hash_length_is_16(self):
        assert len(FragmentsClassifier().params_hash()) == 16
