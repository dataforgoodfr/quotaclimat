import hashlib
import json
import logging

from .e04_group_chunks import ChunkGroup
from .tools.common_objects import Fragment, FragmentClassification

logger = logging.getLogger(__name__)

IS_AD: tuple[FragmentClassification, ...] = ("already_known_ad", "new_ad")
MAXIMUM_SECONDS = 60


def higher_classification(
    class1: FragmentClassification, class2: FragmentClassification
) -> FragmentClassification:
    """Returns the higher classification between the two given classifications, with the following order: already_known_ad > new_ad > jingle > content > unknown."""
    order = {
        "already_known_ad": 4,
        "new_ad": 3,
        "jingle": 2,
        "content": 1,
        "unknown": 0,
    }
    return class1 if order[class1] >= order[class2] else class2


def discriminate_fragment(fragment: Fragment) -> FragmentClassification:
    """Returns a first categorization of a fragment based only on its own properties (group size, known-ad status)."""
    return fragment.classification


class FragmentsClassifier:
    def __init__(
        self, repetition_threshold: int = 3, tunnel_terminal_threshold: int = 2
    ):
        self.repetition_threshold = repetition_threshold
        self.tunnel_terminal_threshold = tunnel_terminal_threshold

    def run(
        self, groups: list[ChunkGroup], already_known_fragments: list[Fragment] = []
    ) -> list[Fragment]:
        fragments = self._get_internal_fragments(groups) + already_known_fragments
        fragments.sort(key=lambda f: f.start_sec)

        new_classifications = [
            self._categorize_fragment(i, fragments) for i in range(len(fragments))
        ]
        for fragment, classification in zip(fragments, new_classifications):
            fragment.classification = classification

        fragments = self._merge_continous_fragments(fragments)
        return self._convert_to_output_fragments(fragments)

    def _get_internal_fragments(self, groups: list[ChunkGroup]) -> list[Fragment]:
        """Classify the chunks of audio files into fragments."""
        fragments: list[Fragment] = []
        for index, group in enumerate(groups):
            if group.count == 1:
                occ = group.occurrences[0]
                fragments.append(
                    Fragment(
                        start_sec=occ.start_sec,
                        end_sec=occ.end_sec,
                        channel=occ.channel,
                        classification="content",
                        group_id=None,
                        chunks=[occ],
                    )
                )
            else:
                classification = (
                    "new_ad" if group.count >= self.repetition_threshold else "unknown"
                )
                for occ in group.occurrences:
                    fragments.append(
                        Fragment(
                            start_sec=occ.start_sec,
                            end_sec=occ.end_sec,
                            channel=occ.channel,
                            classification=classification,
                            group_id=f"group_{index}",
                            chunks=[occ],
                        )
                    )

        return fragments

    def _is_ad_anchor(self, fragment: Fragment) -> bool:
        """True if a fragment acts as an ad boundary for tunnel detection."""
        if discriminate_fragment(fragment) in IS_AD:
            return True
        if self.tunnel_terminal_threshold == 2 and fragment.group_id is not None:
            return True
        return False

    def _categorize_fragment(
        self, index: int, fragments: list[Fragment]
    ) -> FragmentClassification:
        """Decides the final category of a fragment by examining all surrounding context.

        Applies three tunnel rules in order using discriminate_fragment on neighbours:
          1. Short tunnel: fragment sits between two ad anchors within 4 positions.
          2. Long tunnel (NAD=4, NNAD=3): fragment is in a short gap between two large ad blocks.
          3. Long tunnel (NAD=5, NNAD=5): same with stricter block-size requirements.
        """
        base = discriminate_fragment(fragments[index])

        if base in IS_AD or base == "content":
            return base

        if self._is_in_short_tunnel(index, fragments):
            return "new_ad"

        for NAD, NNAD in [(4, 3), (5, 5)]:
            if self._is_in_long_tunnel(index, fragments, NAD, NNAD):
                return "new_ad"

        return base

    def _is_in_short_tunnel(self, index: int, fragments: list[Fragment]) -> bool:
        """True if fragment is in a short non-ad gap between two ad anchors.

        Rule: an ad anchor L exists within 4 positions to the left, another ad anchor R
        exists within L+4 positions to the right, and the total duration of non-ad
        fragments between L and R is within MAXIMUM_SECONDS.
        """
        # Find nearest ad anchor to the left (within 4 positions)
        left_anchor = None
        for j in range(index - 1, max(index - 5, -1), -1):
            if self._is_ad_anchor(fragments[j]):
                left_anchor = j
                break

        if left_anchor is None:
            return False

        # Find next ad anchor to the right, but only within 4 positions of left_anchor
        for j in range(index + 1, min(left_anchor + 5, len(fragments))):
            if self._is_ad_anchor(fragments[j]):
                candidates = [
                    fragments[k]
                    for k in range(left_anchor + 1, j)
                    if not self._is_ad_anchor(fragments[k])
                ]
                return sum(f.end_sec - f.start_sec for f in candidates) <= MAXIMUM_SECONDS

        return False

    def _is_in_long_tunnel(
        self, index: int, fragments: list[Fragment], NAD: int, NNAD: int
    ) -> bool:
        """True if fragment is in a non-ad gap flanked by two large ad blocks.

        Rule: the contiguous non-ad gap containing index has at least NAD ads on each side,
        the gap is at most 2*NNAD wide, and at least one side has 2*NAD ads OR the gap is
        at most NNAD wide. Total duration of the gap must also fit within MAXIMUM_SECONDS.
        """
        # Find the full extent of the contiguous non-ad gap containing index
        gap_start = index
        while gap_start > 0 and discriminate_fragment(fragments[gap_start - 1]) not in IS_AD:
            gap_start -= 1

        gap_end = index
        while gap_end < len(fragments) - 1 and discriminate_fragment(fragments[gap_end + 1]) not in IS_AD:
            gap_end += 1

        gap_size = gap_end - gap_start + 1

        # Measure the contiguous ad block immediately to the left of the gap
        if gap_start == 0:
            return False
        left_end = gap_start - 1
        left_start = left_end
        while left_start > 0 and discriminate_fragment(fragments[left_start - 1]) in IS_AD:
            left_start -= 1
        left_size = left_end - left_start + 1

        # Measure the contiguous ad block immediately to the right of the gap
        if gap_end == len(fragments) - 1:
            return False
        right_start = gap_end + 1
        right_end = right_start
        while right_end < len(fragments) - 1 and discriminate_fragment(fragments[right_end + 1]) in IS_AD:
            right_end += 1
        right_size = right_end - right_start + 1

        if not (left_size >= NAD and gap_size <= NNAD * 2 and right_size >= NAD):
            return False

        if not (left_size >= NAD * 2 or gap_size <= NNAD or right_size >= NAD * 2):
            return False

        gap_duration = sum(
            fragments[k].end_sec - fragments[k].start_sec
            for k in range(gap_start, gap_end + 1)
        )
        return gap_duration <= MAXIMUM_SECONDS

    def _convert_to_output_fragments(self, fragments: list[Fragment]) -> list[Fragment]:
        output_fragments: list[Fragment] = []
        for fragment in fragments:
            output_fragments.append(
                Fragment(
                    start_sec=fragment.start_sec,
                    end_sec=fragment.end_sec,
                    channel=fragment.chunks[0].channel,
                    classification=fragment.classification,
                    chunks=fragment.chunks,
                    group_id=fragment.group_id,
                )
            )
        return output_fragments

    def _merge_continous_fragments(self, fragments: list[Fragment]) -> list[Fragment]:
        """Merge groups whose fragments are almost always continuous.

        For each group, look at the groups following its fragments. If one group
        consistently follows and the relationship is reciprocal, merge them.
        """
        groups_to_merge: list[tuple[str, str]] = []

        groups_fragment_index: dict[str, list[int]] = {}
        for index, fragment in enumerate(fragments):
            if (
                fragment.group_id is not None
                and fragment.classification != "already_known_ad"
            ):
                if fragment.group_id not in groups_fragment_index:
                    groups_fragment_index[fragment.group_id] = []
                groups_fragment_index[fragment.group_id].append(index)

        for group_id, fragment_indexes in groups_fragment_index.items():
            following_groups_count = self._get_group_repartition_following(
                fragments, fragment_indexes
            )

            if len(following_groups_count) == 1 and (
                "none" not in following_groups_count
            ):
                most_followed_group_id = list(following_groups_count.keys())[0]
                preceeding_most_followed_group_id = (
                    self._get_group_repartition_preceeding(
                        fragments, groups_fragment_index[most_followed_group_id]
                    )
                )
                if len(preceeding_most_followed_group_id) == 1 and (
                    "none" not in preceeding_most_followed_group_id
                ):
                    most_preceeding_followed_group_id = list(
                        preceeding_most_followed_group_id.keys()
                    )[0]
                    if most_preceeding_followed_group_id != group_id:
                        logger.debug(
                            f"Preceeding group index repartition for group {most_followed_group_id} is {preceeding_most_followed_group_id}, while following group index repartition for group {group_id} is {following_groups_count}. Not merging those groups because of this incoherence."
                        )
                    else:
                        logger.debug(
                            f"Merging group {group_id} with group {most_followed_group_id} because all the following fragments of group {group_id} are in group {most_followed_group_id}, and all the preceeding fragments of group {most_followed_group_id} are in group {group_id}."
                        )
                        groups_to_merge.append((group_id, most_followed_group_id))
                elif len(preceeding_most_followed_group_id) == 2:
                    most_preceeding_followed_group_id = max(
                        preceeding_most_followed_group_id,
                        key=preceeding_most_followed_group_id.get,
                    )
                    less_preceeding_followed_group_id = min(
                        preceeding_most_followed_group_id,
                        key=preceeding_most_followed_group_id.get,
                    )
                    if (
                        preceeding_most_followed_group_id[
                            most_preceeding_followed_group_id
                        ]
                        / (
                            preceeding_most_followed_group_id[
                                less_preceeding_followed_group_id
                            ]
                            + preceeding_most_followed_group_id[
                                most_preceeding_followed_group_id
                            ]
                        )
                        >= 0.8
                    ):
                        logger.debug(
                            f"Preceeding group index repartition for group {most_followed_group_id} is {preceeding_most_followed_group_id}, while following group index repartition for group {group_id} is {following_groups_count}. Not merging those groups because of this incoherence, even if there is a majority in the following repartition, because of the significant presence of the other group in the preceeding repartition."
                        )
            elif len(following_groups_count) == 2:
                most_followed_group_id = max(
                    following_groups_count, key=following_groups_count.get
                )
                less_followed_group_id = min(
                    following_groups_count, key=following_groups_count.get
                )
                if (
                    following_groups_count[most_followed_group_id]
                    / (
                        following_groups_count[less_followed_group_id]
                        + following_groups_count[most_followed_group_id]
                    )
                    >= 0.8
                ):
                    logger.debug(
                        f"Not merging group {group_id} with group {most_followed_group_id} because it is followed in {following_groups_count[most_followed_group_id]} cases out of {following_groups_count[less_followed_group_id] + following_groups_count[most_followed_group_id]}"
                    )

        for group_id_to_merge, group_id_to_merge_with in groups_to_merge:
            fragments = self._merge_groups(
                fragments, group_id_to_merge, group_id_to_merge_with
            )
        return fragments

    def _get_group_repartition_following(
        self, fragments: list[Fragment], fragment_indexes: list[int]
    ) -> dict[int, int]:
        following_groups_count: dict[int, int] = {}
        for fragment_index in fragment_indexes:
            fragment = fragments[fragment_index]
            if fragment_index < len(fragments) - 1:
                next_fragment = fragments[fragment_index + 1]

                if next_fragment.start_sec - fragment.end_sec <= 1.0:
                    next_group_id = next_fragment.group_id
                    if (
                        next_group_id is None
                        or next_fragment.classification == "already_known_ad"
                    ):
                        next_group_id = "none"
                    if next_group_id not in following_groups_count:
                        following_groups_count[next_group_id] = 0
                    following_groups_count[next_group_id] += 1
        return following_groups_count

    def _get_group_repartition_preceeding(
        self, fragments: list[Fragment], fragment_indexes: list[int]
    ) -> dict[int, int]:
        preceding_groups_count: dict[int, int] = {}
        for fragment_index in fragment_indexes:
            fragment = fragments[fragment_index]
            if fragment_index > 0:
                previous_fragment = fragments[fragment_index - 1]

                if fragment.start_sec - previous_fragment.end_sec <= 1.0:
                    previous_group_id = previous_fragment.group_id
                    if previous_group_id is None:
                        previous_group_id = "none"
                    if previous_group_id not in preceding_groups_count:
                        preceding_groups_count[previous_group_id] = 0
                    preceding_groups_count[previous_group_id] += 1
        return preceding_groups_count

    def _merge_groups(
        self,
        fragments: list[Fragment],
        group_id_to_merge: int,
        group_id_to_merge_with: int,
    ) -> list[Fragment]:
        """Keep the second group id so it can later be merged with another group."""
        new_fragments = []
        i = 0
        while i < len(fragments):
            fragment = fragments[i]
            if fragment.group_id == group_id_to_merge:
                if i < len(fragments) - 1:
                    next_fragment = fragments[i + 1]
                    if next_fragment.group_id == group_id_to_merge_with:
                        new_fragments.append(
                            Fragment(
                                start_sec=fragment.start_sec,
                                end_sec=next_fragment.end_sec,
                                channel=fragment.channel,
                                classification=higher_classification(
                                    fragment.classification,
                                    next_fragment.classification,
                                ),
                                group_id=group_id_to_merge_with,
                                chunks=fragment.chunks + next_fragment.chunks,
                            )
                        )
                        i += 2
                        continue
                new_fragments.append(fragment)
            elif fragment.group_id == group_id_to_merge_with:
                # Not merged with previous one; clear group_id to avoid future incoherence
                new_fragments.append(
                    Fragment(
                        start_sec=fragment.start_sec,
                        end_sec=fragment.end_sec,
                        channel=fragment.channel,
                        classification=fragment.classification,
                        group_id=None,
                        chunks=fragment.chunks,
                    )
                )
            else:
                new_fragments.append(fragment)
            i += 1
        return new_fragments

    def params(self) -> dict:
        """Returns all constructor parameters as a dict."""
        return {
            "repetition_threshold": self.repetition_threshold,
            "tunnel_terminal_threshold": self.tunnel_terminal_threshold,
        }

    def params_hash(self) -> str:
        """Returns a stable SHA256 hash of all constructor parameters.
        Changes when any parameter value changes, identical otherwise."""
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
