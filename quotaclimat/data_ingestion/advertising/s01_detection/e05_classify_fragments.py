import hashlib
import json
import logging
from dataclasses import dataclass

from .e04_group_chunks import ChunkGroup
from .tools.common_objects import Fragment, FragmentClassification

logger = logging.getLogger(__name__)

IS_AD: tuple[FragmentClassification, ...] = ("already_known_ad", "new_ad")
MAXIMUM_SECONDS = 60


@dataclass
class _FragmentWrapper:
    # Class dedicated to computes inside FragmentsClassifier
    fragment: Fragment
    group: ChunkGroup | None = None


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


class FragmentsClassifier:
    def __init__(
        self, repetition_threshold: int = 3, tunnel_terminal_threshold: int = 2
    ):
        self.repetition_threshold = repetition_threshold
        self.tunnel_terminal_threshold = tunnel_terminal_threshold

    def run(
        self, groups: list[ChunkGroup], already_known_fragments: list[Fragment] = []
    ) -> list[Fragment]:
        fws: list[_FragmentWrapper] = self._get_internal_fragments(groups) + [
            _FragmentWrapper(fragment=f) for f in already_known_fragments
        ]
        fws.sort(key=lambda f: f.fragment.start_sec)

        # First discrimination based on segment: already known classification or group size
        for fw in fws:
            fw.fragment.classification = self._first_discrimination(fw)

        # Second categorization based on short tunnels: mark repeted segments around identified ads
        for index, fw in enumerate(fws):
            if fw.fragment.classification == "unknown" and (
                self._is_in_short_tunnel(index, fws)
            ):
                fw.fragment.classification = "new_ad"

        # Third categorization based on long tunnels: mark segments around long tunnels of identified ad
        for index, fw in enumerate(fws):
            if fw.fragment.classification == "unknown" and (
                self._is_in_long_tunnel(index, fws, 4, 3)
                or self._is_in_long_tunnel(index, fws, 5, 5)
            ):
                fw.fragment.classification = "new_ad"

        fragments = self._convert_to_output_fragments(fws)
        fragments = self._merge_continous_fragments(fragments)
        return fragments

    def _get_internal_fragments(
        self, groups: list[ChunkGroup]
    ) -> list[_FragmentWrapper]:
        """Classify the chunks of audio files into fragments."""
        fws: list[_FragmentWrapper] = []
        for index, group in enumerate(groups):
            if group.count == 1:
                occ = group.occurrences[0]
                fws.append(
                    _FragmentWrapper(
                        fragment=Fragment(
                            start_sec=occ.start_sec,
                            end_sec=occ.end_sec,
                            channel=occ.channel,
                            classification="unknown",
                            group_id=None,
                            chunks=[occ],
                        )
                    )
                )
            else:
                for occ in group.occurrences:
                    fws.append(
                        _FragmentWrapper(
                            fragment=Fragment(
                                start_sec=occ.start_sec,
                                end_sec=occ.end_sec,
                                channel=occ.channel,
                                classification="unknown",
                                group_id=f"group_{index}",
                                chunks=[occ],
                            ),
                            group=group,
                        )
                    )

        return fws

    def _first_discrimination(self, fw: _FragmentWrapper) -> FragmentClassification:
        """Returns a first categorization of a fragment based only on its own properties (group size, known-ad status)."""
        if fw.fragment.classification != "unknown":
            return fw.fragment.classification

        if fw.group is None:
            return "unknown"

        if fw.group.count >= self.repetition_threshold:
            return "new_ad"

        return "unknown"

    def _is_ad_anchor(self, fw: _FragmentWrapper) -> bool:
        """True if a fragment acts as an ad boundary for tunnel detection."""
        if (
            fw.group is not None
            and fw.group.count >= self.tunnel_terminal_threshold == 2
        ):
            return True
        return False

    def _is_in_short_tunnel(self, index: int, fws: list[_FragmentWrapper]) -> bool:
        """True if fragment is in a short non-ad gap between two ad anchors.

        Rule:
        - an ad exists within 4 positions to the left or the right
        - an ad anchor exists on the other side (or the fragment itself)
        - the index between the two is less or equal to 4
        - the total duration between the two is within MAXIMUM_SECONDS
        """
        # Find nearest ad anchor to the left (within 4 positions)
        left_anchor = None
        right_anchor = None
        found_ad = False
        for j in range(index - 1, max(index - 5, -1), -1):
            if fws[j].fragment.classification in IS_AD:
                left_anchor = j
                found_ad = True
                break
            if self._is_ad_anchor(fws[j]):
                left_anchor = j
                break

        if left_anchor is None:
            return False

        # Find next ad anchor to the right, but only within 4 positions of left_anchor
        for j in range(index + 1, min(left_anchor + 5, len(fws))):
            if fws[j].fragment.classification in IS_AD:
                right_anchor = j
                found_ad = True
                break
            if self._is_ad_anchor(fws[j]):
                right_anchor = j
                break

        if right_anchor is None:
            return False

        return (
            found_ad
            and (right_anchor - left_anchor <= 4)
            and (
                fws[right_anchor].fragment.start_sec - fws[left_anchor].fragment.end_sec
                <= MAXIMUM_SECONDS
            )
        )

    def _is_in_long_tunnel(
        self, index: int, fws: list[_FragmentWrapper], NAD: int, NNAD: int
    ) -> bool:
        """True if fragment is in a non-ad gap flanked by two large ad blocks.

        Rule: the contiguous non-ad gap containing index has at least NAD ads on each side,
        the gap is at most 2*NNAD wide, and at least one side has 2*NAD ads OR the gap is
        at most NNAD wide. Total duration of the gap must also fit within MAXIMUM_SECONDS.
        """
        # Find the full extent of the contiguous non-ad gap containing index
        gap_start = index
        gap_end = index

        while (
            gap_start > 0
            and fws[gap_start - 1].fragment.classification not in IS_AD
            and gap_end - gap_start <= 2 * NNAD
        ):
            gap_start -= 1

        while (
            gap_end < len(fws) - 1
            and fws[gap_end + 1].fragment.classification not in IS_AD
            and gap_end - gap_start <= 2 * NNAD
        ):
            gap_end += 1

        gap_size = gap_end - gap_start + 1

        # Measure the contiguous ad block immediately to the left of the gap
        left_start = gap_start
        while (
            left_start > 0
            and (
                fws[left_start].fragment.start_sec
                - fws[left_start - 1].fragment.end_sec
                < 1.0  # The two segments are following each others
            )
            and fws[left_start - 1].fragment.classification in IS_AD
            and gap_start - left_start <= 2 * NNAD
        ):
            left_start -= 1

        # Measure the contiguous ad block immediately to the right of the gap
        right_end = gap_end
        while (
            right_end < len(fws) - 1
            and (
                fws[right_end + 1].fragment.start_sec - fws[right_end].fragment.end_sec
                < 1.0  # The two segments are following each others
            )
            and fws[right_end + 1].fragment.classification in IS_AD
            and right_end - gap_end <= 2 * NNAD
        ):
            right_end += 1

        # Mnimum rules: they all need to be true
        if not (
            gap_start - left_start >= NAD
            and gap_size <= NNAD * 2
            and right_end - gap_end >= NAD
        ):
            return False

        # Sufficient rules: only one of them should be true (in addition to the minimum ones)
        if not (
            gap_start - left_start >= NAD * 2
            or gap_size <= NNAD
            or right_end - gap_end >= NAD * 2
        ):
            return False

        gap_duration = fws[gap_end].fragment.start_sec - fws[gap_start].fragment.end_sec
        return gap_duration <= MAXIMUM_SECONDS

    def _convert_to_output_fragments(
        self, fragment_wrappers: list[Fragment]
    ) -> list[Fragment]:
        return [fw.fragment for fw in fragment_wrappers]

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
