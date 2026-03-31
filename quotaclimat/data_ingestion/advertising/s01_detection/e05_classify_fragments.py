import hashlib
import json
import logging

from .e04_group_chunks import ChunkGroup
from .tools.common_objects import Fragment, FragmentClassification

logger = logging.getLogger(__name__)


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
    if order[class1] >= order[class2]:
        return class1
    else:
        return class2


class FragmentsClassifier:
    def __init__(self, repetition_threshold: int = 3):
        self.repetition_threshold = repetition_threshold

    def run(
        self, groups: list[ChunkGroup], already_known_fragments: list[Fragment] = []
    ) -> list[Fragment]:
        fragments = self._get_internal_fragments(groups) + already_known_fragments
        fragments.sort(key=lambda f: f.start_sec)
        self._detect_tunnels(fragments)
        self._detect_long_tunnels(fragments)  # 10 d'un côté et 5 de l'autre
        fragments = self._merge_continous_fragments(fragments)

        return self._convert_to_output_fragments(fragments)

    def _get_internal_fragments(self, groups: list[ChunkGroup]) -> list[Fragment]:
        """
        Classify the chunks of audio files into fragments.
        """
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

    def _detect_tunnels(self, fragments: list[Fragment]) -> None:
        """
        Detect ad tunnels in the fragments, which are sequences of continous ads.
        The rule is that if we have a segment tagged as already_known_ad or who is repeated (has a group index) and another segment is an already_known_ad or a new_ad in the next 4 segments in the order,
        Then all those segments are tagged as new_ad, if not tagged already.
        """
        for i, fragment in enumerate(fragments):
            if (
                fragment.classification in ("already_known_ad", "new_ad")
                or fragment.group_id is not None
            ):
                ad_in_the_block = fragment.classification in (
                    "already_known_ad",
                    "new_ad",
                )

                for j in range(i + 1, min(i + 5, len(fragments))):
                    next_fragment = fragments[j]
                    if (
                        next_fragment.classification in ("already_known_ad", "new_ad")
                        or next_fragment.group_id is not None
                    ):
                        ad_in_the_block = (
                            ad_in_the_block
                            or next_fragment.classification
                            in (
                                "already_known_ad",
                                "new_ad",
                            )
                        )
                        if ad_in_the_block:
                            for k in range(i, j + 1):
                                if fragments[k].classification not in (
                                    "already_known_ad",
                                    "new_ad",
                                ):
                                    fragments[k].classification = "new_ad"

    def _detect_long_tunnels(self, fragments: list[Fragment]) -> None:
        """
        Detect long ad tunnels in the fragments, which are sequences of continous ads.
        The rule is that if we have a sequence of fragments between two sequences of already_known_ad or new_ad, then this unkown sequence of fragment is tagged as new_ad (if not already tagged).
        We can have up to 5 non-ad segments in the middle, and the two ad blocks on the side must have at least 5 segments.
        We also accept 10 non-ad segments of one of the two sides is a very long sequence of 10 ads.
        """
        start_of_first_block = 0
        while start_of_first_block < len(fragments):
            if fragments[start_of_first_block].classification in (
                "already_known_ad",
                "new_ad",
            ):
                end_of_first_block = start_of_first_block + 1
                while end_of_first_block < len(fragments) and fragments[
                    end_of_first_block
                ].classification in (
                    "already_known_ad",
                    "new_ad",
                ):
                    end_of_first_block += 1

                start_of_second_block = end_of_first_block
                while start_of_second_block < len(fragments) and fragments[
                    start_of_second_block
                ].classification not in (
                    "already_known_ad",
                    "new_ad",
                ):
                    start_of_second_block += 1

                end_of_second_block = start_of_second_block + 1
                while end_of_second_block < len(fragments) and fragments[
                    end_of_second_block
                ].classification in (
                    "already_known_ad",
                    "new_ad",
                ):
                    end_of_second_block += 1

                if (  # Minimum bot not enough
                    (end_of_first_block - start_of_first_block >= 5)
                    and (start_of_second_block - end_of_first_block <= 10)
                    and (end_of_second_block - start_of_second_block >= 5)
                ):
                    if (  # More restrictive conditions, we only need one of them
                        (end_of_first_block - start_of_first_block >= 10)
                        or (start_of_second_block - end_of_first_block <= 5)
                        or (end_of_second_block - start_of_second_block >= 10)
                    ):
                        for m in range(end_of_first_block, start_of_second_block):
                            if fragments[m].classification not in (
                                "already_known_ad",
                                "new_ad",
                            ):
                                fragments[m].classification = "new_ad"

                start_of_first_block = start_of_second_block
            else:
                start_of_first_block += 1

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
        """This function will merge groups, of which fragments are almost always continous.
        For each group, we will look at all the groups that are following the segments of the group.
        Depending on the repartition of those groups, we will merge the most represented group with the current one."""
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

                if (
                    next_fragment.start_sec - fragment.end_sec <= 1.0
                ):  # If the next fragment is within 1 second after the current one
                    next_group_id = next_fragment.group_id
                    if (
                        next_group_id is None
                        or next_fragment.classification == "already_known_ad"
                    ):
                        next_group_id = "none"  # We use "none" to represent fragments without group or that cannot be matched
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

                if (
                    fragment.start_sec - previous_fragment.end_sec <= 1.0
                ):  # If the previous fragment is within 1 second before the current one
                    previous_group_id = previous_fragment.group_id
                    if previous_group_id is None:
                        previous_group_id = (
                            "none"  # We use "none" to represent fragments without group
                        )
                    if previous_group_id not in preceding_groups_count:
                        preceding_groups_count[previous_group_id] = 0
                    preceding_groups_count[previous_group_id] += 1
        return preceding_groups_count

    def _merge_groups(
        self,
        fragments: list[Fragment],
        group_id_to_merge: int,
        group_id_to_merge_with: int,
    ) -> None:
        """Here we keep the second index to be sure it can later be merged with another group"""
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
                new_fragments.append(fragment)  # we keep the original group id
            elif fragment.group_id == group_id_to_merge_with:
                # It has not been merged with the previous one, we remove the group id to avoid future incoherence
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
        }

    def params_hash(self) -> str:
        """Returns a stable SHA256 hash of all constructor parameters.
        Changes when any parameter value changes, identical otherwise."""
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
