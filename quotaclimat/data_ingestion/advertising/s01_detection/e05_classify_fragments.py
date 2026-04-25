import hashlib
import json
import logging
from collections import defaultdict
from dataclasses import dataclass

from .e04_group_chunks import ChunkGroup
from .tools.common_objects import Fragment, FragmentClassification

logger = logging.getLogger(__name__)

IS_AD: tuple[FragmentClassification, ...] = ("already_known_ad", "new_ad")


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
        self,
        repetition_threshold: int = 3,
        tunnel_terminal_threshold: int = 2,
        maximum_gap_for_propagation: int = 60,
        debug_timestamps: list[float] = [],
    ):
        self.repetition_threshold = repetition_threshold
        self.tunnel_terminal_threshold = tunnel_terminal_threshold
        self.maximum_gap_for_propagation = maximum_gap_for_propagation

        self.debug_timestamps = debug_timestamps
        self.debug_result = defaultdict(dict)

    @classmethod
    def from_channel(cls, channel: str):
        match channel:
            case "bfmtv":
                # Info en continue, longue période analysées donc beaucoup de répétition des pubs, on peut augmenter les limites.
                # C'est même nécessaire car il y a beaucoup de répétition des programmes,
                # et on désactive le tunnel_terminal_threshold (= à repetition_threshold) pour ne pas attraper de programmes par erreur.
                return cls(repetition_threshold=7, tunnel_terminal_threshold=7)
            case "itele":
                # CNEWS, découpage très éfficace, on peut repérer la pub avec un filtre très haut type repetition >8 avec beaucoup de précision
                # ci-dessous on abaisse les chiffres, notamment pour attraper les début et fin de tunnel de pub qui comportent parfois moins de répétition
                # mais pas trop bas non plus car certaines informations sont répétées (tunnel d'info jusqu'à 4 répétitions).
                # La meteo est particulièrement longue.
                return cls(
                    repetition_threshold=6,
                    tunnel_terminal_threshold=3,
                    maximum_gap_for_propagation=80,
                )
            case "lci":
                # Info en continue, beaucoup de répétition de pub donc la limite peut être haute, cela évite les petite répétition de contenu
                # Les tunnels à 4 détectent les quelques pubs moins répétées
                return cls(repetition_threshold=7, tunnel_terminal_threshold=4)
            case "france24":
                # Info en continue, très peu de publicité, très facilement identifiable car prise en sandwich entre 2 jingle.
                # Ces jingle sont répétées de très nombreuses fois (plus de 250 par semaine) et vont nous permettre d'identifier le tunnel.
                return cls(repetition_threshold=100, tunnel_terminal_threshold=10)
            case "test-channel":
                # Une limite basse pour qu'une simple répétition soit détectée, pas de tunnel possible dans ce cas
                return cls(repetition_threshold=2, tunnel_terminal_threshold=2)
        return cls(
            repetition_threshold=3,
            tunnel_terminal_threshold=2,
        )

    def run(
        self,
        groups: list[ChunkGroup],
        already_known_fragments: list[Fragment] = [],
    ) -> list[Fragment]:
        fws: list[_FragmentWrapper] = self._get_internal_fragments(groups) + [
            _FragmentWrapper(fragment=f) for f in already_known_fragments
        ]
        fws.sort(key=lambda f: f.fragment.start_sec)
        self._mark_debug_timestamps("initial_value", fws)

        # First discrimination based on segment: already known classification or group size
        for fw in fws:
            fw.fragment.classification = self._first_discrimination(fw)
        self._mark_debug_timestamps("first_discrimination", fws)

        new_ad_group_ids = set()

        # Second categorization based on short tunnels: mark repeted segments around identified ads
        new_ads_list = [
            (
                fw.fragment.classification == "unknown"
                and (self._is_in_short_tunnel(index, fws))
            )
            for index, fw in enumerate(fws)
        ]
        for fw, is_new_ad in zip(fws, new_ads_list):
            if is_new_ad:
                fw.fragment.classification = "new_ad"
                if fw.fragment.group_id is not None:
                    new_ad_group_ids.add(fw.fragment.group_id)
        self._mark_debug_timestamps("after_short_tunnel", fws)

        # Third categorization based on long tunnels: mark segments around long tunnels of identified ad
        new_ads_list = [
            (
                fw.fragment.classification == "unknown"
                and (
                    self._is_in_long_tunnel(index, fws, 4, 3)
                    or self._is_in_long_tunnel(index, fws, 5, 5)
                )
            )
            for index, fw in enumerate(fws)
        ]
        for fw, is_new_ad in zip(fws, new_ads_list):
            if is_new_ad:
                fw.fragment.classification = "new_ad"
                if fw.fragment.group_id is not None:
                    new_ad_group_ids.add(fw.fragment.group_id)
        self._mark_debug_timestamps("after_long_tunnels", fws)

        for fw in fws:
            if (
                fw.fragment.classification == "unknown"
                and fw.fragment.group_id is not None
                and fw.fragment.group_id in new_ad_group_ids
            ):
                fw.fragment.classification = "new_ad"
        self._mark_debug_timestamps("after_group_reconciliation", fws)

        fragments = self._convert_to_output_fragments(fws)
        fragments = self._merge_continous_fragments(fragments)

        self._print_debug_result()
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
        if (fw.group is not None) and (
            fw.group.count >= self.tunnel_terminal_threshold
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
        for left in range(index, max(index - 5, -1), -1):
            if fws[left].fragment.classification in IS_AD:
                # Find next ad anchor to the right, but only within 4 positions of left anchor
                for right in range(index + 1, min(left + 5, len(fws))):
                    if fws[
                        right
                    ].fragment.classification in IS_AD or self._is_ad_anchor(
                        fws[right]
                    ):
                        if (
                            fws[right].fragment.start_sec - fws[left].fragment.end_sec
                            <= self.maximum_gap_for_propagation
                        ):
                            return True

            elif self._is_ad_anchor(fws[left]):
                # Find next ad anchor to the right (needs to be an ad), but only within 4 positions of left anchor
                for right in range(index, min(left + 5, len(fws))):
                    if fws[right].fragment.classification in IS_AD:
                        if (
                            fws[right].fragment.start_sec - fws[left].fragment.end_sec
                            <= self.maximum_gap_for_propagation
                        ):
                            return True

        return False

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
        return gap_duration <= self.maximum_gap_for_propagation

    def _convert_to_output_fragments(
        self, fragment_wrappers: list[Fragment]
    ) -> list[Fragment]:
        return [fw.fragment for fw in fragment_wrappers]

    def _mark_debug_timestamps(self, key: str, fws: list[_FragmentWrapper]):
        for ts in self.debug_timestamps:
            for fw in fws:
                if fw.fragment.start_sec < ts < fw.fragment.end_sec:
                    self.debug_result[ts][key] = fw.fragment.classification
                    break
            else:
                logger.info(f"Did not found fragment for ts={ts} key={key}")

    def _print_debug_result(self):
        for ts, debug in self.debug_result.items():
            logger.info(
                f"[DEBUG] ts={ts} " + " ".join([f"{k}={v}" for k, v in debug.items()])
            )

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
