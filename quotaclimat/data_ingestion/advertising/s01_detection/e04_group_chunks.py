"""
Regroupement de chunks audio par fingerprints pré-calculés
=============================================================
Utilise les hashes pré-calculés par e02 (constellation maps style Shazam)
pour comparer et regrouper les chunks identiques.
"""

import itertools
import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

import numpy as np
from tqdm import tqdm

from .tools.common_objects import Chunk, Fingerprint
from .tools.fingerprint.hash import (
    _build_hash_sets,
    are_chunks_similar,
    make_params_hash,
)

logger = logging.getLogger(__name__)


@dataclass
class ChunkGroup:
    count: int
    duration_mean: float
    duration_std: float
    occurrences: list[Chunk]

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            count=data["count"],
            duration_mean=data["duration_mean"],
            duration_std=data["duration_std"],
            occurrences=[Chunk.from_dict(occ) for occ in data["occurrences"]],
        )


def _cluster(
    chunks: list[Chunk],
    hash_sets: list[set[str]],
    min_matching_hashes: int,
    similarity_threshold: float,
    duration_tol: float = 0.3,
    rms_tol: float = 0.05,
    centroid_tol: float = 0.05,
    zcr_tol: float = 0.1,
) -> Dict[int, List[int]]:
    """
    Group similar chunks via inverted index + Union-Find.
    Returns {group_id: [chunk indices]}.
    """
    n = len(chunks)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    # Inverted index: hash → chunk indices
    logger.debug(f"Clustering {n} chunks — building inverted index...")
    hash_index: Dict[str, List[int]] = defaultdict(list)
    for i, hs in enumerate(hash_sets):
        for h in hs:
            hash_index[h].append(i)

    # Count shared hashes per pair
    shared_counts: Dict[Tuple[int, int], int] = defaultdict(int)
    for bucket in hash_index.values():
        if len(bucket) < 2:
            continue
        for a, b in itertools.combinations(bucket, 2):
            key = (a, b) if a < b else (b, a)
            shared_counts[key] += 1

    # Filter: enough shared hashes as fast pre-filter, then full similarity check
    candidates = [
        (i, j)
        for (i, j), count in shared_counts.items()
        if count >= min_matching_hashes
    ]
    logger.debug(f"  {len(candidates)} candidate pairs to score")

    matches = 0
    for i, j in tqdm(candidates, desc="Comparaison fingerprints"):
        if are_chunks_similar(
            chunks[i],
            chunks[j],
            hash_sets[i],
            hash_sets[j],
            min_matching_hashes,
            similarity_threshold,
            duration_tol,
            rms_tol,
            centroid_tol,
            zcr_tol,
        ):
            union(i, j)
            matches += 1

    logger.debug(f"  {matches} similar pairs found")

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    return dict(groups)


class ChunkGrouping:
    def __init__(
        self,
        similarity_threshold: float = 0.08,  # Min fingerprint score to consider two chunks identical.
        #   Score = temporally coherent hashes / min(hashes_a, hashes_b).
        #   Lower = more matches (more false positives). Higher = stricter.
        min_matching_hashes: int = 2,  # Min shared hashes before scoring a pair.
        #   Acts as a fast pre-filter via the inverted index.
        #   1 = very permissive (more candidates to score). 3+ = stricter.
        # Legacy params kept for params_hash cache invalidation
        duration_tol: float = 0.3,
        rms_tol: float = 0.05,
        centroid_tol: float = 0.05,
        zcr_tol: float = 0.1,
    ):
        self.similarity_threshold = similarity_threshold
        self.min_matching_hashes = min_matching_hashes
        self.duration_tol = duration_tol
        self.rms_tol = rms_tol
        self.centroid_tol = centroid_tol
        self.zcr_tol = zcr_tol

    def params(self) -> dict:
        return {
            "similarity_threshold": self.similarity_threshold,
            "min_matching_hashes": self.min_matching_hashes,
            "duration_tol": self.duration_tol,
            "rms_tol": self.rms_tol,
            "centroid_tol": self.centroid_tol,
            "zcr_tol": self.zcr_tol,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())

    def run(self, source: List[Chunk]) -> list[ChunkGroup]:
        # Filter out very short chunks
        chunks = [c for c in source if c.fingerprint.duration_sec >= 0.5]
        logger.debug(f"{len(chunks)} chunks to group")

        hash_sets = _build_hash_sets(chunks)
        groups = _cluster(
            chunks,
            hash_sets,
            self.min_matching_hashes,
            self.similarity_threshold,
            self.duration_tol,
            self.rms_tol,
            self.centroid_tol,
            self.zcr_tol,
        )

        report_groups: list[ChunkGroup] = []
        for member_idxs in groups.values():
            members = sorted(
                [chunks[i] for i in member_idxs], key=lambda c: c.start_sec
            )
            durations = [c.fingerprint.duration_sec for c in members]

            report_groups.append(
                ChunkGroup(
                    count=len(members),
                    duration_mean=round(float(np.mean(durations)), 2),
                    duration_std=round(float(np.std(durations)), 2),
                    occurrences=members,
                )
            )

        report_groups.sort(key=lambda g: -g.count)
        return report_groups


def canonical(chunks: list[Chunk]) -> Chunk:
    """
    Build a canonical Chunk from multiple occurrences of the same audio segment.

    Designed to maximise future matching probability against the grouping logic:

    - Hashes: only hashes present in at least half the occurrences are kept.
      Hashes that appear in most occurrences are stable fingerprints of the ad;
      hashes that appear in only one are likely noise artefacts.
      The time offset kept for each hash is the median across occurrences,
      which is robust to minor chunk-boundary shifts between recordings.
      If the frequency filter yields fewer than MIN_CANONICAL_HASHES results,
      the top hashes ranked by occurrence frequency are used as a fallback
      so the canonical always carries enough fingerprint material to be matchable.

    - Acoustic features (duration, energy_mean, spectral_centroid, zcr_mean):
      median across all occurrences.  The median sits at the group centroid and
      therefore minimises the distance to any real future occurrence, maximising
      the chance of passing the _features_compatible pre-filter.

    - peaks: taken from the occurrence with the most hashes (richest fingerprint)
      so that re-hashing from the canonical peaks yields a dense set.
    """
    if len(chunks) == 1:
        return chunks[0]

    # Count how many distinct chunks each hash appears in,
    # and collect all its time offsets across occurrences.
    hash_occurrence_count: Counter = Counter()
    hash_to_times: dict[str, list[int]] = defaultdict(list)

    for chunk in chunks:
        seen_in_chunk: set[str] = set()
        for h, t in chunk.fingerprint.hashes or []:
            hash_to_times[h].append(t)
            seen_in_chunk.add(h)
        hash_occurrence_count.update(seen_in_chunk)

    # Keep only hashes present in at least half the occurrences.
    # If that yields fewer than MIN_CANONICAL_HASHES, fall back to the
    # top-MIN_CANONICAL_HASHES hashes ranked by occurrence frequency so
    # the canonical always has enough fingerprint material to be matchable.
    MIN_CANONICAL_HASHES = 10
    min_freq = max(1, len(chunks) // 2)
    stable_hashes = [
        (h, int(np.median(times)))
        for h, times in hash_to_times.items()
        if hash_occurrence_count[h] >= min_freq
    ]
    if len(stable_hashes) < MIN_CANONICAL_HASHES:
        stable_hashes = [
            (h, int(np.median(hash_to_times[h])))
            for h, _ in hash_occurrence_count.most_common(MIN_CANONICAL_HASHES)
        ]

    # Median acoustic features.
    durations = [c.fingerprint.duration_sec for c in chunks]
    energies = [c.fingerprint.energy_mean for c in chunks]
    centroids = [c.fingerprint.spectral_centroid for c in chunks]
    zcrs = [c.fingerprint.zcr_mean for c in chunks]

    # Richest occurrence as source for peaks.
    richest = max(chunks, key=lambda c: len(c.fingerprint.hashes or []))

    return Chunk(
        start_sec=richest.start_sec,
        end_sec=richest.end_sec,
        channel=richest.channel,
        fingerprint=Fingerprint(
            duration_sec=float(np.median(durations)),
            energy_mean=float(np.median(energies)),
            spectral_centroid=float(np.median(centroids)),
            zcr_mean=float(np.median(zcrs)),
            peaks=richest.fingerprint.peaks,
            hashes=stable_hashes,
        ),
    )


def debug_pair(a: Chunk, b: Chunk, grouping: "ChunkGrouping") -> None:
    """
    Print a step-by-step explanation of why two chunks are or are not grouped.

    Usage:
        debug_pair(chunks[i], chunks[j], chunk_grouping)
    """
    PASS = "✓"
    FAIL = "✗"

    def rel_diff(x: float, y: float) -> float:
        return abs(x - y) / max(abs(x), abs(y), 1e-8)

    print("=" * 60)
    print("DEBUG: chunk pair grouping analysis")
    print(f"  A: [{a.start_sec:.2f}s – {a.end_sec:.2f}s]  channel={a.channel}")
    print(f"  B: [{b.start_sec:.2f}s – {b.end_sec:.2f}s]  channel={b.channel}")
    print("=" * 60)

    # ── Step 1: minimum duration filter (applied in run()) ──────────────
    print("\n[1] Minimum duration filter (>= 0.5 s)")
    a_ok = a.fingerprint.duration_sec >= 0.5
    b_ok = b.fingerprint.duration_sec >= 0.5
    print(f"    A duration: {a.fingerprint.duration_sec:.3f}s  {PASS if a_ok else FAIL}")
    print(f"    B duration: {b.fingerprint.duration_sec:.3f}s  {PASS if b_ok else FAIL}")
    if not (a_ok and b_ok):
        print(
            "  → BLOCKED: one or both chunks are too short and would be filtered out."
        )
        return

    # ── Step 2: acoustic pre-filter (_features_compatible) ──────────────
    print("\n[2] Acoustic pre-filter (_features_compatible)")

    fp_a, fp_b = a.fingerprint, b.fingerprint

    dur_diff = abs(fp_a.duration_sec - fp_b.duration_sec)
    dur_ok = dur_diff <= grouping.duration_tol
    print(
        f"    duration |A-B| = {dur_diff:.3f}s  (tol={grouping.duration_tol})  {PASS if dur_ok else FAIL}"
    )

    rms_ok = True
    if fp_a.energy_mean > 0 and fp_b.energy_mean > 0:
        rms_diff = rel_diff(fp_a.energy_mean, fp_b.energy_mean)
        rms_ok = rms_diff <= grouping.rms_tol
        print(
            f"    energy_mean rel_diff = {rms_diff:.4f}  (tol={grouping.rms_tol})  {PASS if rms_ok else FAIL}"
            f"  (A={fp_a.energy_mean:.4f}, B={fp_b.energy_mean:.4f})"
        )
    else:
        print("    energy_mean: skipped (one value is 0)")

    centroid_ok = True
    if fp_a.spectral_centroid > 0 and fp_b.spectral_centroid > 0:
        centroid_diff = rel_diff(fp_a.spectral_centroid, fp_b.spectral_centroid)
        centroid_ok = centroid_diff <= grouping.centroid_tol
        print(
            f"    spectral_centroid rel_diff = {centroid_diff:.4f}  (tol={grouping.centroid_tol})  {PASS if centroid_ok else FAIL}"
            f"  (A={fp_a.spectral_centroid:.1f}, B={fp_b.spectral_centroid:.1f})"
        )
    else:
        print("    spectral_centroid: skipped (one value is 0)")

    zcr_ok = True
    if fp_a.zcr_mean > 0 and fp_b.zcr_mean > 0:
        zcr_diff = rel_diff(fp_a.zcr_mean, fp_b.zcr_mean)
        zcr_ok = zcr_diff <= grouping.zcr_tol
        print(
            f"    zcr_mean rel_diff = {zcr_diff:.4f}  (tol={grouping.zcr_tol})  {PASS if zcr_ok else FAIL}"
            f"  (A={fp_a.zcr_mean:.4f}, B={fp_b.zcr_mean:.4f})"
        )
    else:
        print("    zcr_mean: skipped (one value is 0)")

    features_ok = dur_ok and rms_ok and centroid_ok and zcr_ok
    if not features_ok:
        print("  → BLOCKED: acoustic pre-filter rejected this pair.")
        return

    # ── Step 3: shared hash count (min_matching_hashes) ─────────────────
    print("\n[3] Shared hash count (min_matching_hashes)")
    set_a = {h for h, _ in (fp_a.hashes or [])}
    set_b = {h for h, _ in (fp_b.hashes or [])}
    common = set_a & set_b
    hashes_ok = len(common) >= grouping.min_matching_hashes
    print(
        f"    hashes A: {len(set_a)},  hashes B: {len(set_b)},  shared: {len(common)}"
    )
    print(
        f"    min_matching_hashes = {grouping.min_matching_hashes}  {PASS if hashes_ok else FAIL}"
    )
    if not hashes_ok:
        print("  → BLOCKED: not enough shared hashes to score the pair.")
        return

    # ── Step 4: temporal coherence score (_score) ────────────────────────
    print("\n[4] Fingerprint similarity score (_score)")
    hashes_a = fp_a.hashes or []
    hashes_b = fp_b.hashes or []
    index_a = {h: t for h, t in hashes_a if h in common}
    index_b = {h: t for h, t in hashes_b if h in common}
    offsets = [index_a[h] - index_b[h] for h in common if h in index_a and h in index_b]

    if offsets:
        offset_counts = Counter(offsets)
        best_offset, coherent_count = offset_counts.most_common(1)[0]
        min_hashes = min(len(hashes_a), len(hashes_b)) + 1
        score = coherent_count / min_hashes
        print(f"    offsets computed: {len(offsets)}")
        print(
            f"    most common offset: {best_offset}  (coherent hashes: {coherent_count}/{len(offsets)})"
        )
        print(f"    score = {coherent_count} / {min_hashes} = {score:.4f}")
        print(f"    similarity_threshold = {grouping.similarity_threshold}")
        score_ok = score >= grouping.similarity_threshold
        print(
            f"    {PASS if score_ok else FAIL} score {'≥' if score_ok else '<'} threshold"
        )
        if not score_ok:
            print("  → BLOCKED: score below similarity threshold.")
            return
    else:
        print("    No overlapping offsets could be computed.")
        print("  → BLOCKED: score cannot be computed.")
        return

    print("\n" + "=" * 60)
    print("  → These two chunks WOULD be grouped together.")
    print("=" * 60)


if __name__ == "__main__":
    debug_pair(
        Chunk.from_dict(
            {
                "start_sec": 1746729816.595737,
                "end_sec": 1746729847.9891157,
                "channel": "tf1",
                "duration_sec": 31.39337868480726,
                "energy_mean": 0.039986010640859604,
                "spectral_centroid": 3366.8334208995534,
                "zcr_mean": 0.16432030018715524,
                "peaks": [
                    [1288, 35],
                    [822, 5],
                    [553, 6],
                    [1038, 2],
                    [1132, 5],
                    [734, 8],
                    [987, 42],
                    [404, 47],
                    [132, 54],
                    [1011, 33],
                    [364, 50],
                    [647, 8],
                    [534, 6],
                    [572, 7],
                    [475, 7],
                    [584, 7],
                    [960, 26],
                    [690, 4],
                    [932, 55],
                    [924, 56],
                    [673, 8],
                    [609, 8],
                    [309, 102],
                    [753, 8],
                    [1101, 28],
                    [462, 6],
                    [309, 51],
                    [1180, 50],
                    [881, 22],
                    [1197, 6],
                ],
                "hashes": [
                    ["c52df19cf11b", 309],
                    ["b24014122f8b", 309],
                    ["24abdbba8e9c", 309],
                    ["e83b54fed99e", 309],
                    ["1c4b8febb49d", 364],
                    ["bed5c5612de5", 364],
                    ["e37dafd6e3ba", 404],
                    ["9543c7073c70", 404],
                    ["245d89a09bdb", 462],
                    ["75b0c91a78d5", 462],
                    ["810310f5c8f9", 462],
                    ["032abe20663e", 475],
                    ["73e9660e2606", 475],
                    ["3a7f245fa394", 475],
                    ["4161a3a7407a", 534],
                    ["7ffdb2b42d5a", 534],
                    ["2b3dd3fecaf2", 534],
                    ["cfd54e9174e4", 534],
                    ["0c547d52bd8e", 553],
                    ["d4e729f6efc4", 553],
                    ["810c884da668", 553],
                    ["74d9591f777d", 553],
                    ["e7494f726ee7", 572],
                    ["17f4c058aa26", 572],
                    ["6543fb977a3b", 572],
                    ["eb1cf2eb9020", 584],
                    ["e1b963668d8d", 584],
                    ["676b695dcbc3", 584],
                    ["89b8d16fde12", 609],
                    ["45fa341dd232", 609],
                    ["2461fdeb94dc", 609],
                    ["61920f6119b9", 647],
                    ["44ff6b56e7f3", 647],
                    ["aa86eb27dbbb", 647],
                    ["5e79074ac547", 673],
                    ["212959fb70fa", 673],
                    ["61e38832946d", 673],
                    ["b7e31212aabb", 690],
                    ["82bd7ce1a7b8", 690],
                    ["9268419570f9", 734],
                    ["78ee8eb19c92", 734],
                    ["ea604d751ba4", 753],
                    ["899afdb7a397", 822],
                    ["a81f15a43d71", 881],
                    ["943d10d2b22c", 881],
                    ["9b918a6cd9d6", 881],
                    ["e3c31e9c2d3c", 924],
                    ["cbfbee2f557a", 924],
                    ["6d5fcc3bd5b6", 924],
                    ["7070e6a1149f", 924],
                    ["9f2d2cb3f794", 932],
                    ["7789c138f4e4", 932],
                    ["da3cdf6365cb", 932],
                    ["d988dca620dd", 960],
                    ["7cd33a146d27", 960],
                    ["0b5c8e8c14a0", 960],
                    ["dfe424883fc0", 987],
                    ["d1da40c37d01", 987],
                    ["92f05d841df8", 1011],
                    ["b6960a0326bc", 1011],
                    ["770a87986801", 1038],
                    ["e90828af0500", 1038],
                    ["8098426802c4", 1101],
                    ["f4be4e088fc7", 1101],
                    ["91e7fc048282", 1101],
                    ["56b4bcc6c49a", 1132],
                    ["ee0686867d3e", 1132],
                    ["31f6d42df082", 1180],
                    ["4fd62142f259", 1197],
                ],
            }
        ),
        Chunk.from_dict(
            {
                "start_sec": 1746874679.1687982,
                "end_sec": 1746874691.9862132,
                "channel": "tf1",
                "duration_sec": 12.817414965986472,
                "energy_mean": 0.03261508792638779,
                "spectral_centroid": 2682.5048359943517,
                "zcr_mean": 0.14810633429672446,
                "peaks": [
                    [160, 20],
                    [416, 20],
                    [251, 32],
                    [389, 18],
                    [156, 36],
                    [201, 20],
                    [201, 40],
                    [397, 19],
                    [290, 19],
                    [227, 24],
                    [211, 27],
                    [265, 21],
                    [243, 24],
                    [432, 69],
                    [387, 37],
                    [504, 6],
                    [436, 20],
                    [267, 40],
                    [175, 6],
                    [206, 64],
                    [450, 16],
                    [227, 48],
                    [260, 81],
                    [293, 34],
                    [405, 42],
                    [302, 30],
                    [268, 59],
                    [243, 47],
                    [303, 15],
                    [279, 36],
                ],
                "hashes": [
                    ["15d2b05208ee", 156],
                    ["3f9168446c75", 156],
                    ["f2f726cb3066", 156],
                    ["3a915fd51c6c", 156],
                    ["9ec8f2b33286", 156],
                    ["20e600826088", 156],
                    ["1010f843f65a", 156],
                    ["a598b4f52fbc", 156],
                    ["af98eefa745b", 156],
                    ["388fa86a54b8", 156],
                    ["295500cad13b", 156],
                    ["3c376b670bb2", 160],
                    ["5f7078b6474b", 160],
                    ["1bece7c8579c", 160],
                    ["fae39aa91bf8", 160],
                    ["33dc316baf5f", 160],
                    ["413d6193af1a", 160],
                    ["4d587edd1d3a", 160],
                    ["e82a670194fe", 160],
                    ["41211825e6b9", 160],
                    ["ee0e1cff9ead", 160],
                    ["f789e31cc56f", 160],
                    ["9d9029272414", 175],
                    ["ea842aaa26b8", 175],
                    ["11b7332c207e", 175],
                    ["590b6cddfd69", 175],
                    ["82cfa199a5e3", 175],
                    ["996bdb8f9544", 175],
                    ["93e5c7a2ad07", 175],
                    ["43f73c14f1c3", 175],
                    ["e1d2b5f598f9", 175],
                    ["caac46adbf94", 175],
                    ["f8b3437b49d0", 175],
                    ["f63b51f2dc60", 175],
                    ["b131b6625bcc", 175],
                    ["917fa09ee788", 201],
                    ["8d514b545e7d", 201],
                    ["84d170691c29", 201],
                    ["e9a848d9efb3", 201],
                    ["d5b576530505", 201],
                    ["7431ad1661f4", 201],
                    ["0b197643203a", 201],
                    ["0f55bf3f2b27", 201],
                    ["7ce7727a0a71", 201],
                    ["6d5f583cb713", 201],
                    ["ef0eb6b93c3a", 201],
                    ["015fb244dcd5", 201],
                    ["a4993c01c80e", 201],
                    ["15c3ff60d349", 201],
                    ["3947a19d3d44", 201],
                    ["1381be97e3ac", 201],
                    ["0a493b5089e3", 201],
                    ["fa54e8c393a6", 201],
                    ["33158b9c4a2a", 201],
                    ["77e18333c5e9", 201],
                    ["2d6847390c18", 201],
                    ["7a88dcab0cc0", 201],
                    ["e57707f91362", 201],
                    ["4e25510c4042", 201],
                    ["a91d2c3034d3", 201],
                    ["dff7a35f25a8", 201],
                    ["c4bb5836d4f4", 201],
                    ["0a7cb517064b", 201],
                    ["b8f9f33ccf7d", 206],
                    ["f89d2639376e", 206],
                    ["328a073d84ab", 206],
                    ["fe34400f3c80", 206],
                    ["cd5f61de74e6", 206],
                    ["91ad996f5f42", 206],
                    ["884550038e5e", 206],
                    ["fc4a1387c673", 206],
                    ["5d5419701b86", 206],
                    ["22234a22c0fa", 206],
                    ["96d723f70a2c", 206],
                    ["136577a4dd99", 206],
                    ["59b27dec5cf2", 206],
                    ["6b4f8dbcbebf", 206],
                    ["7030999fc5f8", 206],
                    ["9466bb0e0e69", 211],
                    ["c4e47355d120", 211],
                    ["0bc3ba137023", 211],
                    ["b3531885ffef", 211],
                    ["6a21ee58d633", 211],
                    ["fc161b5e006a", 211],
                    ["507d66aaf03b", 211],
                    ["0b5c2a6c3f71", 211],
                    ["6ff74268e2c8", 211],
                    ["f80a1085a162", 211],
                    ["fff511dc85d0", 211],
                    ["65a1b695babd", 211],
                    ["8e5b41ed2fb4", 211],
                    ["392a5acd42ca", 211],
                    ["2341c05749fa", 227],
                    ["33bdb5bdfd59", 227],
                    ["635f180401fa", 227],
                    ["253834119ed6", 227],
                    ["263a34e99336", 227],
                    ["30e71dfcdfcf", 227],
                    ["801ad5c96e73", 227],
                    ["8b494701a012", 227],
                    ["a6b7cab38138", 227],
                    ["a9d2a3bbaf2e", 227],
                    ["db42cfb79c77", 227],
                    ["b517b0cec9ac", 227],
                    ["19f350b8d2f8", 227],
                    ["7f55b4cca03e", 227],
                    ["4903a1d5886a", 227],
                    ["f7b9b2676301", 227],
                    ["7e573f7ede6f", 227],
                    ["e3c4a46cf02d", 227],
                    ["6e628889c553", 227],
                    ["7d1126cb5227", 227],
                    ["8b575f4b943e", 227],
                    ["f21eb920e72b", 227],
                    ["2fd0df9b992c", 227],
                    ["59bf4577de3e", 227],
                    ["e5450c9702f6", 243],
                    ["0599218a17a2", 243],
                    ["4e7303743908", 243],
                    ["fd9b90506d6a", 243],
                    ["49d64e251db1", 243],
                    ["af91bea0b0ee", 243],
                    ["811234c9ea46", 243],
                    ["d79aa8899bb2", 243],
                    ["c06db2f404bd", 243],
                    ["6869fe0e418c", 243],
                    ["c0072d422a64", 243],
                    ["902f8f510d50", 243],
                    ["284cbca5495b", 243],
                    ["292534257e19", 243],
                    ["3437ab5b40d1", 243],
                    ["1b1a20b56bb2", 243],
                    ["ddb6eb9bde37", 243],
                    ["1bcef72fe7cb", 243],
                    ["5be2e5a118a1", 243],
                    ["43ea317f02c6", 243],
                    ["3e60db5a87d4", 251],
                    ["0c4ce879ea8a", 251],
                    ["0b15867b02b0", 251],
                    ["b6c7715313ac", 251],
                    ["8a18435f9470", 251],
                    ["d6c71f17d671", 251],
                    ["93db901b89a3", 251],
                    ["48b819b09329", 251],
                    ["8937927ef7b4", 251],
                    ["cadde16a3cca", 260],
                    ["73fc040bcd9b", 260],
                    ["92051ea3c7c5", 260],
                    ["2538533dffbd", 260],
                    ["71dab5976aa8", 260],
                    ["47e0229f361f", 260],
                    ["2bd95a7d1c26", 260],
                    ["c776d7ff7ccd", 260],
                    ["d4f5d8375cfc", 265],
                    ["ffae04990929", 265],
                    ["09276e769fd1", 265],
                    ["7179515d379d", 265],
                    ["5744f366fced", 265],
                    ["158e21f13cf1", 265],
                    ["d548603fc5fa", 265],
                    ["b37e8508993c", 267],
                    ["a412f8754f24", 267],
                    ["6d4097c79d52", 267],
                    ["5b890c2953d1", 267],
                    ["1946544e2a4a", 267],
                    ["f260104f2516", 267],
                    ["69b69e1348f9", 268],
                    ["2e873ab72c50", 268],
                    ["3de3a39adc29", 268],
                    ["04b7c4921a86", 268],
                    ["010d1c73fb5e", 268],
                    ["3149815090cf", 279],
                    ["86631c044544", 279],
                    ["3cbc22896488", 279],
                    ["289a1f12a1bc", 279],
                    ["f126c3051400", 290],
                    ["a00a43573f63", 290],
                    ["2b2caf9e6e3b", 290],
                    ["04c1f7a573ee", 290],
                    ["73549391a667", 290],
                    ["2492f12e4246", 293],
                    ["508cfcb64eff", 293],
                    ["404b81181cc5", 293],
                    ["c0dc5766cfdb", 293],
                    ["f5ad62c67527", 302],
                    ["912aef05beea", 302],
                    ["4e9867068a06", 302],
                    ["54e6a17ee014", 302],
                    ["b0646b4d9350", 303],
                    ["887600b0c75b", 303],
                    ["4a909dc76e16", 303],
                    ["79072b11676f", 387],
                    ["3cbf7d03b887", 387],
                    ["b523a0ca4fd7", 387],
                    ["cfb5086e6093", 387],
                    ["4cb6849cd07c", 387],
                    ["5c99c70af5fe", 387],
                    ["ed8d02a5cbdd", 387],
                    ["b46255c84be7", 389],
                    ["1c649fd6d34f", 389],
                    ["e59b2a54ba4b", 389],
                    ["ce19af7f1de8", 389],
                    ["cb38ae9408e7", 389],
                    ["c62c56937220", 389],
                    ["6bd58ffb038c", 397],
                    ["1e366d01840c", 397],
                    ["7fbbf43c40b1", 397],
                    ["03648e7ea89b", 397],
                    ["91795b16c50f", 397],
                    ["30de8f13c5d6", 405],
                    ["82e506f1ea6c", 405],
                    ["d66746445d4a", 405],
                    ["d22d02e42dc8", 405],
                    ["a88b47680a86", 405],
                    ["e4e00542a2fe", 416],
                    ["ce4a85c84206", 416],
                    ["9d2597a4280f", 416],
                    ["a5afff3426ec", 416],
                    ["bde8b906f3c9", 432],
                    ["cdcd57ed86d6", 432],
                    ["17c07f2c32de", 432],
                    ["660c48f4bf11", 436],
                    ["9b7e25194869", 436],
                    ["4b8ae1b23e8f", 450],
                ],
            }
        ),
        ChunkGrouping(
            similarity_threshold=0.05,
            min_matching_hashes=1,
            duration_tol=0.4,
            rms_tol=0.05,
            centroid_tol=0.05,
            zcr_tol=0.1,
        ),
    )
