"""Exploration: find pairs of ads of the database that look like duplicates (the same audio stored as
two ads), from their fingerprints. Nothing is written to the database: the pairs go to a CSV file to
check by eye (screenshots, videos) before deciding what to do with them (merge, correction...).

How it works:
1. The ads having a chunk entry with the fingerprint hash (the current fingerprinter by default) are
   streamed by increasing duration.
2. Each ad becomes one fingerprint of the whole ad: the pairs of its chunks one after the other, their
   time offset shifted by the start of their chunk. Two copies of the same audio then match whatever
   the way they were split in chunks.
3. Sliding window on the duration: an ad is compared only with the ads read before it whose duration
   is at most --duration-tol shorter (no comparison between a 10 s and a 30 s ad).
4. Blocking (--blocking): "brand" compares only the ads with the same brand (normalised like the merge
   of the split ads, tools/ad_merge.brand_key), "none" compares every ad with every ad of the window.
5. Candidates: an inverted index of quantized pairs (same keys as FingerprintsIndex), with a sample of
   --index-pairs pairs per ad, keeps the ads sharing at least --min-shared-keys keys with the new ad.
   The keys shared by more than --max-posting ads of the window are ignored, like stop words.
6. Score: FingerprintsCompare._score of the detection (s01_detection/processor.py tolerances) on the two
   whole-ad fingerprints: share of the pairs of the smaller ad found in the other at a consistent time
   offset (score_min), also given relative to the bigger ad (score_max).
7. The pairs with score_min >= --min-score are grouped (connected components): a group_id gathers
   the ads linked by these pairs. Output, one row per ad of a group (an ad of a group of 2 has its
   own row, as the other one): the CSV with the mosaic and data of the ad, the target proposed for
   a merge of its group (choose_target, first row of the group), its duration and Mediatree link,
   the mosaics of the pieces of a merged ad (one column each), whether it matches chunk by chunk as the detection (chunks_coherent),
   and a row of "-" between groups, and
   a .txt file next to it (<csv name>_metabase.txt) holding a SQL query to run in Metabase, which
   shows the same rows with the mosaic of each ad and links to its video. A second query
   (<csv name>_metabase_multimarques.txt) shows only the groups with several brands, with, for
   each ad, its best match with an ad of another brand (scores, mosaic and transcript of the
   match), to inspect these matches. Groups with several brands need --blocking none.

Thresholds are not calibrated: start on one brand or a duration range, check pairs by eye at several
score levels, then widen. A high score_min with a low score_max means that a short ad is contained in
a longer one (a shared jingle or end of ad), not a duplicate.

Usage (POSTGRES_* env vars of the database holding the advertising schema):
    poetry run python -m quotaclimat.data_ingestion.advertising.s04_consolidation.duplicate_ads.find_duplicates \
        --min-duration 19 --max-duration 21 --output duplicates.csv
"""

from __future__ import annotations

import argparse
import calendar
import csv
import math
import logging
import random
import time
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
import os
from datetime import date, datetime, timedelta

from sqlalchemy import cast, exists, func, literal_column, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import aliased

from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Occurrence,
    Ad_Screenshot,
)

from quotaclimat.data_ingestion.advertising.tools.ad_merge import brand_key
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.compare import (
    _ADJ_3D,
    FingerprintsCompare,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.fingerprint import (
    Fingerprint,
)

logger = logging.getLogger(__name__)

# fragment_type of the ads: "advertising" from the detection, "AD" after the classification
DEFAULT_FRAGMENT_TYPES = ("advertising", "AD")


@dataclass
class AdPrint:
    """One ad, with the fingerprint of the whole ad and its index keys."""

    id: str
    duration_sec: float
    block: str  # blocking key: normalised brand, or "*" without blocking
    fingerprint: Fingerprint
    nb_chunks: int
    index_keys: frozenset = field(default_factory=frozenset)
    meta: dict = field(default_factory=dict)  # copied to the output (brand, sector...)
    chunks: list[Fingerprint] = field(
        default_factory=list
    )  # chunk fingerprints, in order


def whole_ad_fingerprint(
    chunk_fingerprints: list[dict], frames_per_sec: float
) -> Fingerprint:
    """Fingerprint of the whole ad: the pairs of its chunks one after the other, the time offset of
    each pair (4th field, in frames) shifted by the start of its chunk (sum of the durations of the
    chunks before it). Features: means weighted by the duration of the chunks."""
    pairs: list[tuple] = []
    start_sec = 0.0
    weights, energies, centroids, zcrs = [], [], [], []
    for fp in chunk_fingerprints:
        shift = round(start_sec * frames_per_sec)
        for pair in fp.get("pairs") or []:
            pair = list(pair)
            pair[3] = int(pair[3]) + shift
            pairs.append(tuple(pair))
        duration = float(fp.get("duration_sec") or 0.0)
        start_sec += duration
        weights.append(duration)
        energies.append(float(fp.get("energy_mean") or 0.0))
        centroids.append(float(fp.get("spectral_centroid") or 0.0))
        zcrs.append(float(fp.get("zcr_mean") or 0.0))

    def weighted_mean(values: list[float]) -> float:
        total = sum(weights)
        if total <= 0:
            return 0.0
        return sum(v * w for v, w in zip(values, weights)) / total

    return Fingerprint(
        duration_sec=round(start_sec, 2),
        energy_mean=weighted_mean(energies),
        spectral_centroid=weighted_mean(centroids),
        zcr_mean=weighted_mean(zcrs),
        pairs=pairs,
    )


def pair_keys(pairs: Iterable[tuple], freq_tol: int, dt_tol: int) -> list[tuple]:
    """Quantized keys of the pairs, as in FingerprintsIndex.build()."""
    return [(p[0] // freq_tol, p[1] // freq_tol, p[2] // dt_tol) for p in pairs]


def sample_index_keys(
    pairs: list[tuple], n: int, freq_tol: int, dt_tol: int, seed: str
) -> frozenset:
    """At most n distinct keys of the ad, drawn evenly over the ad (deterministic per ad)."""
    keys = list(dict.fromkeys(pair_keys(pairs, freq_tol, dt_tol)))
    if len(keys) <= n:
        return frozenset(keys)
    return frozenset(random.Random(seed).sample(keys, n))


@dataclass
class DuplicatePair:
    ad_a: AdPrint
    ad_b: AdPrint
    shared_keys: int
    score_min: float
    score_max: float
    features_compatible: bool


class DuplicateFinder:
    """Sliding window over ads sorted by increasing duration, with one inverted index per block."""

    def __init__(
        self,
        compare: FingerprintsCompare,
        duration_tol: float = 1.0,
        min_shared_keys: int = 10,
        max_posting: int = 5000,
        min_score: float = 0.2,
    ):
        self.compare = compare
        self.duration_tol = duration_tol
        self.min_shared_keys = min_shared_keys
        self.max_posting = max_posting
        self.min_score = min_score
        self._window: deque[AdPrint] = deque()
        self._index: dict[str, dict[tuple, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        self._ads: dict[str, AdPrint] = {}
        self.stats = Counter()

    def _evict(self, duration_sec: float) -> None:
        while (
            self._window
            and self._window[0].duration_sec < duration_sec - self.duration_tol
        ):
            old = self._window.popleft()
            index = self._index[old.block]
            for key in old.index_keys:
                ids = index.get(key)
                if ids is not None:
                    ids.discard(old.id)
                    if not ids:
                        del index[key]
            if not index:
                del self._index[old.block]
            del self._ads[old.id]

    def _candidates(self, ad: AdPrint) -> Counter:
        index = self._index.get(ad.block)
        if not index:
            return Counter()
        neighbor_keys = {
            (k0 + d0, k1 + d1, k2 + d2)
            for k0, k1, k2 in set(
                pair_keys(
                    ad.fingerprint.pairs, self.compare.freq_tol, self.compare.dt_tol
                )
            )
            for d0, d1, d2 in _ADJ_3D
        }
        shared: Counter = Counter()
        for key in neighbor_keys:
            ids = index.get(key)
            if not ids:
                continue
            if len(ids) > self.max_posting:
                self.stats["skipped_frequent_keys"] += 1
                continue
            shared.update(ids)
        return shared

    def add(self, ad: AdPrint) -> list[DuplicatePair]:
        """Compare the ad with the ads of the window (shorter or of the same duration), then add it."""
        self._evict(ad.duration_sec)
        self.stats["ads"] += 1
        found = []
        for other_id, shared_keys in self._candidates(ad).items():
            if shared_keys < self.min_shared_keys:
                continue
            other = self._ads[other_id]
            self.stats["candidates"] += 1
            score_min = self.compare._score(ad.fingerprint, other.fingerprint)
            if score_min < self.min_score:
                continue
            n_small = min(len(ad.fingerprint.pairs), len(other.fingerprint.pairs))
            n_big = max(len(ad.fingerprint.pairs), len(other.fingerprint.pairs))
            best_count = score_min * (n_small + 1)
            found.append(
                DuplicatePair(
                    ad_a=other,
                    ad_b=ad,
                    shared_keys=shared_keys,
                    score_min=round(score_min, 3),
                    score_max=round(best_count / (n_big + 1), 3),
                    features_compatible=self.compare._features_compatible(
                        ad.fingerprint, other.fingerprint
                    ),
                )
            )
        self.stats["pairs"] += len(found)

        self._window.append(ad)
        self._ads[ad.id] = ad
        index = self._index[ad.block]
        for key in ad.index_keys:
            index[key].add(ad.id)
        self.stats["max_window"] = max(self.stats["max_window"], len(self._window))
        return found


def connected_groups(pairs: list[DuplicatePair]) -> dict[str, tuple[str, int]]:
    """Ad id -> (group id, group size): connected components of the pairs. The group id is the
    smallest ad id of the group."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for pair in pairs:
        root_a, root_b = find(pair.ad_a.id), find(pair.ad_b.id)
        if root_a != root_b:
            parent[max(root_a, root_b)] = min(root_a, root_b)

    members: dict[str, list[str]] = defaultdict(list)
    for ad_id in list(parent):
        members[find(ad_id)].append(ad_id)
    return {ad_id: (root, len(ids)) for root, ids in members.items() for ad_id in ids}


def ads_query(args: argparse.Namespace):
    """Ads having a chunk entry with the hash, by increasing duration, with their live occurrences,
    their brand (the canonical one, or else the raw brand of the LLM, like the merge of the split
    ads) and the fingerprints of that chunk entry."""
    nb_occurrences = (
        select(func.count())
        .where(Ad_Occurrence.ad_id == Ad.id, Ad_Occurrence.deleted_at.is_(None))
        .correlate(Ad)
        .scalar_subquery()
    )
    entry = func.json_array_elements(Ad.chunks).table_valued("value").alias("entry")
    fingerprints = (
        select(entry.c.value.op("->")("fingerprints"))
        .where(entry.c.value.op("->>")("hash") == args.hash)
        .limit(1)
        .correlate(Ad)
        .scalar_subquery()
    )
    mosaic = (
        select(Ad_Screenshot.image_url)
        .where(
            Ad_Screenshot.target_type == "ad",
            Ad_Screenshot.target_id == Ad.id,
            Ad_Screenshot.status == "done",
        )
        .correlate(Ad)
        .scalar_subquery()
    )
    # an ad built by merging pieces (tools/ad_merge.py), whatever the status of the merge
    is_merged = exists(select(1).where(Ad_Merge.merged_ad_id == Ad.id)).correlate(Ad)
    raw_brand = func.jsonb_path_query_first(
        cast(Ad.prediction, JSONB),
        '$[*] ? (@.stage == "ad_type").raw_response.brand_name',
    ).op("#>>")(literal_column("'{}'"))

    query = (
        select(
            Ad.id,
            Ad.duration_sec,
            Ad.fragment_type,
            Ad.first_detection_date,
            Ad.predicted_sector,
            Ad.predicted_product_category,
            func.coalesce(Ad.predicted_brand, raw_brand).label("brand"),
            nb_occurrences.label("nb_occurrences"),
            fingerprints.label("fingerprints"),
            mosaic.label("mosaic"),
            is_merged.label("is_merged"),
        )
        .where(
            fingerprints.is_not(None),
            Ad.deleted_at.is_(None),
            Ad.fragment_type.in_(list(args.fragment_types)),
            nb_occurrences >= args.min_occurrences,
        )
        .order_by(Ad.duration_sec, Ad.id)
    )
    if args.min_duration is not None:
        query = query.where(Ad.duration_sec >= args.min_duration)
    if args.max_duration is not None:
        query = query.where(Ad.duration_sec <= args.max_duration)
    if args.date_from is not None:
        query = query.where(Ad.first_detection_date >= args.date_from)
    if args.date_to is not None:
        query = query.where(Ad.first_detection_date < args.date_to)
    if args.max_ads is not None:
        # sample of ads spread over all durations, stable across runs (hash of the id)
        sampled = aliased(Ad)
        query = query.where(
            Ad.id.in_(
                select(sampled.id).order_by(func.md5(sampled.id)).limit(args.max_ads)
            )
        )
    return query


def stream_ads(
    connection, args: argparse.Namespace, frames_per_sec: float
) -> Iterator[AdPrint]:
    brand_filter = brand_key(args.brand) if args.brand else None
    result = connection.execution_options(stream_results=True, yield_per=1000).execute(
        ads_query(args)
    )
    for row in result.mappings():
        key = brand_key(row["brand"])
        if brand_filter is not None and key != brand_filter:
            continue
        if args.blocking == "brand":
            if not key and not args.include_unbranded:
                continue
            block = key
        else:
            block = "*"
        fingerprint = whole_ad_fingerprint(row["fingerprints"] or [], frames_per_sec)
        if not fingerprint.pairs:
            continue
        yield AdPrint(
            id=row["id"],
            duration_sec=float(row["duration_sec"]),
            block=block,
            fingerprint=fingerprint,
            nb_chunks=len(row["fingerprints"] or []),
            chunks=[Fingerprint.from_dict(c) for c in row["fingerprints"] or []],
            index_keys=sample_index_keys(
                fingerprint.pairs,
                args.index_pairs,
                args.freq_tol,
                args.dt_tol,
                seed=row["id"],
            ),
            meta={
                "brand": row["brand"],
                "sector": row["predicted_sector"],
                "category": row["predicted_product_category"],
                "fragment_type": row["fragment_type"],
                "first_detection_date": row["first_detection_date"],
                "nb_occurrences": row["nb_occurrences"],
                "mosaic": row["mosaic"],
                "is_merged": row["is_merged"],
            },
        )


AD_COLUMNS = [
    "duration_sec",
    "nb_chunks",
    "brand",
    "sector",
    "category",
    "fragment_type",
    "first_detection_date",
    "nb_occurrences",
]
# Groups with several brands: the brands of the group, and the matches of the ad with ads of
# another brand only (empty when the ad has no brand, or is linked to the other brands only
# through ads of its own brand)
MULTIBRAND_COLUMNS = [
    "group_brands",
    "is_multibrand_group",
    "nb_matches_other_brand",
    "other_brand_match_ad_id",
    "other_brand_match_brand",
    "other_brand_score_min",
    "other_brand_score_max",
    "other_brand_shared_keys",
    "other_brand_features_compatible",
    "other_brand_duration_diff_sec",
    "other_brand_matches",
]


def choose_target(ads: list[AdPrint]) -> AdPrint:
    """Ad of a group proposed as the target of a merge of the group:
    1. among the ads of the most frequent brand (brand_key; several brands when tied; the ads
       without brand only when no ad of the group has one);
    2. the ads that are not a merged ad (tools/ad_merge.py) when there are some: more likely to
       have their video and their data;
    3. the most broadcast one (live occurrences), then the smallest id."""
    by_brand: dict[str, list[AdPrint]] = defaultdict(list)
    for ad in ads:
        by_brand[brand_key(ad.meta.get("brand"))].append(ad)
    branded = {key: group for key, group in by_brand.items() if key} or by_brand
    biggest = max(len(group) for group in branded.values())
    candidates = [
        ad for group in branded.values() if len(group) == biggest for ad in group
    ]
    not_merged = [ad for ad in candidates if not ad.meta.get("is_merged")]
    candidates = not_merged or candidates
    return min(
        candidates, key=lambda ad: (-(ad.meta.get("nb_occurrences") or 0), ad.id)
    )


def chunks_coherent(compare: FingerprintsCompare, ad_a: AdPrint, ad_b: AdPrint) -> bool:
    """Whether the two ads match chunk by chunk, in order, as the detection recognises a known ad
    (s01_detection/e03_already_identified_advertising.py): same number of chunks, and chunk i of
    one is_similar to chunk i of the other (acoustic pre-filter, then score threshold)."""
    if not ad_a.chunks or len(ad_a.chunks) != len(ad_b.chunks):
        return False
    return all(compare.is_similar(a, b) for a, b in zip(ad_a.chunks, ad_b.chunks))


def group_rows(
    pairs: list[DuplicatePair], compare: FingerprintsCompare | None = None
) -> list[dict]:
    """One row per ad of a group, sorted by group (biggest first), the target of the group first
    (choose_target, in target_ad_id on every row of the group), then by occurrences. Matching columns: the
    number of ads of the group it matches directly, its best match (highest score_min) with its
    scores, and every direct match as "ad_id:score_min" separated by spaces. Plus the
    MULTIBRAND_COLUMNS: a group is multi-brand when its ads have at least two different brands
    (brand_key, the ads without brand not counted). With compare: chunk_coherent (one of its direct
    matches at least is chunk-coherent, see chunks_coherent) and chunk_coherent_target (with the
    target of the group, empty on the target's own row)."""
    groups = connected_groups(pairs)
    ads: dict[str, AdPrint] = {}
    matches: dict[str, list[tuple[DuplicatePair, AdPrint]]] = defaultdict(list)
    for pair in pairs:
        ads[pair.ad_a.id], ads[pair.ad_b.id] = pair.ad_a, pair.ad_b
        matches[pair.ad_a.id].append((pair, pair.ad_b))
        matches[pair.ad_b.id].append((pair, pair.ad_a))

    group_brands: dict[str, dict[str, str]] = defaultdict(dict)  # group -> key -> brand
    for ad_id, ad in ads.items():
        key = brand_key(ad.meta.get("brand"))
        if key:
            group_brands[groups[ad_id][0]].setdefault(key, ad.meta["brand"])

    group_ads: dict[str, list[AdPrint]] = defaultdict(list)
    for ad_id, ad in ads.items():
        group_ads[groups[ad_id][0]].append(ad)
    targets = {
        group_id: choose_target(members).id for group_id, members in group_ads.items()
    }

    rows = []
    for ad_id, ad in ads.items():
        group_id, group_size = groups[ad_id]
        own_key = brand_key(ad.meta.get("brand"))
        other_brand = [
            (p, other)
            for p, other in sorted(
                matches[ad_id], key=lambda m: (-m[0].score_min, m[1].id)
            )
            if own_key
            and brand_key(other.meta.get("brand"))
            and brand_key(other.meta.get("brand")) != own_key
        ]
        ad_matches = sorted(matches[ad_id], key=lambda m: (-m[0].score_min, m[1].id))
        best_pair, best_ad = ad_matches[0]
        rows.append(
            {
                "group_id": group_id,
                "group_size": group_size,
                "ad_id": ad_id,
                "target_ad_id": targets[group_id],
                "mosaic": ad.meta.get("mosaic"),
                "is_merged": ad.meta.get("is_merged"),
                "nb_matches": len(ad_matches),
                "best_match_ad_id": best_ad.id,
                "best_score_min": best_pair.score_min,
                "best_score_max": best_pair.score_max,
                "best_shared_keys": best_pair.shared_keys,
                "matches": " ".join(
                    f"{other.id}:{p.score_min}" for p, other in ad_matches
                ),
                "duration_sec": ad.duration_sec,
                "nb_chunks": ad.nb_chunks,
                **{column: ad.meta.get(column) for column in AD_COLUMNS[2:]},
                "group_brands": " | ".join(sorted(group_brands[group_id].values())),
                "is_multibrand_group": len(group_brands[group_id]) >= 2,
                **other_brand_columns(ad, other_brand),
                "chunk_coherent": (
                    any(chunks_coherent(compare, ad, other) for _, other in ad_matches)
                    if compare
                    else None
                ),
                "chunk_coherent_target": (
                    chunks_coherent(compare, ad, ads[targets[group_id]])
                    if compare and ad_id != targets[group_id]
                    else None
                ),
            }
        )
    return sorted(
        rows,
        key=lambda r: (
            -r["group_size"],
            r["group_id"],
            r["ad_id"] != r["target_ad_id"],
            -(r["nb_occurrences"] or 0),
            r["ad_id"],
        ),
    )


def other_brand_columns(
    ad: AdPrint, other_brand: list[tuple[DuplicatePair, AdPrint]]
) -> dict:
    """MULTIBRAND_COLUMNS of the matches of the ad with ads of another brand (best first)."""
    if not other_brand:
        return {"nb_matches_other_brand": 0} | {
            column: None for column in MULTIBRAND_COLUMNS[3:]
        }
    best_pair, best_ad = other_brand[0]
    return {
        "nb_matches_other_brand": len(other_brand),
        "other_brand_match_ad_id": best_ad.id,
        "other_brand_match_brand": best_ad.meta.get("brand"),
        "other_brand_score_min": best_pair.score_min,
        "other_brand_score_max": best_pair.score_max,
        "other_brand_shared_keys": best_pair.shared_keys,
        "other_brand_features_compatible": best_pair.features_compatible,
        "other_brand_duration_diff_sec": round(
            best_ad.duration_sec - ad.duration_sec, 2
        ),
        "other_brand_matches": " ".join(
            f"{other.id}({other.meta.get('brand')}):{p.score_min}"
            for p, other in other_brand
        ),
    }


# CSV column -> key of the row (None: left empty, mosaique_image is for an image formula)
MAX_SOURCE_MOSAICS = 5
CSV_COLUMNS = {
    "group_id": "group_id",
    "group_size": "group_size",
    "mosaique": "mosaic",
    "mosaique_image": None,
    "ad_id": "ad_id",
    "target_ad_id": "target_ad_id",
    "brand": "brand",
    "sector": "sector",
    "category": "category",
    "nb_occurrences": "nb_occurrences",
    "duration_sec": "duration_sec",
    "lien_mediatree": "mediatree_link",
    "nb_sources_fusion": "nb_merge_sources",
    # mosaics of the pieces of a merged ad, each one followed by an empty column for an image formula
    **{
        column: key
        for i in range(1, MAX_SOURCE_MOSAICS + 1)
        for column, key in (
            (f"mosaique_source_{i}", f"source_mosaic_{i}"),
            (f"mosaique_source_{i}_image", None),
        )
    },
    "chunk_coherent": "chunk_coherent",
    "chunk_coherent_target": "chunk_coherent_target",
}
SEPARATOR_ROW = {column: "-" for column in CSV_COLUMNS}


def write_csv(path: str, rows: list[dict]) -> None:
    """The rows of group_rows, with a row of "-" between two groups."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        previous_group = None
        for row in rows:
            if previous_group is not None and row["group_id"] != previous_group:
                writer.writerow(SEPARATOR_ROW)
            previous_group = row["group_id"]
            writer.writerow(
                {
                    column: row.get(key) if key else None
                    for column, key in CSV_COLUMNS.items()
                }
            )


def mediatree_link(channel_name: str, start: datetime, duration_sec: float) -> str:
    """Mediatree player on a broadcast, like lien_mediatree of curated.publicite (start in UTC: its
    epoch is the cts of Mediatree)."""
    epoch = calendar.timegm(start.utctimetuple()) + start.microsecond / 1e6
    return (
        f"https://keywords.mediatree.fr/player/?fifo={channel_name}"
        f"&start_cts={math.floor(epoch)}&end_cts={math.ceil(epoch + duration_sec)}"
        f"&position_cts={math.floor(epoch)}"
    )


def _batches(values: list, size: int = 1000) -> Iterator[list]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def screenshot_ad_ids(connection, rows: list[dict]) -> list[str]:
    """The ads of the rows and, for the merged ads, the ads they were merged from: the ads whose
    mosaics the CSV shows."""
    ad_ids = {row["ad_id"] for row in rows}
    sources = set()
    for batch in _batches(sorted(ad_ids)):
        for (source_ad_ids,) in connection.execute(
            select(Ad_Merge.source_ad_ids).where(Ad_Merge.merged_ad_id.in_(batch))
        ):
            sources.update(source_ad_ids)
    return sorted(ad_ids | sources)


def make_screenshots(ad_ids: list[str]) -> None:
    """Make the missing mosaics of these ads with the screenshots job (s05_enrichment/e02_screenshots),
    a merged ad without its own video from the videos of its pieces (merged_from_sources)."""
    import asyncio

    from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.run import (
        run_ads_by_ids,
    )

    logger.info(f"Screenshots of {len(ad_ids)} ads (missing mosaics only)")
    asyncio.run(run_ads_by_ids(ad_ids, merged_from_sources=True))


def enrich_rows(
    connection, rows: list[dict], link_max_age_days: int, now: datetime | None = None
) -> None:
    """Add to the rows, from the database:
    - mosaic: the mosaic of the ad (read again: --screenshots may have made it since the ads were read);
    - mediatree_link: Mediatree player on the last live occurrence of the ad, when it is less than
      link_max_age_days old (Mediatree keeps a limited history, like curated.publicite);
    - nb_merge_sources: when the ad is a merged ad (advertising.ad_merge), the number of ads it was
      merged from;
    - source_mosaic_1 to source_mosaic_<MAX_SOURCE_MOSAICS>: the mosaics of these ads, in broadcast
      order, the first ones only, pieces without mosaic skipped. The screenshots job takes the most
      broadcast ads first (only those broadcast since OCCURRENCES_FROM when set): the pieces of a
      merge, whose occurrences are deleted, come last or not at all, most of them have none."""
    now = now or datetime.utcnow()
    ad_ids = list({row["ad_id"] for row in rows})
    last_occurrence: dict[str, tuple[datetime, str]] = {}
    sources: dict[str, list[str]] = {}
    for batch in _batches(ad_ids):
        for ad_id, occurrence_date, channel_name in connection.execute(
            select(
                Ad_Occurrence.ad_id,
                Ad_Occurrence.occurrence_date,
                Ad_Occurrence.channel_name,
            )
            .where(Ad_Occurrence.ad_id.in_(batch), Ad_Occurrence.deleted_at.is_(None))
            .distinct(Ad_Occurrence.ad_id)
            .order_by(Ad_Occurrence.ad_id, Ad_Occurrence.occurrence_date.desc())
        ):
            last_occurrence[ad_id] = (occurrence_date, channel_name)
        for merged_ad_id, source_ad_ids in connection.execute(
            select(Ad_Merge.merged_ad_id, Ad_Merge.source_ad_ids).where(
                Ad_Merge.merged_ad_id.in_(batch)
            )
        ):
            sources[merged_ad_id] = list(source_ad_ids)

    source_ids = list(
        {ad_id for ids in sources.values() for ad_id in ids} | set(ad_ids)
    )
    mosaics: dict[str, str] = {}
    for batch in _batches(source_ids):
        for target_id, image_url in connection.execute(
            select(Ad_Screenshot.target_id, Ad_Screenshot.image_url).where(
                Ad_Screenshot.target_type == "ad",
                Ad_Screenshot.target_id.in_(batch),
                Ad_Screenshot.status == "done",
            )
        ):
            mosaics[target_id] = image_url

    for row in rows:
        row["mosaic"] = mosaics.get(row["ad_id"])
        occurrence = last_occurrence.get(row["ad_id"])
        row["mediatree_link"] = (
            mediatree_link(occurrence[1], occurrence[0], row["duration_sec"])
            if occurrence and occurrence[0] >= now - timedelta(days=link_max_age_days)
            else None
        )
        ad_sources = sources.get(row["ad_id"], [])
        row["nb_merge_sources"] = len(ad_sources) if ad_sources else None
        source_mosaics = [mosaics[i] for i in ad_sources if i in mosaics]
        for i in range(MAX_SOURCE_MOSAICS):
            row[f"source_mosaic_{i + 1}"] = (
                source_mosaics[i] if i < len(source_mosaics) else None
            )


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


METABASE_QUERY = """-- Doublons de pubs (s04_consolidation/duplicate_ads/find_duplicates.py), {nb_rows} lignes.
-- Une ligne par pub d'un groupe : les pubs d'un groupe sont reliées par des paires dont l'audio se
-- ressemble. meilleur_match_id est la pub du groupe la plus proche (elle a sa propre ligne).
-- Colonnes à afficher en image (réglage de colonne « Afficher comme : Image ») : mosaique.
-- Colonnes à afficher en lien : lien_sauvegarde, lien_mediatree (dernière diffusion, le lien
-- Mediatree n'est valable que pour les diffusions récentes).
WITH resultat (groupe_id, taille_groupe, ad_id, nb_matches, meilleur_match_id, meilleur_score_min,
               meilleur_score_max, matches) AS (
  VALUES
{values}
)
SELECT
  r.groupe_id,
  r.taille_groupe,
  r.ad_id,
  s.image_url AS mosaique,
  a.duration_sec AS duree_sec,
  COALESCE(a.predicted_brand,
    jsonb_path_query_first(a.prediction::jsonb, '$[*] ? (@.stage == "ad_type").raw_response.brand_name') #>> '{{}}'
  ) AS marque,
  a.predicted_sector AS secteur,
  a.predicted_product_category AS categorie,
  a.fragment_type AS type,
  a.prediction_status AS statut_classification,
  a.transcript AS transcription,
  a.first_detection_date AS premiere_detection,
  occ.nb_diffusions,
  occ.derniere_diffusion,
  occ.derniere_chaine,
  r.nb_matches,
  r.meilleur_match_id,
  r.meilleur_score_min,
  r.meilleur_score_max,
  r.matches,
  'https://console.scaleway.com/object-storage/buckets/fr-par/{bucket}/files/ads/' || r.ad_id || '/'
    AS lien_sauvegarde,
  'https://keywords.mediatree.fr/player/?fifo=' || occ.derniere_chaine
    || '&start_cts=' || FLOOR(EXTRACT(EPOCH FROM occ.derniere_diffusion))::bigint
    || '&end_cts=' || CEIL(EXTRACT(EPOCH FROM occ.derniere_diffusion) + a.duration_sec)::bigint
    || '&position_cts=' || FLOOR(EXTRACT(EPOCH FROM occ.derniere_diffusion))::bigint
    AS lien_mediatree
FROM resultat r
JOIN advertising.ad a ON a.id = r.ad_id
LEFT JOIN advertising.ad_screenshot s
  ON s.target_type = 'ad' AND s.target_id = r.ad_id AND s.status = 'done'
LEFT JOIN LATERAL (
  SELECT
    count(*) OVER () AS nb_diffusions,
    o.occurrence_date AS derniere_diffusion,
    o.channel_name AS derniere_chaine
  FROM advertising.ad_occurrence o
  WHERE o.ad_id = r.ad_id AND o.deleted_at IS NULL
  ORDER BY o.occurrence_date DESC
  LIMIT 1
) occ ON true
WHERE r.ad_id IS NOT NULL
ORDER BY r.taille_groupe DESC, r.groupe_id, r.ad_id
"""


def metabase_query(rows: list[dict], advertising_bucket: str) -> str:
    """Metabase SQL query showing the rows with the data of the ads (mosaic, metadata, links): the
    ids and matching columns of the rows are written in the query."""
    columns = [
        "group_id",
        "group_size",
        "ad_id",
        "nb_matches",
        "best_match_ad_id",
        "best_score_min",
        "best_score_max",
        "matches",
    ]
    if rows:
        values = ",\n".join(
            "    (" + ", ".join(_sql_literal(row[c]) for c in columns) + ")"
            for row in rows
        )
    else:
        # no duplicate: one empty typed row, filtered out by the query, so that it still runs
        values = "    (NULL::text, 0, NULL::text, 0, NULL::text, 0.0, 0.0, NULL::text)"
    return METABASE_QUERY.format(
        nb_rows=len(rows), values=values, bucket=advertising_bucket
    )


def metabase_query_path(csv_path: str, suffix: str = "metabase") -> str:
    root, _ = os.path.splitext(csv_path)
    return f"{root}_{suffix}.txt"


MULTIBRAND_QUERY = """-- Doublons de pubs entre marques différentes (s04_consolidation/duplicate_ads/find_duplicates.py),
-- {nb_groups} groupes, {nb_rows} lignes.
-- Seulement les groupes dont les pubs ont au moins deux marques différentes (marques normalisées,
-- pubs sans marque non comptées). Une ligne par pub du groupe, chaque pub a sa propre ligne.
-- Les colonnes match_* ne décrivent que le match de la pub avec une pub d'une AUTRE marque (la plus
-- proche). Elles sont vides quand la pub n'a pas de marque, ou n'est reliée aux autres marques
-- que par des pubs de sa propre marque (lien indirect, voir les autres lignes du groupe).
-- Indices pour juger un match (raisonnement, seuils non calibrés) :
--   - match_score_min : part de l'audio de la plus courte des deux pubs retrouvée dans l'autre ;
--     match_score_max : la même part rapportée à la plus longue ;
--   - deux scores proches et élevés : même audio sur presque toute la durée, donc probablement la
--     même pub, dont la marque est fausse d'un côté (comparer transcription et transcription_match) ;
--   - score_min élevé et score_max bas : la plus courte est contenue dans la plus longue (jingle,
--     musique ou fin de pub commune, par exemple d'un même groupe d'annonceurs) ;
--   - scores proches du seuil : ressemblance partielle (musique de fond, voix off), souvent un faux
--     positif ;
--   - match_ecart_duree_sec : durée du match moins durée de la pub ;
--   - match_acoustique_compatible : le pré-filtre acoustique de la détection (énergie, centroïde
--     spectral, passages par zéro, durée) accepte les deux pubs entières.
-- Colonnes à afficher en image (réglage de colonne « Afficher comme : Image ») : mosaique,
-- mosaique_match. Colonnes à afficher en lien : lien_sauvegarde, lien_sauvegarde_match.
WITH resultat (groupe_id, taille_groupe, marques_groupe, ad_id, nb_matches_autre_marque,
               match_id, marque_match, match_score_min, match_score_max, match_cles_communes,
               match_acoustique_compatible, match_ecart_duree_sec, matches_autre_marque) AS (
  VALUES
{values}
)
SELECT
  r.groupe_id,
  r.taille_groupe,
  r.marques_groupe,
  r.ad_id,
  s.image_url AS mosaique,
  COALESCE(a.predicted_brand,
    jsonb_path_query_first(a.prediction::jsonb, '$[*] ? (@.stage == "ad_type").raw_response.brand_name') #>> '{{}}'
  ) AS marque,
  a.predicted_sector AS secteur,
  a.predicted_product_category AS categorie,
  a.fragment_type AS type,
  a.prediction_status AS statut_classification,
  a.duration_sec AS duree_sec,
  a.transcript AS transcription,
  (SELECT count(*) FROM advertising.ad_occurrence o
    WHERE o.ad_id = r.ad_id AND o.deleted_at IS NULL) AS nb_diffusions,
  r.nb_matches_autre_marque,
  r.match_id,
  r.marque_match,
  r.match_score_min,
  r.match_score_max,
  r.match_cles_communes,
  r.match_acoustique_compatible,
  r.match_ecart_duree_sec,
  sm.image_url AS mosaique_match,
  m.transcript AS transcription_match,
  r.matches_autre_marque,
  'https://console.scaleway.com/object-storage/buckets/fr-par/{bucket}/files/ads/' || r.ad_id || '/'
    AS lien_sauvegarde,
  'https://console.scaleway.com/object-storage/buckets/fr-par/{bucket}/files/ads/' || r.match_id || '/'
    AS lien_sauvegarde_match
FROM resultat r
JOIN advertising.ad a ON a.id = r.ad_id
LEFT JOIN advertising.ad_screenshot s
  ON s.target_type = 'ad' AND s.target_id = r.ad_id AND s.status = 'done'
LEFT JOIN advertising.ad m ON m.id = r.match_id
LEFT JOIN advertising.ad_screenshot sm
  ON sm.target_type = 'ad' AND sm.target_id = r.match_id AND sm.status = 'done'
WHERE r.ad_id IS NOT NULL
ORDER BY r.taille_groupe DESC, r.groupe_id, r.ad_id
"""


def metabase_multibrand_query(rows: list[dict], advertising_bucket: str) -> str:
    """Metabase SQL query showing the rows of the groups with several brands, with the matches of
    each ad with the other brands, the mosaics and the links of the ad and of its match."""
    columns = [
        "group_id",
        "group_size",
        "group_brands",
        "ad_id",
        "nb_matches_other_brand",
        "other_brand_match_ad_id",
        "other_brand_match_brand",
        "other_brand_score_min",
        "other_brand_score_max",
        "other_brand_shared_keys",
        "other_brand_features_compatible",
        "other_brand_duration_diff_sec",
        "other_brand_matches",
    ]
    multibrand = [row for row in rows if row["is_multibrand_group"]]
    # typed first row (NULL columns would be text otherwise), filtered out by the query
    values = [
        "    (NULL::text, 0, NULL::text, NULL::text, 0, NULL::text, NULL::text, "
        "0.0::double precision, 0.0::double precision, 0, NULL::boolean, "
        "0.0::double precision, NULL::text)"
    ] + [
        "    (" + ", ".join(_sql_literal(row[c]) for c in columns) + ")"
        for row in multibrand
    ]
    return MULTIBRAND_QUERY.format(
        nb_groups=len({row["group_id"] for row in multibrand}),
        nb_rows=len(multibrand),
        values=",\n".join(values),
        bucket=advertising_bucket,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    from quotaclimat.data_ingestion.advertising.s01_detection.processor import (
        fingerprints_compare,
    )
    from quotaclimat.data_ingestion.advertising.tools.fingerprints import fingerprinter

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--hash",
        default=fingerprinter.params_hash(),
        help="hash of the chunk entry to read (default: the current fingerprinter)",
    )
    parser.add_argument(
        "--output",
        default=f"duplicate_ads_{datetime.now():%Y-%m-%dT%H-%M-%S}.csv",
        help="local CSV file (default: duplicate_ads_<date and time>.csv)",
    )
    parser.add_argument(
        "--output-bucket",
        help="also upload the CSV to this bucket (BUCKET/BUCKET_SECRET credentials, "
        "e.g. test-advertising-detection)",
    )
    parser.add_argument(
        "--advertising-bucket",
        default=os.environ.get("ADVERTISING_BUCKET_NAME") or "advertising-detection",
        help="bucket of the ads videos, for the links of the Metabase query "
        "(default: ADVERTISING_BUCKET_NAME, else advertising-detection)",
    )
    parser.add_argument(
        "--output-prefix",
        default="duplicate_ads",
        help="folder of the CSV in --output-bucket (default duplicate_ads)",
    )
    parser.add_argument("--blocking", choices=["brand", "none"], default="brand")
    parser.add_argument(
        "--include-unbranded",
        action="store_true",
        help="with --blocking brand, also compare the ads without brand among themselves",
    )
    parser.add_argument("--brand", help="only the ads of this brand (normalised)")
    parser.add_argument("--min-duration", type=float)
    parser.add_argument("--max-duration", type=float)
    parser.add_argument(
        "--date-from",
        type=date.fromisoformat,
        help="first_detection_date >= (ISO date)",
    )
    parser.add_argument(
        "--date-to", type=date.fromisoformat, help="first_detection_date < (ISO date)"
    )
    parser.add_argument(
        "--fragment-types", nargs="+", default=list(DEFAULT_FRAGMENT_TYPES)
    )
    parser.add_argument(
        "--min-occurrences",
        type=int,
        default=1,
        help="occurrences not deleted (default 1: skips the pieces of merged ads and the junk)",
    )
    parser.add_argument(
        "--max-ads",
        type=int,
        help="random sample of ads (stable), to try the thresholds",
    )
    parser.add_argument(
        "--screenshots",
        action="store_true",
        help="before writing the CSV, make the missing mosaics of the ads of the groups and of "
        "the pieces of the merged ones (screenshots job, a merged ad without video from the "
        "videos of its pieces)",
    )
    parser.add_argument(
        "--mediatree-link-max-age-days",
        type=int,
        default=365,
        help="no Mediatree link for a last occurrence older than this (default 365, as "
        "curated.publicite)",
    )
    parser.add_argument("--duration-tol", type=float, default=1.0)
    parser.add_argument("--index-pairs", type=int, default=60)
    parser.add_argument("--min-shared-keys", type=int, default=10)
    parser.add_argument("--max-posting", type=int, default=5000)
    parser.add_argument("--min-score", type=float, default=0.2)
    args = parser.parse_args(argv)
    args.compare = fingerprints_compare
    args.freq_tol = fingerprints_compare.freq_tol
    args.dt_tol = fingerprints_compare.dt_tol
    args.frames_per_sec = fingerprinter.sr / fingerprinter.hop_length
    return args


def run(connection, args: argparse.Namespace) -> list[DuplicatePair]:
    finder = DuplicateFinder(
        compare=args.compare,
        duration_tol=args.duration_tol,
        min_shared_keys=args.min_shared_keys,
        max_posting=args.max_posting,
        min_score=args.min_score,
    )
    pairs: list[DuplicatePair] = []
    started = time.monotonic()
    for ad in stream_ads(connection, args, args.frames_per_sec):
        pairs.extend(finder.add(ad))
        if finder.stats["ads"] % 5000 == 0:
            logger.info(
                f"{dict(finder.stats)}, duration {ad.duration_sec:.1f}s, "
                f"{time.monotonic() - started:.0f}s elapsed"
            )
    logger.info(f"Done: {dict(finder.stats)} in {time.monotonic() - started:.0f}s")
    return pairs


def main(argv: list[str] | None = None) -> None:
    from postgres.database_connection import connect_to_db

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args(argv)
    logger.info(f"Fingerprint hash {args.hash}, blocking {args.blocking}")
    engine = connect_to_db()
    with engine.connect() as connection:
        pairs = run(connection, args)
        rows = group_rows(pairs, args.compare)
        if args.screenshots:
            ad_ids = screenshot_ad_ids(connection, rows)
    if args.screenshots:
        make_screenshots(ad_ids)
    with engine.connect() as connection:
        enrich_rows(connection, rows, args.mediatree_link_max_age_days)
    write_csv(args.output, rows)
    query_path = metabase_query_path(args.output)
    with open(query_path, "w") as f:
        f.write(metabase_query(rows, args.advertising_bucket))
    multibrand_query_path = metabase_query_path(args.output, "metabase_multimarques")
    with open(multibrand_query_path, "w") as f:
        f.write(metabase_multibrand_query(rows, args.advertising_bucket))
    nb_groups = len({row["group_id"] for row in rows})
    nb_multibrand = len({row["group_id"] for row in rows if row["is_multibrand_group"]})
    logger.info(
        f"{len(pairs)} pairs, {len(rows)} ads in {nb_groups} groups "
        f"({nb_multibrand} with several brands) written to {args.output}, "
        f"{query_path} and {multibrand_query_path} (Metabase queries)"
    )
    if args.output_bucket:
        for path in (args.output, query_path, multibrand_query_path):
            logger.info(f"Uploaded to s3://{upload_file(args, path)}")


def output_s3_path(bucket: str, prefix: str, local_path: str) -> str:
    folder = "/".join(part for part in (bucket, prefix.strip("/")) if part)
    return f"{folder}/{os.path.basename(local_path)}"


def upload_file(args: argparse.Namespace, path: str) -> str:
    """Upload a file to --output-bucket/--output-prefix, return its S3 path."""
    from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
        get_s3_filesystem,
    )

    s3_path = output_s3_path(args.output_bucket, args.output_prefix, path)
    get_s3_filesystem().put_file(path, s3_path)
    return s3_path


if __name__ == "__main__":
    main()
