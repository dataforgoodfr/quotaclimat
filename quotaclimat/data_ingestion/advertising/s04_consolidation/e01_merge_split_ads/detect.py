"""Detection of the ads split in several pieces by the detection step.

Two consecutive pieces on a channel are the same ad when they have the same brand (two ads of the
same brand never follow each other) and only short segments (shorter than short_sec, whatever their
brand) and at most gap_max_sec of uncovered time separate them. The brand is the canonical one when the
brand dictionary found it, the raw brand of the LLM otherwise (dict_miss).

Only the unambiguous cases are kept (see PairType): the same ad twice in a row and the overlapping or
included pieces look like detection errors, not like a split ad.
"""

import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from sqlalchemy import text
from sqlalchemy.engine import Connection


class PairType(str, Enum):
    SAME_AD = "same_ad"  # the same ad twice in a row
    CONTIGUOUS = "contiguous"  # nothing between, gap of at most 1 s
    SHORT_UNBRANDED = "short_unbranded"  # separated by short segments without brand
    SHORT_OTHER_BRAND = (
        "short_other_brand"  # separated by a short segment of another brand
    )
    INCLUDED = "included"  # piece 2 inside piece 1
    OVERLAP = "overlap"  # pieces overlapping by more than 1 s
    GAP = "gap"  # gap of 1 to gap_max_sec without segment


MERGEABLE_PAIR_TYPES = {
    PairType.CONTIGUOUS,
    PairType.SHORT_UNBRANDED,
    PairType.SHORT_OTHER_BRAND,
}

# the pairs whose first piece starts in [date_start, date_end + 5 min): the margin completes the chains
# starting before date_end (see build_sequences)
PAIRS_QUERY = text(r"""
WITH occ AS (
  SELECT
    o.id AS occurrence_id,
    o.channel_name,
    o.ad_id,
    o.occurrence_date AS start_date,
    o.occurrence_date + make_interval(secs => a.duration_sec) AS end_date,
    a.duration_sec,
    COALESCE(
      a.predicted_brand,
      jsonb_path_query_first(a.prediction::jsonb, '$[*] ? (@.stage == "ad_type").raw_response.brand_name') #>> '{}'
    ) AS brand,
    a.duration_sec >= :short_sec AS is_long
  FROM advertising.ad_occurrence o
  JOIN advertising.ad a ON a.id = o.ad_id
  WHERE o.deleted_at IS NULL
    AND o.occurrence_date >= :date_start
    AND o.occurrence_date < :date_end + interval '10 minutes'
),
seq AS (
  SELECT
    occ.*,
    regexp_replace(lower(brand), '\W|_', '', 'g') AS brand_key,
    -- position of the segment on the channel, and number of long segments seen so far
    ROW_NUMBER() OVER w AS rn,
    SUM(CASE WHEN is_long THEN 1 ELSE 0 END) OVER w AS long_seq
  FROM occ
  WINDOW w AS (PARTITION BY channel_name ORDER BY start_date, occurrence_id)
),
-- for each segment with a brand (short or long): the next segment of the same brand on the channel
cand AS (
  SELECT
    seq.*,
    LEAD(occurrence_id) OVER b AS next_occurrence_id,
    LEAD(ad_id)         OVER b AS next_ad_id,
    LEAD(start_date)    OVER b AS next_start_date,
    LEAD(end_date)      OVER b AS next_end_date,
    LEAD(is_long)       OVER b AS next_is_long,
    LEAD(rn)            OVER b AS next_rn,
    LEAD(long_seq)      OVER b AS next_long_seq
  FROM seq
  WHERE brand_key <> ''
  WINDOW b AS (PARTITION BY channel_name, brand_key ORDER BY start_date, occurrence_id)
),
-- only short segments (with or without brand) between the two pieces
pairs_raw AS (
  SELECT c.*
  FROM cand c
  WHERE c.next_rn IS NOT NULL
    AND c.start_date < :date_end + interval '5 minutes'
    AND c.next_long_seq - c.long_seq - (CASE WHEN c.next_is_long THEN 1 ELSE 0 END) = 0
    AND c.next_start_date - c.end_date
        <= make_interval(secs => :gap_max_sec + (c.next_rn - c.rn - 1) * :short_sec)
)
SELECT
  c.channel_name,
  c.occurrence_id,
  c.next_occurrence_id,
  c.ad_id,
  c.next_ad_id,
  c.brand,
  c.start_date,
  c.end_date,
  c.next_start_date,
  c.next_end_date,
  COALESCE(e.between_ids, ARRAY[]::text[]) AS between_occurrence_ids,
  COALESCE(e.between_sec, 0) AS between_sec,
  COALESCE(e.nb_other_brand, 0) AS nb_other_brand
FROM pairs_raw c
LEFT JOIN LATERAL (
  -- segments between the two pieces (read in the source tables to use the index on the date)
  SELECT
    array_agg(o2.id ORDER BY o2.occurrence_date, o2.id) AS between_ids,
    SUM(a2.duration_sec) AS between_sec,
    COUNT(COALESCE(
      a2.predicted_brand,
      jsonb_path_query_first(a2.prediction::jsonb, '$[*] ? (@.stage == "ad_type").raw_response.brand_name') #>> '{}'
    )) AS nb_other_brand
  FROM advertising.ad_occurrence o2
  JOIN advertising.ad a2 ON a2.id = o2.ad_id
  WHERE o2.channel_name = c.channel_name
    AND o2.deleted_at IS NULL
    AND o2.occurrence_date BETWEEN c.start_date AND c.next_start_date
    AND (o2.occurrence_date, o2.id) > (c.start_date, c.occurrence_id)
    AND (o2.occurrence_date, o2.id) < (c.next_start_date, c.next_occurrence_id)
) e ON c.next_rn - c.rn > 1
-- the time not covered by the segments between stays under gap_max_sec
WHERE EXTRACT(EPOCH FROM c.next_start_date - c.end_date) - COALESCE(e.between_sec, 0) <= :gap_max_sec
ORDER BY c.channel_name, c.start_date, c.occurrence_id
""")


@dataclass(frozen=True)
class Pair:
    channel_name: str
    occurrence_id: str
    next_occurrence_id: str
    ad_id: str
    next_ad_id: str
    brand: str
    start_date: datetime
    end_date: datetime
    next_start_date: datetime
    next_end_date: datetime
    between_occurrence_ids: tuple[str, ...]
    between_sec: float
    nb_other_brand: int

    @property
    def gap_sec(self) -> float:
        return (self.next_start_date - self.end_date).total_seconds()

    @property
    def pair_type(self) -> PairType:
        if self.ad_id == self.next_ad_id:
            return PairType.SAME_AD
        if not self.between_occurrence_ids and abs(self.gap_sec) <= 1:
            return PairType.CONTIGUOUS
        if self.between_occurrence_ids and self.nb_other_brand == 0:
            return PairType.SHORT_UNBRANDED
        if self.nb_other_brand > 0:
            return PairType.SHORT_OTHER_BRAND
        if self.next_end_date <= self.end_date:
            return PairType.INCLUDED
        if self.gap_sec < -1:
            return PairType.OVERLAP
        return PairType.GAP


@dataclass
class SplitBroadcast:
    """One broadcast of a split ad: its pieces in broadcast order and the short segments between them."""

    channel_name: str
    piece_occurrence_ids: list[str]
    piece_ad_ids: list[str]
    absorbed_occurrence_ids: list[str] = field(default_factory=list)
    start_date: datetime | None = None
    end_date: datetime | None = None
    pair_types: list[PairType] = field(default_factory=list)

    @property
    def duration_sec(self) -> float:
        return (self.end_date - self.start_date).total_seconds()


def fetch_pairs(
    connection: Connection,
    date_start: datetime,
    date_end: datetime,
    short_sec: float,
    gap_max_sec: float,
) -> list[Pair]:
    rows = connection.execute(
        PAIRS_QUERY,
        {
            "date_start": date_start,
            "date_end": date_end,
            "short_sec": short_sec,
            "gap_max_sec": gap_max_sec,
        },
    ).mappings()
    return [
        Pair(
            **{
                **row,
                "between_occurrence_ids": tuple(row["between_occurrence_ids"]),
                "between_sec": float(row["between_sec"]),
            }
        )
        for row in rows
    ]


def build_sequences(pairs: list[Pair], date_end: datetime) -> list[SplitBroadcast]:
    """Chain the mergeable pairs sharing a piece (A-B and B-C -> A-B-C).

    A segment is the first piece of at most one pair and the second piece of at most one pair (next
    segment of the same brand), so the chains are linear. Only the chains starting before date_end are
    kept, the next window processes the others.
    """
    mergeable = [p for p in pairs if p.pair_type in MERGEABLE_PAIR_TYPES]
    by_first = {p.occurrence_id: p for p in mergeable}
    seconds = {p.next_occurrence_id for p in mergeable}
    sequences = []
    for pair in mergeable:
        if pair.occurrence_id in seconds:
            continue  # not the start of a chain
        sequence = SplitBroadcast(
            channel_name=pair.channel_name,
            piece_occurrence_ids=[pair.occurrence_id],
            piece_ad_ids=[pair.ad_id],
            start_date=pair.start_date,
            end_date=pair.end_date,
        )
        current = pair
        while current is not None:
            sequence.piece_occurrence_ids.append(current.next_occurrence_id)
            sequence.piece_ad_ids.append(current.next_ad_id)
            sequence.absorbed_occurrence_ids.extend(current.between_occurrence_ids)
            sequence.end_date = max(sequence.end_date, current.next_end_date)
            sequence.pair_types.append(current.pair_type)
            current = by_first.get(current.next_occurrence_id)
        if sequence.start_date < date_end:
            sequences.append(sequence)
    # a short segment between two pieces can itself be a piece of a pair of short segments of
    # another brand: the longest sequence wins, the segment is absorbed
    absorbed = {occ_id for s in sequences for occ_id in s.absorbed_occurrence_ids}
    sequences = [
        s for s in sequences if not absorbed.intersection(s.piece_occurrence_ids)
    ]
    logging.info(
        "%s pairs, %s mergeable, %s sequences",
        len(pairs),
        len(mergeable),
        len(sequences),
    )
    return sequences


def group_by_source_ads(
    sequences: list[SplitBroadcast],
) -> dict[tuple[str, ...], list[SplitBroadcast]]:
    """Sequences grouped by their ads in broadcast order: one merged ad per group."""
    groups: dict[tuple[str, ...], list[SplitBroadcast]] = {}
    for sequence in sequences:
        groups.setdefault(tuple(sequence.piece_ad_ids), []).append(sequence)
    return groups


def merged_duration_sec(sequences: list[SplitBroadcast]) -> float:
    """Duration of the merged ad: median duration of its broadcasts."""
    return statistics.median(s.duration_sec for s in sequences)
