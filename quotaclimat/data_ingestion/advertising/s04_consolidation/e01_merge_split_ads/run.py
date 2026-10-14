"""Stage 4 (consolidation), step 1: merge, without supervision, the ads split in several pieces by the
detection (see detect.py for the rule and the cases kept).

For each window of one day, from the oldest:
1. the broadcasts of split ads are detected (pieces of the same brand, separated by short segments);
2. one merged ad is created per sequence of source ads (tools/ad_merge.create_merged_ad): it has no
   prediction, the exportation and classification steps process it like a new ad;
3. each broadcast is replaced by one occurrence of the merged ad (tools/ad_merge.merge_occurrences),
   the occurrences of the pieces and of the short segments between them are soft deleted.
Everything is logged in advertising.ad_merge and advertising.ad_merge_occurrence.

Env: POSTGRES_*, AD_MERGE_START_DATE / AD_MERGE_END_DATE (ISO dates, UTC, default: the
AD_MERGE_DAYS last days, default 7), AD_MERGE_SHORT_SEC (default 2), AD_MERGE_GAP_MAX_SEC (default 3),
AD_MERGE_DRY_RUN (default true: only logs what would be merged).
"""

import logging
import os
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from quotaclimat.data_ingestion.advertising.s04_consolidation.e01_merge_split_ads.detect import (
    SplitBroadcast,
    build_sequences,
    fetch_pairs,
    group_by_source_ads,
    merged_duration_sec,
)
from quotaclimat.data_ingestion.advertising.tools.ad_merge import (
    MergeRevertedError,
    create_merged_ad,
    merge_occurrences,
)
from quotaclimat.utils.sentry import sentry_init

MERGE_METHOD = "auto_split_pieces"


def create_group_ad(
    session: Session,
    source_ad_ids: tuple[str, ...],
    broadcasts: list[SplitBroadcast],
    short_sec: float,
    gap_max_sec: float,
) -> str:
    """Create the merged ad of source_ad_ids, with the context of its detection. Returns its id."""
    return create_merged_ad(
        session,
        source_ad_ids,
        method=MERGE_METHOD,
        duration_sec=merged_duration_sec(broadcasts),
        details={
            "short_sec": short_sec,
            "gap_max_sec": gap_max_sec,
            "broadcasts_at_creation": len(broadcasts),
            "pair_types": dict(
                Counter(t.value for b in broadcasts for t in b.pair_types)
            ),
        },
    )


def process_window(
    engine,
    date_start: datetime,
    date_end: datetime,
    short_sec: float,
    gap_max_sec: float,
    dry_run: bool,
) -> int:
    with engine.connect() as connection:
        pairs = fetch_pairs(connection, date_start, date_end, short_sec, gap_max_sec)
    groups = group_by_source_ads(build_sequences(pairs, date_end))
    logging.info(
        "%s -> %s: %s split ads, %s broadcasts",
        date_start,
        date_end,
        len(groups),
        sum(len(b) for b in groups.values()),
    )
    if dry_run:
        for source_ad_ids, broadcasts in groups.items():
            logging.info(
                "Dry run: %s broadcasts of %s would be merged",
                len(broadcasts),
                " + ".join(source_ad_ids),
            )
        return 0

    merged = 0
    with Session(engine) as session:
        for source_ad_ids, broadcasts in groups.items():
            # savepoints: an inconsistent ad or broadcast does not stop the others
            try:
                with session.begin_nested():
                    merged_ad_id = create_group_ad(
                        session, source_ad_ids, broadcasts, short_sec, gap_max_sec
                    )
            except MergeRevertedError:
                logging.info("Merge of %s reverted by a human, skipped", source_ad_ids)
                continue
            except Exception:
                logging.exception(
                    "Creation of the merge of %s failed, skipped", source_ad_ids
                )
                continue
            for broadcast in broadcasts:
                try:
                    with session.begin_nested():
                        merge_occurrences(
                            session,
                            merged_ad_id,
                            broadcast.piece_occurrence_ids,
                            broadcast.absorbed_occurrence_ids,
                        )
                    merged += 1
                except Exception:
                    logging.exception(
                        "Merge of the occurrences %s failed, skipped",
                        broadcast.piece_occurrence_ids,
                    )
        session.commit()
    logging.info("%s broadcasts merged", merged)
    return merged


def run() -> int:
    days = int(os.environ.get("AD_MERGE_DAYS", "7"))
    today = datetime.now(UTC).replace(
        tzinfo=None, hour=0, minute=0, second=0, microsecond=0
    )
    date_end = datetime.fromisoformat(
        os.environ.get("AD_MERGE_END_DATE") or (today + timedelta(days=1)).isoformat()
    )
    date_start = datetime.fromisoformat(
        os.environ.get("AD_MERGE_START_DATE")
        or (date_end - timedelta(days=days)).isoformat()
    )
    short_sec = float(os.environ.get("AD_MERGE_SHORT_SEC", "2"))
    gap_max_sec = float(os.environ.get("AD_MERGE_GAP_MAX_SEC", "3"))
    dry_run = os.environ.get("AD_MERGE_DRY_RUN", "true").lower() == "true"
    logging.info(
        "Merge of the split ads from %s to %s (short segments < %ss, gap <= %ss, dry run: %s)",
        date_start,
        date_end,
        short_sec,
        gap_max_sec,
        dry_run,
    )

    engine = connect_to_db(use_custom_json_serializer=True)
    merged = 0
    try:
        window_start = date_start
        while window_start < date_end:
            window_end = min(window_start + timedelta(days=1), date_end)
            merged += process_window(
                engine, window_start, window_end, short_sec, gap_max_sec, dry_run
            )
            window_start = window_end
    finally:
        engine.dispose()
    return merged


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"), stream=sys.stdout)
    sentry_init()
    run()
