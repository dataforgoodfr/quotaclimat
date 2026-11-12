"""Recompute the audio chunks (fingerprints) of the ads already exported, with the current
fingerprinter parameters (identified by their hash, `fingerprinter.params_hash()`).

Ads that already have a chunk entry with the current hash are skipped (unless CLEAN_OTHER_CHUNKS
asks to remove their other entries). The ads are processed by decreasing number of occurrences,
so that the most broadcast ones get their chunks first, and MAX_ADS caps the number of ads taken
in one run. Merged ads are not downloaded: at the end of the run, an active merge gets the
concatenation of the chunks of its source ads (see concatenate_merges and the README).

Env vars:
- MAX_ADS: optional maximum number of ads to process in this run (e.g. 2000). Unset: all of them.
- CLEAN_OTHER_CHUNKS: "true" to keep only the chunk entry of the current hash on each ad.
- CURSOR_BATCH_SIZE: number of ads read and committed per batch (default 500).
- AUDIO_WORKERS: number of processes computing the fingerprints (default: CPUs of the
  Scaleway job, from SCW_SLS_CPU, minus one, at least 1).
- DOWNLOAD_CONCURRENCY: number of audio files downloaded at once (default 16). Each batch
  downloads its audio files concurrently and computes them in the worker processes.
"""

import asyncio
import logging
import multiprocessing
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import s3fs
from sentry_sdk.crons import monitor
from sqlalchemy import exists, func, select, update

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Merge, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.processor import chunk_creator
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
    get_margin_from_detection_date,
)
from quotaclimat.data_ingestion.advertising.s02_exportation.run import get_s3_filesystem
from quotaclimat.data_ingestion.advertising.tools.ad_merge import MERGE_STATUS_ACTIVE
from quotaclimat.data_ingestion.advertising.tools.fingerprints import fingerprinter
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "false").strip().lower() in ("1", "true", "yes")


def _env_int(name: str) -> int | None:
    value = os.environ.get(name, "").strip()
    return int(value) if value else None


CLEAN_OTHER_CHUNKS = _env_bool("CLEAN_OTHER_CHUNKS")
CURSOR_BATCH_SIZE = _env_int("CURSOR_BATCH_SIZE") or 500
MAX_ADS = _env_int("MAX_ADS")
# Downloads wait on S3 far more than on the CPU (the fingerprints of a 5s ad take ~25ms):
# many of them run at once whatever the number of workers
DOWNLOAD_CONCURRENCY = _env_int("DOWNLOAD_CONCURRENCY") or 16
MERGES_BATCH_SIZE = 500
# Marks the chunk entry of a merged ad built by concatenating the entries of its source ads
FROM_SOURCES_KEY = "from_sources"


def has_chunk_with_hash(fingerprint_hash: str):
    """SQL condition: the ad has a chunk entry computed with this hash (`chunks` is a JSON list
    of {"hash": ..., "fingerprints": [...]})."""
    chunk = func.json_array_elements(Ad.chunks).table_valued("value").alias("chunk")
    return exists(
        select(1)
        .select_from(chunk)
        .where(chunk.c.value.op("->>")("hash") == fingerprint_hash)
    )


def has_chunk_with_other_hash(fingerprint_hash: str):
    """SQL condition: the ad has a chunk entry computed with another hash."""
    chunk = func.json_array_elements(Ad.chunks).table_valued("value").alias("chunk")
    return exists(
        select(1)
        .select_from(chunk)
        .where(chunk.c.value.op("->>")("hash").is_distinct_from(fingerprint_hash))
    )


# Ads without media (fragment_type set to "no_data" by the classification): nothing to download
NO_DATA_FRAGMENT_TYPE = "no_data"


def has_media():
    """SQL condition: the ad is not marked as having no media (fragment_type "no_data")."""
    return Ad.fragment_type.is_distinct_from(NO_DATA_FRAGMENT_TYPE)


def is_merged_ad():
    """SQL condition: the ad was built by merging other ads (advertising.ad_merge), whatever the
    status of the merge. Its audio is never cut in chunks: an active merge gets the
    concatenation of the chunks of its source ads (concatenate_merges), a reverted one keeps
    the empty chunks set by revert_merge, so that the detection no longer matches it."""
    return exists(select(1).where(Ad_Merge.merged_ad_id == Ad.id))


def is_source_of_active_merge():
    """SQL condition: the ad is one of the source ads of an active merge (advertising.ad_merge)."""
    return exists(
        select(1).where(
            Ad_Merge.status == MERGE_STATUS_ACTIVE,
            Ad_Merge.source_ad_ids.any(Ad.id),
        )
    )


def needs_processing(fingerprint_hash: str, clean_other_chunks: bool):
    """SQL condition: the ad has media, is not a merged ad, and either has no chunk with this
    hash or, with clean_other_chunks, has other chunk entries to remove."""
    has_hash = has_chunk_with_hash(fingerprint_hash)
    # a soft deleted ad (merged by hand into another) is no longer matched by the detection: no
    # fingerprints to compute, unless it is a source of an active merge, whose concatenated
    # fingerprints need its own ones (the merge rule wins over the soft deletion)
    live = Ad.deleted_at.is_(None) | is_source_of_active_merge()
    if clean_other_chunks:
        # several entries of the current hash stay (fingerprints copied from an ad merged by hand)
        to_do = ~has_hash | has_chunk_with_other_hash(fingerprint_hash)
    else:
        to_do = ~has_hash
    return live & has_media() & ~is_merged_ad() & to_do


def _live_occurrences():
    """Number of occurrences not deleted of each ad."""
    return (
        select(Ad_Occurrence.ad_id, func.count().label("nb"))
        .where(Ad_Occurrence.deleted_at.is_(None))
        .group_by(Ad_Occurrence.ad_id)
        .subquery()
    )


def _occurrences_through_merges(own):
    """Number of occurrences not deleted of the active merged ads each ad is a source of. The
    occurrences of a source ad are moved to the merged ad when it is merged: without them, the
    sources of the most broadcast merges would come last."""
    sources = (
        select(
            func.unnest(Ad_Merge.source_ad_ids).label("ad_id"),
            Ad_Merge.merged_ad_id,
        )
        .where(Ad_Merge.status == MERGE_STATUS_ACTIVE)
        .subquery()
    )
    return (
        select(sources.c.ad_id, func.sum(own.c.nb).label("nb"))
        .join(own, own.c.ad_id == sources.c.merged_ad_id)
        .group_by(sources.c.ad_id)
        .subquery()
    )


def ads_to_process_query(
    fingerprint_hash: str, clean_other_chunks: bool, limit: int | None
):
    """Ads to process, the most broadcast first, then by id for a stable order. An ad counts its
    own occurrences (not deleted) plus those of the active merged ads it is a source of."""
    own = _live_occurrences()
    through_merges = _occurrences_through_merges(own)
    priority = func.coalesce(own.c.nb, 0) + func.coalesce(through_merges.c.nb, 0)
    return (
        select(Ad)
        .outerjoin(own, own.c.ad_id == Ad.id)
        .outerjoin(through_merges, through_merges.c.ad_id == Ad.id)
        .where(needs_processing(fingerprint_hash, clean_other_chunks))
        .order_by(priority.desc(), Ad.id)
        .limit(limit)
    )


def _has_concatenated_chunk(fingerprint_hash: str):
    """SQL condition: the ad has a chunk entry with this hash built by concatenate_merges."""
    chunk = func.json_array_elements(Ad.chunks).table_valued("value").alias("chunk")
    return exists(
        select(1)
        .select_from(chunk)
        .where(
            chunk.c.value.op("->>")("hash") == fingerprint_hash,
            chunk.c.value.op("->>")(FROM_SOURCES_KEY) == "true",
        )
    )


def merges_to_concatenate_query(fingerprint_hash: str):
    """Active merges whose merged ad has no concatenated chunk entry with this hash yet."""
    return (
        select(Ad_Merge.merged_ad_id, Ad_Merge.source_ad_ids)
        .join(Ad, Ad.id == Ad_Merge.merged_ad_id)
        .where(
            Ad_Merge.status == MERGE_STATUS_ACTIVE,
            # a merged ad merged by hand into another one is no longer matched by the detection
            Ad.deleted_at.is_(None),
            ~_has_concatenated_chunk(fingerprint_hash),
        )
        .order_by(Ad_Merge.merged_ad_id)
    )


# Marks the chunk entries copied from an ad merged by hand into this one
# (s04_consolidation/e02_manual_corrections, fusion)
COPIED_FROM_KEY = "from_ad_id"


def _own_entry_with_hash(chunks: list | None, fingerprint_hash: str) -> dict | None:
    """The ad's own entry with this hash: the ones copied by a manual fusion hold the audio of
    another ad. Falls back to a copied entry when the ad has no own one."""
    entries = [e for e in (chunks or []) if e.get("hash") == fingerprint_hash]
    own = [e for e in entries if not e.get(COPIED_FROM_KEY)]
    return (own or entries or [None])[0]


def concatenated_chunks(
    merged_chunks: list | None,
    source_chunks: list[list | None],
    fingerprint_hash: str,
    clean_other_chunks: bool,
) -> list | None:
    """New chunks of a merged ad: its own entry with this hash replaced by the fingerprints of its
    source ads one after the other (in broadcast order, as tools/ad_merge.concatenate_chunks).
    The entries copied from ads merged into it by hand are kept. None while a source ad has no
    entry with this hash yet."""
    source_entries = [_own_entry_with_hash(c, fingerprint_hash) for c in source_chunks]
    if any(entry is None for entry in source_entries):
        return None
    new_entry = {
        "hash": fingerprint_hash,
        "fingerprints": [
            fingerprint
            for entry in source_entries
            for fingerprint in entry.get("fingerprints", [])
        ],
        FROM_SOURCES_KEY: True,
    }
    kept = [
        entry
        for entry in (merged_chunks or [])
        if (
            entry.get(COPIED_FROM_KEY)
            if entry.get("hash") == fingerprint_hash
            else not clean_other_chunks
        )
    ]
    return [*kept, new_entry]


def concatenate_merges(
    read_session, write_session, fingerprint_hash: str, clean_other_chunks: bool
) -> dict:
    """Give every active merged ad the concatenation of the chunks of its source ads, once all
    of them have a chunk entry with this hash. Database only (no download), so it is done for
    every merge ready, whatever MAX_ADS."""
    counters = {"merges_concatenated": 0, "merges_waiting_for_sources": 0}
    merges = read_session.execute(merges_to_concatenate_query(fingerprint_hash)).all()
    for start in range(0, len(merges), MERGES_BATCH_SIZE):
        batch = merges[start : start + MERGES_BATCH_SIZE]
        ad_ids = {m.merged_ad_id for m in batch} | {
            ad_id for m in batch for ad_id in m.source_ad_ids
        }
        chunks_by_id = dict(
            read_session.execute(
                select(Ad.id, Ad.chunks).where(Ad.id.in_(ad_ids))
            ).all()
        )
        for merge in batch:
            new_chunks = concatenated_chunks(
                chunks_by_id.get(merge.merged_ad_id),
                [chunks_by_id.get(ad_id) for ad_id in merge.source_ad_ids],
                fingerprint_hash,
                clean_other_chunks,
            )
            if new_chunks is None:
                counters["merges_waiting_for_sources"] += 1
                continue
            counters["merges_concatenated"] += 1
            write_session.execute(
                update(Ad).where(Ad.id == merge.merged_ad_id).values(chunks=new_chunks)
            )
        write_session.commit()
    return counters


def count_ads(session, fingerprint_hash: str, clean_other_chunks: bool) -> dict:
    """Numbers logged at the start of the job."""
    has_hash = has_chunk_with_hash(fingerprint_hash)
    return {
        "total": session.scalar(select(func.count()).select_from(Ad)),
        "already_had_chunk": session.scalar(
            select(func.count()).select_from(Ad).where(has_hash)
        ),
        "no_data": session.scalar(
            select(func.count()).select_from(Ad).where(~has_media())
        ),
        "merged": session.scalar(
            select(func.count()).select_from(Ad).where(is_merged_ad())
        ),
        "merges_to_concatenate": session.scalar(
            select(func.count()).select_from(
                merges_to_concatenate_query(fingerprint_hash).subquery()
            )
        ),
        "to_process": session.scalar(
            select(func.count())
            .select_from(Ad)
            .where(needs_processing(fingerprint_hash, clean_other_chunks))
        ),
    }


async def _download_audio_file(
    fs: s3fs.S3FileSystem, ad_id: str, dest_dir: str
) -> str | None:
    s3_key = ad_media_s3_key(ad_id, "mp3")
    audio_file_path = os.path.join(dest_dir, ad_id + ".mp3")

    try:
        await fs._get_file(s3_key, audio_file_path)
        return audio_file_path
    except Exception as e:
        logger.error(f"Error downloading {s3_key} from S3: {e}")
        return None


def compute_fingerprints(
    audio_file_path: str,
    first_detection_date: datetime,
    duration_sec: float,
) -> tuple[list[dict] | None, str | None]:
    """(fingerprints as dicts, None), or (None, error) when the file cannot be decoded
    (e.g. an mp3 of a single frame, which libsndfile refuses to open): one bad file must not
    stop the whole run. Runs in a worker process, whose logging is not configured: takes and
    returns plain values only, the caller logs the error."""
    try:
        fingerprints = chunk_creator.run_on_audio_file(
            audio_file_path=audio_file_path,
            offset=get_margin_from_detection_date(first_detection_date).total_seconds(),
            duration=duration_sec,
        )
        return [f.to_dict() for f in fingerprints], None
    except Exception as e:
        return None, repr(e)


def number_of_workers() -> int:
    """AUDIO_WORKERS, or the CPUs of the Scaleway job (SCW_SLS_CPU, in mVCPU) minus one for
    the main process, at least 1."""
    workers = _env_int("AUDIO_WORKERS")
    if workers:
        return workers
    mvcpu = _env_int("SCW_SLS_CPU")
    return max(1, (mvcpu or 0) // 1000 - 1)


async def _download_and_compute(
    ad: Ad,
    fs: s3fs.S3FileSystem,
    dest_dir: str,
    executor: ProcessPoolExecutor,
    downloads: asyncio.Semaphore,
) -> tuple[str, list[dict] | None]:
    """("download_failed", None), ("decode_failed", None) or ("ok", fingerprints)."""
    async with downloads:
        audio_file_path = await _download_audio_file(fs, ad.id, dest_dir)
    if audio_file_path is None:
        return "download_failed", None
    try:
        fingerprints, error = await asyncio.get_running_loop().run_in_executor(
            executor,
            compute_fingerprints,
            audio_file_path,
            ad.first_detection_date,
            ad.duration_sec,
        )
    finally:
        os.remove(audio_file_path)
    if error is not None:
        logger.error(f"Error computing the fingerprints of ad {ad.id}: {error}")
        return "decode_failed", None
    return "ok", fingerprints


async def run():
    fingerprint_hash = fingerprinter.params_hash()
    fs = get_s3_filesystem()
    workers = number_of_workers()
    downloads = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    counters = {
        "ads_seen": 0,
        "already_had_chunk": 0,
        "cleaned_other_chunks": 0,
        "download_failed": 0,
        "decode_failed": 0,
        "chunk_computed": 0,
    }

    with (
        get_db_session() as read_session,
        get_db_session() as write_session,
        # spawn rather than fork: s3fs runs its own event loop thread in this process
        ProcessPoolExecutor(
            max_workers=workers, mp_context=multiprocessing.get_context("spawn")
        ) as executor,
    ):
        counts = count_ads(read_session, fingerprint_hash, CLEAN_OTHER_CHUNKS)
        taken = (
            counts["to_process"]
            if MAX_ADS is None
            else min(MAX_ADS, counts["to_process"])
        )
        logger.info(
            f"Recompute saved chunks with fingerprint hash {fingerprint_hash}: "
            f"{counts['total']} ads in total, "
            f"{counts['already_had_chunk']} already have a chunk with this hash, "
            f"{counts['no_data']} skipped as fragment_type no_data (no media), "
            f"{counts['merged']} merged ads not downloaded "
            f"({counts['merges_to_concatenate']} active merges to concatenate at the end), "
            f"{counts['to_process']} to process"
            + (
                " (including ads to clean, CLEAN_OTHER_CHUNKS)"
                if CLEAN_OTHER_CHUNKS
                else ""
            )
            + f", {taken} taken in this run "
            + (f"(MAX_ADS={MAX_ADS})" if MAX_ADS is not None else "(no MAX_ADS)")
            + f", most occurrences first (with those of their merges), {workers} workers, "
            f"{DOWNLOAD_CONCURRENCY} concurrent downloads"
        )

        query = ads_to_process_query(fingerprint_hash, CLEAN_OTHER_CHUNKS, MAX_ADS)
        for ads in read_session.scalars(
            query.execution_options(yield_per=CURSOR_BATCH_SIZE)
        ).partitions():
            to_compute = []
            for ad in ads:
                counters["ads_seen"] += 1
                # every entry of the current hash: the one of the ad, and the ones copied from the ads
                # merged into it by hand (s04_consolidation/e02_manual_corrections, fusion)
                current_entries = [
                    chunk
                    for chunk in ad.chunks
                    if chunk.get("hash") == fingerprint_hash
                ]
                if not current_entries:
                    to_compute.append(ad)
                    continue
                counters["already_had_chunk"] += 1
                if CLEAN_OTHER_CHUNKS and len(current_entries) < len(ad.chunks):
                    counters["cleaned_other_chunks"] += 1
                    write_session.execute(
                        update(Ad).where(Ad.id == ad.id).values(chunks=current_entries)
                    )

            with tempfile.TemporaryDirectory() as dest_dir:
                results = await asyncio.gather(
                    *(
                        _download_and_compute(ad, fs, dest_dir, executor, downloads)
                        for ad in to_compute
                    )
                )

            for ad, (status, fingerprints) in zip(to_compute, results):
                if status != "ok":
                    counters[status] += 1
                    continue
                counters["chunk_computed"] += 1
                new_chunk_entry = {
                    "hash": fingerprint_hash,
                    "fingerprints": fingerprints,
                }
                write_session.execute(
                    update(Ad)
                    .where(Ad.id == ad.id)
                    .values(
                        chunks=(
                            [new_chunk_entry]
                            if CLEAN_OTHER_CHUNKS or len(ad.chunks) == 0
                            else [*ad.chunks, new_chunk_entry]
                        )
                    )
                )

            write_session.commit()
            logger.info(f"Recompute saved chunks progress: {counters} / {taken}")

        counters.update(
            concatenate_merges(
                read_session, write_session, fingerprint_hash, CLEAN_OTHER_CHUNKS
            )
        )

    logger.info(f"Recompute saved chunks finished: {counters}")


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-exportation"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        asyncio.run(run())
