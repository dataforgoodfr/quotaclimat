"""Recompute the audio chunks (fingerprints) of the ads already exported, with the current
fingerprinter parameters (identified by their hash, `fingerprinter.params_hash()`).

Ads that already have a chunk entry with the current hash are skipped (unless CLEAN_OTHER_CHUNKS
asks to remove their other entries). The ads are processed by decreasing number of occurrences,
so that the most broadcast ones get their chunks first, and MAX_ADS caps the number of ads taken
in one run.

Env vars:
- MAX_ADS: optional maximum number of ads to process in this run (e.g. 2000). Unset: all of them.
- CLEAN_OTHER_CHUNKS: "true" to keep only the chunk entry of the current hash on each ad.
- CURSOR_BATCH_SIZE: number of ads read and committed per batch (default 500).
"""

import asyncio
import logging
import os
import tempfile

import s3fs
from sentry_sdk.crons import monitor
from sqlalchemy import exists, func, select, update

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.processor import chunk_creator
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
    get_margin_from_detection_date,
)
from quotaclimat.data_ingestion.advertising.s02_exportation.run import get_s3_filesystem
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


def needs_processing(fingerprint_hash: str, clean_other_chunks: bool):
    """SQL condition: the ad has media and either has no chunk with this hash or, with
    clean_other_chunks, has other chunk entries to remove."""
    has_hash = has_chunk_with_hash(fingerprint_hash)
    # a soft deleted ad is no longer matched by the detection: no fingerprints to compute
    live = Ad.deleted_at.is_(None)
    if clean_other_chunks:
        # several entries of the current hash stay (fingerprints copied from an ad merged by hand)
        return (
            live
            & has_media()
            & (~has_hash | has_chunk_with_other_hash(fingerprint_hash))
        )
    return live & has_media() & ~has_hash


def ads_to_process_query(
    fingerprint_hash: str, clean_other_chunks: bool, limit: int | None
):
    """Ads to process, the most broadcast first (number of occurrences not deleted), then by id
    for a stable order."""
    nb_occurrences = (
        select(func.count())
        .where(Ad_Occurrence.ad_id == Ad.id, Ad_Occurrence.deleted_at.is_(None))
        .correlate(Ad)
        .scalar_subquery()
    )
    return (
        select(Ad)
        .where(needs_processing(fingerprint_hash, clean_other_chunks))
        .order_by(nb_occurrences.desc(), Ad.id)
        .limit(limit)
    )


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


async def run():
    fingerprint_hash = fingerprinter.params_hash()
    fs = get_s3_filesystem()

    counters = {
        "ads_seen": 0,
        "already_had_chunk": 0,
        "cleaned_other_chunks": 0,
        "download_failed": 0,
        "chunk_computed": 0,
    }

    with get_db_session() as read_session, get_db_session() as write_session:
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
            f"{counts['to_process']} to process"
            + (
                " (including ads to clean, CLEAN_OTHER_CHUNKS)"
                if CLEAN_OTHER_CHUNKS
                else ""
            )
            + f", {taken} taken in this run "
            + (f"(MAX_ADS={MAX_ADS})" if MAX_ADS is not None else "(no MAX_ADS)")
            + ", most occurrences first"
        )

        query = ads_to_process_query(fingerprint_hash, CLEAN_OTHER_CHUNKS, MAX_ADS)
        for ads in read_session.scalars(
            query.execution_options(yield_per=CURSOR_BATCH_SIZE)
        ).partitions():
            for ad in ads:
                counters["ads_seen"] += 1
                # every entry of the current hash: the one of the ad, and the ones copied from the ads
                # merged into it by hand (s04_consolidation/e02_manual_corrections, fusion)
                current_entries = [
                    chunk
                    for chunk in ad.chunks
                    if chunk.get("hash") == fingerprint_hash
                ]
                if current_entries:
                    counters["already_had_chunk"] += 1
                    if CLEAN_OTHER_CHUNKS and len(current_entries) < len(ad.chunks):
                        counters["cleaned_other_chunks"] += 1
                        write_session.execute(
                            update(Ad)
                            .where(Ad.id == ad.id)
                            .values(chunks=current_entries)
                        )
                else:
                    with tempfile.TemporaryDirectory() as dest_dir:
                        audio_file_path = await _download_audio_file(
                            fs, ad.id, dest_dir
                        )

                        if audio_file_path is None:
                            counters["download_failed"] += 1
                        else:
                            fingerprints = chunk_creator.run_on_audio_file(
                                audio_file_path=audio_file_path,
                                offset=(
                                    get_margin_from_detection_date(
                                        ad.first_detection_date
                                    ).total_seconds()
                                ),
                                duration=ad.duration_sec,
                            )
                            new_chunk_entry = Ad.generate_chunk_dict(
                                fingerprint_hash, fingerprints
                            )
                            counters["chunk_computed"] += 1

                            if CLEAN_OTHER_CHUNKS or len(ad.chunks) == 0:
                                write_session.execute(
                                    update(Ad)
                                    .where(Ad.id == ad.id)
                                    .values(chunks=[new_chunk_entry])
                                )
                            else:
                                write_session.execute(
                                    update(Ad)
                                    .where(Ad.id == ad.id)
                                    .values(chunks=[*ad.chunks, new_chunk_entry])
                                )

            write_session.commit()
            logger.info(f"Recompute saved chunks progress: {counters} / {taken}")

    logger.info(f"Recompute saved chunks finished: {counters}")


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-exportation"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        asyncio.run(run())
