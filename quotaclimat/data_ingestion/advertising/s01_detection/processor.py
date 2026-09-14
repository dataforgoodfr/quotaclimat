import json
import logging
import os
from datetime import datetime
from functools import partial

from ..tools.fingerprint_tools.compare import FingerprintsCompare
from ..tools.fingerprints import fingerprinter
from .e00_partition_window import Segment
from .e01_download_audio import AudioProcessor
from .e02_create_chunks import ChunkCreator
from .e03_already_identified_advertising import run_chunk_identification
from .e04_group_chunks import group_chunks
from .e05_classify_fragments import FragmentsClassifier
from .e06_export_classification import (
    clean_pre_existing_detections,
    database_storage_save,
)
from .e07_export_raw_data import Report, TimingCollector, export_chunks_to_s3
from .tools.cache import LocalCache
from .tools.common_objects import Chunk

logger = logging.getLogger(__name__)


chunk_creator = ChunkCreator(
    fingerprinter=fingerprinter,
    min_chunk_sec=1.0,
    silence_percentile=5.0,
)
fingerprints_compare = FingerprintsCompare(
    min_matching_pairs=10,
    similarity_threshold=0.05,  # C'est bas, mais les tol ci-dessous font un pré filtre très éfficace déjà
    freq_tol=2,  # ~15.6 Hz per bin tolerance
    dt_tol=1,  # ~64 ms per frame tolerance
    offset_tol=2,  # ~128 ms temporal coherence tolerance
    duration_tol=1.0,  # C'est relativement haut, mais les autres filtres affinent bien. 1 = durée minimum d'un segment, pour que l'absorption ou non d'un micro segment ne soit pas discriminant
    rms_tol=0.1,
    centroid_tol=0.05,
    zcr_tol=0.1,
)


def process_audio(
    segment: Segment,
    audio_file_path: str,
    cache: LocalCache,
    chunk_creator: ChunkCreator,
) -> bool:
    """Returns True if processing was cached (skipped), False if actually processed."""
    file_name = segment.identifier + ".json"

    if cache.exists(file_name):
        return True
    else:
        chunks = chunk_creator.run(segment, audio_file_path)
        cache.set(file_name, json.dumps([c.to_dict() for c in chunks]))
        return False


async def processor(
    channel: str,
    operation_name: str,
    report_folder: str | None,
    segments: list[Segment],
    annotations: list[dict] = [],
    num_workers: int = 1,
):
    timings = TimingCollector()

    fingerprint_hash = fingerprinter.params_hash()
    logger.info(f"Process is run with fingerprint_hash={fingerprint_hash}")

    #### Audio processing

    with timings.measure("audio_processing"):
        with LocalCache(name="chunks", version=fingerprint_hash) as chunk_cache:
            process_media = partial(
                process_audio, chunk_creator=chunk_creator, cache=chunk_cache
            )

            await AudioProcessor(
                num_workers=num_workers,
                segments=segments,
                process_media=process_media,
                max_concurrent_downloads=5,
                max_queue_size=10,
                delete_files_after_processing=(
                    os.environ.get("OPTIMIZE_MEMORY", "true").lower() == "true"
                ),
            ).run()

            chunks: list[Chunk] = []
            for segment in segments:
                try:
                    chunk_batch = json.loads(
                        chunk_cache.get(segment.identifier + ".json")
                    )
                    chunks.extend([Chunk.from_dict(d) for d in chunk_batch])
                except:
                    logger.error(f"Could not get content of {segment.identifier}")
                    raise

            # Sort by start time. Should already be the case, but ensure it.
            chunks.sort(key=lambda c: c.start_sec)

    #### Identification of known chunks

    with timings.measure("chunk_identification"):
        previously_known_fragments, unknown_chunks = await run_chunk_identification(
            chunks,
            params_hash=fingerprint_hash,
            compare=fingerprints_compare,
        )

    #### Chunk grouping

    with timings.measure("chunk_grouping"):
        groups = group_chunks(unknown_chunks, compare=fingerprints_compare)

    #### Fragment classification

    with timings.measure("fragment_classification"):
        fragment_classifier = FragmentsClassifier.from_channel(channel)
        fragments = fragment_classifier.run(
            groups, already_known_fragments=previously_known_fragments
        )

    #### Database storage

    with timings.measure("clean_pre_existing_occurrences"):
        clean_pre_existing_detections(segments)

    with timings.measure("database_storage"):
        database_storage_save(fragments, fingerprint_hash=fingerprint_hash)

    #### Results exportation

    with LocalCache(name="reports", version=fingerprint_hash) as reports_cache:
        reports = Report(
            reports_name=f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{channel}_{operation_name}",
            params={
                "channel": channel,
                "operation_name": operation_name,
                "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "chunk_creator": chunk_creator.params(),
                "fingerprints_compare": fingerprints_compare.params(),
                "fragment_classifier": fragment_classifier.params(),
            },
            local_path=reports_cache.cache_folder,
        )
        reports.generate(
            fragments=fragments,
            annotations=annotations,
            timings=timings,
        )

        print(f"""Reports generated:
            HTML: {reports.html_report_path.absolute()}
            Text: {reports.text_report_path.absolute()}
        """)

        if report_folder:
            reports.save_to_s3(report_folder)
            export_chunks_to_s3(chunks, report_folder)

    return fragments
