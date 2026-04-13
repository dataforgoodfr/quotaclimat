import json
import logging
import os
from datetime import datetime
from functools import partial

from .e00_partition_window import Segment
from .e01_download_audio import AudioProcessor
from .e02_create_chunks import ChunkCreator
from .e03_already_identified_advertising import run_chunk_identification
from .e04_group_chunks import ChunkGrouping
from .e05_classify_fragments import FragmentsClassifier
from .e06_export_classification import database_storage_save
from .e07_export_raw_data import Report, TimingCollector
from .tools.cache import LocalCache
from .tools.common_objects import Chunk

logger = logging.getLogger(__name__)


# --- Signal-based pipeline (default, existing) ---

chunk_creator = ChunkCreator(
    sr=16000,
    hop_length=1024,
    n_mfcc=13,
    context_sec=1.0,
    novelty_smooth_sec=0.5,
    min_chunk_sec=1.0,
    silence_percentile=5.0,
    n_fft=2048,
    n_peaks=20,
    neighborhood=15,
    min_amplitude=0.01,
    max_pairs=30,
)
chunk_grouping = ChunkGrouping(
    duration_tol=1.0,  # C'est relativement haut, mais les autres filtres affinent bien. 1 = durée minimum d'un segment, pour que l'absorption ou non d'un micro segment ne soit pas discriminant
    rms_tol=0.1,
    centroid_tol=0.05,
    zcr_tol=0.1,
    similarity_threshold=0.05,  # C'est bas, mais les tol ci-dessus font un pré filtre très éfficace déjà
    min_matching_pairs=10,
    freq_tol=2,  # ~15.6 Hz per bin tolerance
    dt_tol=1,  # ~64 ms per frame tolerance
    offset_tol=2,  # ~128 ms temporal coherence tolerance
)
fragment_classifier = FragmentsClassifier(
    repetition_threshold=3,
)


def process_audio(
    segment: Segment,
    audio_file_path: str,
    cache: LocalCache,
    chunk_creator: ChunkCreator,
) -> bool:
    """Returns True if processing was cached (skipped), False if actually processed."""
    logger.info(f"Processing audio {segment.identifier}")

    file_name = segment.identifier + ".json"

    if cache.exists(file_name):
        return True
    else:
        chunks = chunk_creator.run(segment, audio_file_path)
        cache.set(file_name, json.dumps([fp.to_dict() for fp in chunks]))
        return False


async def processor(
    operation_name: str,
    report_folder: str | None,
    segments: list[Segment],
    annotations: list[dict] = [],
    num_workers: int = 1,
):
    """Original signal-based pipeline (e02 → e03 → e04 → e05 → e06 → e07)."""
    timings = TimingCollector()

    chunk_hash = chunk_creator.params_hash()
    logger.info(f"Process is run with chunk_hash={chunk_hash}")

    #### Audio processing

    with timings.measure("audio_processing"):
        with LocalCache(name="chunks", version=chunk_hash) as chunk_cache:
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
                chunk_batch = json.loads(chunk_cache.get(segment.identifier + ".json"))
                chunks.extend([Chunk.from_dict(d) for d in chunk_batch])

            # Sort by start time. Should already be the case, but ensure it.
            chunks.sort(key=lambda c: c.start_sec)

    #### Identification of known chunks

    with timings.measure("chunk_identification"):
        previously_known_fragments, unknown_chunks = await run_chunk_identification(
            chunks,
            params_hash=chunk_hash,
            min_matching_pairs=chunk_grouping.min_matching_pairs,
            similarity_threshold=chunk_grouping.similarity_threshold,
            freq_tol=chunk_grouping.freq_tol,
            dt_tol=chunk_grouping.dt_tol,
            offset_tol=chunk_grouping.offset_tol,
        )

    #### Chunk grouping

    with timings.measure("chunk_grouping"):
        groups = chunk_grouping.run(unknown_chunks)

    #### Fragment classification

    with timings.measure("fragment_classification"):
        fragments = fragment_classifier.run(
            groups, already_known_fragments=previously_known_fragments
        )

    #### Database storage

    with timings.measure("database_storage"):
        database_storage_save(fragments, chunk_hash=chunk_hash)

    #### Results exportation

    with LocalCache(name="reports", version=chunk_hash) as reports_cache:
        reports = Report(
            operation_name=operation_name,
            chunk_hash=chunk_hash,
            params={
                "operation_name": operation_name,
                "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "chunk_creator": chunk_creator.params(),
                "chunk_grouping": chunk_grouping.params(),
                "fragment_classifier": fragment_classifier.params(),
            },
            local_path=reports_cache.cache_folder
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

    return fragments
