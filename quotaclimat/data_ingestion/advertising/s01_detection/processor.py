import json
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from zoneinfo import ZoneInfo

from ..tools.fingerprint_tools.compare import FingerprintsCompare
from ..tools.fingerprints import fingerprinter
from ..tools.interactive_tqdm import interactive_tqdm
from ..tools.mediatree.bucket_mediatree import (
    download_days_audio_parts,
    get_s3_filesystem,
)
from ..tools.segments import Segment
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
    start_date: datetime,
    end_date: datetime,
    operation_name: str,
    report_folder: str | None,
    partition: list[Segment],
    annotations: list[dict] = [],
    num_workers: int = 1,
):
    timings = TimingCollector()

    fingerprint_hash = fingerprinter.params_hash()
    logger.info(f"Process is run with fingerprint_hash={fingerprint_hash}")

    #### Download all weeks audio segments

    with timings.measure("audio_download"):
        audio_files = await download_days_audio_parts(
            fs=get_s3_filesystem(),
            channel=channel,
            # list all days between start_date and end_date
            days=[
                start_date.date() + timedelta(days=i)
                for i in range((end_date.date() - start_date.date()).days)
            ],
            dest_dir="./.cache/mediatree",
            max_concurrent_downloads=10,
        )
        logger.info(
            f"Downloaded {len(audio_files)} audio files for channel {channel} between {start_date} and {end_date}"
        )

        # audio file names look like: franceinfotv_2026-09-07T04-36-00Z_2026-09-07T04-38-00Z.mp3
        filename_dt_format = "%Y-%m-%dT%H-%M-%SZ"
        audio_segments = [
            (
                Segment(
                    start_date=datetime.strptime(
                        f.split("/")[-1].split(".")[0].split("_")[1],
                        filename_dt_format,
                    ).replace(tzinfo=ZoneInfo("UTC")),
                    end_date=datetime.strptime(
                        f.split("/")[-1].split(".")[0].split("_")[2],
                        filename_dt_format,
                    ).replace(tzinfo=ZoneInfo("UTC")),
                    channel=channel,
                ),
                f,
            )
            for f in audio_files
        ]

    #### Audio processing

    with timings.measure("audio_processing"):
        with LocalCache(name="chunks", version=fingerprint_hash) as chunk_cache:
            progress = interactive_tqdm(
                total=len(audio_segments), desc="Processing audio segments"
            )

            worker = partial(
                process_audio, cache=chunk_cache, chunk_creator=chunk_creator
            )
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                for was_cached in executor.map(
                    worker,
                    (segment for segment, _ in audio_segments),
                    (audio_file_path for _, audio_file_path in audio_segments),
                ):
                    progress.count("cached" if was_cached else "computed")
                    progress.update(1)

            progress.close()
            logger.info(f"Audio processing: {progress.counts}")

            chunks: list[Chunk] = [
                chunk
                for segment, _ in audio_segments
                for chunk in (
                    Chunk.from_dict(d)
                    for d in json.loads(chunk_cache.get(segment.identifier + ".json"))
                )
            ]

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
        clean_pre_existing_detections([segment for segment, _ in audio_segments])

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
