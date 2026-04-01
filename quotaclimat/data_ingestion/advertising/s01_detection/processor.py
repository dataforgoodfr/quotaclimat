import json
import logging
import os
import time
from datetime import datetime
from functools import partial
from pathlib import Path

import boto3

from .e00_partition_window import Segment
from .e01_download_audio import AudioProcessor
from .e02_create_chunks import ChunkCreator
from .e03_already_identified_advertising import run_chunk_identification
from .e04_group_chunks import ChunkGrouping
from .e05_classify_fragments import FragmentsClassifier
from .e06_export_classification import database_storage_save
from .tools.cache import LocalCache
from .tools.common_objects import Chunk
from .tools.visualizer.weekly_viewer import generate_weekly_viewer

logger = logging.getLogger(__name__)

ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
REPORTS_S3_PREFIX = "reports"


def get_s3_client():
    return boto3.client(
        service_name="s3",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
    )


def upload_to_s3(local_path: Path, s3_key: str, s3_client):
    try:
        s3_client.upload_file(str(local_path), BUCKET_NAME, s3_key)
        logger.info(f"Uploaded s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to S3: {e}")


chunk_creator = ChunkCreator(
    sr=16000,
    hop_length=1024,
    n_mfcc=13,
    context_sec=1.0,
    novelty_smooth_sec=0.5,
    min_chunk_sec=1.0,
    silence_percentile=5.0,
    n_fft=2048,
    n_peaks=30,
    neighborhood=15,
    min_amplitude=0.01,
)
chunk_grouping = ChunkGrouping(
    duration_tol=1.0,  # C'est relativement haut, mais les autres filtres affinent bien. 1 = durée minimum d'un segment, pour que l'absorption ou non d'un micro segment ne soit pas discriminant
    rms_tol=0.1,
    centroid_tol=0.05,
    zcr_tol=0.1,
    similarity_threshold=0.05,  # C'est bas, mais les tol ci-dessus font un pré filtre très éfficace déjà
    min_matching_hashes=1,
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

    file_name = segment.identifier + ".json"

    if cache.exists(file_name):
        return True
    else:
        chunks = chunk_creator.run(segment, audio_file_path)
        cache.set(file_name, json.dumps([fp.to_dict() for fp in chunks]))
        return False


async def processor(
    report_folder: str | None,
    segments: list[Segment],
    annotations: list[dict] = [],
):
    timings: dict[str, float] = {}
    t_start = time.monotonic()

    #### Audio processing

    new_workers = max(1, os.cpu_count() - 1)  # Laisser 1-2 CPUs libres pour l'OS

    t0 = time.monotonic()
    chunk_hash = chunk_creator.params_hash()
    with LocalCache(name="chunks", version=chunk_hash) as chunk_cache:
        process_media = partial(
            process_audio, chunk_creator=chunk_creator, cache=chunk_cache
        )

        await AudioProcessor(
            num_workers=new_workers,
            segments=segments,
            process_media=process_media,
            max_concurrent_downloads=5,
            max_queue_size=10,
        ).run()

        chunks: list[Chunk] = []
        for segment in segments:
            chunk_batch = json.loads(chunk_cache.get(segment.identifier + ".json"))
            chunks.extend([Chunk.from_dict(d) for d in chunk_batch])

        # Sort by start time. Should already be the case, but ensure it.
        chunks.sort(key=lambda c: c.start_sec)
    timings["audio_processing"] = time.monotonic() - t0

    #### Identification of known chunks

    t0 = time.monotonic()
    previously_known_fragments, unknown_chunks = await run_chunk_identification(
        chunks,
        params_hash=chunk_hash,
        min_matching_hashes=chunk_grouping.min_matching_hashes,
        similarity_threshold=chunk_grouping.similarity_threshold,
    )
    timings["chunk_identification"] = time.monotonic() - t0

    #### Chunk grouping

    t0 = time.monotonic()

    groups = chunk_grouping.run(unknown_chunks)

    timings["chunk_grouping"] = time.monotonic() - t0

    #### Fragment classification

    t0 = time.monotonic()

    fragments = fragment_classifier.run(
        groups, already_known_fragments=previously_known_fragments
    )

    timings["fragment_classification"] = time.monotonic() - t0

    #### Database storage

    t0 = time.monotonic()
    database_storage_save(fragments, chunk_hash=chunk_hash)
    timings["database_storage"] = time.monotonic() - t0

    timings["total"] = time.monotonic() - t_start

    #### Results exportation

    reports_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{chunk_hash}"

    local_reports_path = Path(".cache") / "reports" / report_folder
    local_reports_path.mkdir(parents=True, exist_ok=True)

    html_report = generate_weekly_viewer(
        fragments=fragments,
        annotations=annotations,
        params_summary={
            "operation_name": report_folder,
            "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "chunk_creator": chunk_creator.params(),
            "chunk_grouping": chunk_grouping.params(),
            "fragment_classifier": fragment_classifier.params(),
        },
    )

    html_report_path = local_reports_path / f"{reports_name}.html"
    with open(html_report_path, "w", encoding="utf-8") as f:
        f.write(html_report)

    timing_lines = [
        f"Timing report for: {report_folder}",
        f"Generated: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        "",
    ]
    for step, duration in timings.items():
        timing_lines.append(f"  {step:<30} {duration:>8.2f}s")

    timing_report = "\n".join(timing_lines)

    text_report_path = local_reports_path / f"{reports_name}.txt"
    with open(text_report_path, "w", encoding="utf-8") as f:
        f.write(timing_report)

    print(f"""Reports generated:
        HTML: {html_report_path.absolute()}
        Text: {text_report_path.absolute()}
    """)

    if report_folder is not None:
        s3_client = get_s3_client()
        s3_folder = f"{REPORTS_S3_PREFIX}/{report_folder}"
        upload_to_s3(html_report_path, f"{s3_folder}/{reports_name}.html", s3_client)
        upload_to_s3(text_report_path, f"{s3_folder}/{reports_name}.txt", s3_client)

    return fragments
