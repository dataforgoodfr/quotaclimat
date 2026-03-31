import json
import logging
import os
from datetime import datetime
from functools import partial
from pathlib import Path

from .e00_partition_window import Segment
from .e01_download_audio import AudioProcessor
from .e02_create_chunks import ChunkCreator
from .e03_already_identified_advertising import run_chunk_identification
from .e04_group_chunks import ChunkGroup, ChunkGrouping
from .e05_classify_fragments import FragmentsClassifier
from .e06_export_classification import database_storage_save
from .tools.cache import LocalCache
from .tools.common_objects import Chunk, Fragment
from .tools.visualizer.weekly_viewer import generate_weekly_viewer

logger = logging.getLogger(__name__)

chunk_creator = ChunkCreator(
    sr=22050,
    hop_length=512,
    n_mfcc=20,
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
    operation_name: str,
    segments: list[Segment],
    annotations: list[dict] = [],
):
    #### Audio processing

    new_workers = max(1, os.cpu_count() - 1)  # Laisser 1-2 CPUs libres pour l'OS

    chunk_hash = chunk_creator.params_hash()
    params_hash_key = chunk_hash
    with LocalCache(name="chunks", version=params_hash_key) as chunk_cache:
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

    #### Identification of known chunks

    previously_known_chunks, unkown_chunks = await run_chunk_identification(
        chunks,
        params_hash=chunk_hash,
        min_matching_hashes=chunk_grouping.min_matching_hashes,
        similarity_threshold=chunk_grouping.similarity_threshold,
    )

    #### Chunk grouping

    params_hash_key += "-" + chunk_grouping.params_hash()
    with LocalCache(name="grouping", version=params_hash_key) as group_cache:
        group_cache_file = operation_name + ".json"
        if group_cache.exists(group_cache_file):
            groups_data = json.loads(group_cache.get(group_cache_file))

            groups = [ChunkGroup.from_dict(d) for d in groups_data]

        else:
            groups = chunk_grouping.run(unkown_chunks)

            group_cache.set(
                operation_name + ".json",
                json.dumps([group.to_dict() for group in groups]),
            )

    #### Fragment classification

    params_hash_key += "-" + fragment_classifier.params_hash()
    with LocalCache(name="fragments", version=params_hash_key) as fragments_cache:
        fragments_cache_file = operation_name + ".json"
        if fragments_cache.exists(fragments_cache_file):
            fragments_data = json.loads(fragments_cache.get(fragments_cache_file))

            fragments = [Fragment.from_dict(d) for d in fragments_data]
        else:
            fragments = fragment_classifier.run(
                groups, already_known_chunks=previously_known_chunks
            )

            fragments_cache.set(
                operation_name + ".json",
                json.dumps([fragment.to_dict() for fragment in fragments], default=str),
            )

    #### Database storage

    database_storage_save(
        fragments, chunk_hash=chunk_hash, already_known_chunks=previously_known_chunks
    )

    #### Results exportation

    reports_path = Path(".cache") / "reports" / operation_name
    reports_path.mkdir(parents=True, exist_ok=True)

    html_report = generate_weekly_viewer(
        fragments=fragments,
        annotations=annotations,
        params_summary={
            "operation_name": operation_name,
            "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "chunk_creator": chunk_creator.params(),
            "chunk_grouping": chunk_grouping.params(),
            "fragment_classifier": fragment_classifier.params(),
        },
    )

    html_report_path = reports_path / f"{params_hash_key}.html"
    with open(html_report_path, "w", encoding="utf-8") as f:
        f.write(html_report)

    print(f"""Reports generated:
        HTML: {html_report_path.absolute()}
    """)

    return groups
