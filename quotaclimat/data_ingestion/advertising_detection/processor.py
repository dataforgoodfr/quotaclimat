import asyncio
import json
import logging
import os
from functools import partial
from pathlib import Path

from .e00_partition_window import Segment, partition_week
from .e01_download_audio import AudioProcessor
from .e02_create_chunks import Chunk, ChunkCreator
from .e04_group_chunks import ChunkGrouping
from .e05_classify_fragments import FragmentsClassifier
from .tools.cache import LocalCache
from .tools.testimony_data.extract import get_testimony_data
from .tools.visualizer.weekly_viewer import generate_weekly_viewer

logger = logging.getLogger(__name__)


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
    new_workers = max(1, os.cpu_count() - 1)  # Laisser 1-2 CPUs libres pour l'OS

    chunk_creator = ChunkCreator(
        sr=22050,
        hop_length=512,
        n_mfcc=20,
        context_sec=1.0,
        novelty_smooth_sec=0.5,
        min_chunk_sec=5.0,
        sensitivity=0.25,
        silence_percentile=5.0,
        n_fft=2048,
        n_peaks=30,
        neighborhood=15,
        min_amplitude=0.01,
    )
    chunk_grouping = ChunkGrouping(
        similarity_threshold=0.05,
        sr=22050,
        min_matching_hashes=1,
        n_peaks_by_chunk=5,
        neighborhood_peaks_filter=15,
        min_peak_amplitude=0.01,
    )
    fragment_classifier = FragmentsClassifier(
        repetition_threshold=5,
    )

    params_hash_key = chunk_creator.params_hash()
    with LocalCache(name="chunks", version=params_hash_key) as chunk_cache:
        process_media = partial(
            process_audio, chunk_creator=chunk_creator, cache=chunk_cache
        )

        await AudioProcessor(
            num_workers=new_workers,
            segment_segment=segments,
            process_media=process_media,
            max_concurrent_downloads=5,
            max_queue_size=10,
        ).run()

        chunks: list[Chunk] = []
        for segment in segments:
            chunk_batch = json.loads(chunk_cache.get(segment.identifier + ".json"))
            chunks.extend([Chunk.from_dict(d) for d in chunk_batch])

    params_hash_key += "-" + chunk_grouping.params_hash()
    with LocalCache(name="grouping", version=params_hash_key) as group_cache:
        group_cache_file = operation_name + ".json"
        if group_cache.exists(group_cache_file):
            groups_data = json.loads(group_cache.get(group_cache_file))

            groups = [ChunkGrouping.ChunkGroup.from_dict(d) for d in groups_data]

        else:
            groups = chunk_grouping.run(chunks)

            group_cache.set(
                operation_name + ".json",
                json.dumps([group.to_dict() for group in groups]),
            )

    params_hash_key += "-" + fragment_classifier.params_hash()
    with LocalCache(name="fragments", version=params_hash_key) as fragments_cache:
        fragments_cache_file = operation_name + ".json"
        if fragments_cache.exists(fragments_cache_file):
            fragments_data = json.loads(fragments_cache.get(fragments_cache_file))

            fragments = [
                FragmentsClassifier.Fragment.from_dict(d) for d in fragments_data
            ]
        else:
            fragments = fragment_classifier.run(groups)

            fragments_cache.set(
                operation_name + ".json",
                json.dumps([fragment.to_dict() for fragment in fragments], default=str),
            )

    html = generate_weekly_viewer(
        fragments=fragments,
        annotations=annotations,
        params_summary={
            "operation_name": operation_name,
            "chunk_creator": chunk_creator.params(),
            "chunk_grouping": chunk_grouping.params(),
            "fragment_classifier": fragment_classifier.params(),
        },
    )
    output_path = Path(".cache") / "reports" / f"{params_hash_key}.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return groups


if __name__ == "__main__":
    channel = "tf1"
    TESTIMONY_CHANNEL = "TF1"
    start_date = "2025-05-05"

    partition = list(
        partition_week(
            channel=channel,
            start_date=start_date,
        )
    )

    annotations = get_testimony_data(
        channel=TESTIMONY_CHANNEL,
        from_date=partition[0].start_date,
        to_date=partition[-1].end_date,
        source_file="export.csv",
    )

    asyncio.run(
        processor(
            operation_name=f"{channel}-full_week-{start_date}",
            segments=partition,
            annotations=annotations,
        )
    )
