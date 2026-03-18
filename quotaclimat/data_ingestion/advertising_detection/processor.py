import asyncio
import json
import logging
import os
from datetime import datetime
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

from .e00_partition_window import Segment, partition_week
from .e01_download_audio import AudioProcessor
from .e02_create_chunks import Chunk, ChunkCreator
from .e04_group_chunks import ChunkGrouping
from .tools.cache import LocalCache
from .tools.mediatree import CachedMediatreeAPI
from .tools.testimony_data.extract import get_testimony_data

logger = logging.getLogger(__name__)

api = CachedMediatreeAPI()


async def semaphore_wrap(semaphore: asyncio.Semaphore, coro):
    async with semaphore:
        return await coro


async def download_and_write_subtitle(occurences: list[dict], file_path: Path):
    if file_path.exists():
        return True

    for occ in occurences:
        subtitle = await api.get_subtitle(
            occ["channel"],
            occ["start_date"],
            occ["end_date"],
        )
        if subtitle:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(subtitle)
                # We try all the occurences until we get one, because we don't target the good ones for now
            return True
    return False


async def export_advertisings(advertisings: list[dict], path: Path):
    # This semaphore should be handled somewhere else
    semaphore = asyncio.Semaphore(5)
    tasks = []

    for i, ad in enumerate(advertisings):
        ad_path = path / str(i)
        ad_path.mkdir(parents=True, exist_ok=True)

        with open(ad_path / "description.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "count": ad["count"],
                    "occurences": [
                        {
                            "start_date": (
                                occ["start_date"]
                                .astimezone(ZoneInfo("Europe/Paris"))
                                .isoformat()
                            ),
                            "end_date": (
                                occ["end_date"]
                                .astimezone(ZoneInfo("Europe/Paris"))
                                .isoformat()
                            ),
                            "channel": occ["channel"],
                        }
                        for occ in ad["occurences"]
                    ],
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        occ = ad["occurences"][0]
        tasks.append(
            semaphore_wrap(
                semaphore,
                api.download_export(
                    occ["channel"],
                    occ["start_date"],
                    occ["end_date"],
                    "mp3",
                    ad_path / "version.mp3",
                ),
            )
        )
        tasks.append(
            semaphore_wrap(
                semaphore,
                api.download_export(
                    occ["channel"],
                    occ["start_date"],
                    occ["end_date"],
                    "mp4",
                    ad_path / "version.mp4",
                ),
            )
        )
        tasks.append(
            semaphore_wrap(
                semaphore,
                download_and_write_subtitle(ad["occurences"], ad_path / "subtitle.txt"),
            )
        )

    await asyncio.gather(*tasks)


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
    channel: str,
    start_date: str,
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
        max_ruptures=0,
        silence_percentile=5.0,
        cosine_weight=1.0,
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
        groups = chunk_grouping.run(chunks)

        group_cache.set(
            operation_name + ".json", json.dumps([group.to_dict() for group in groups])
        )

        advertisings = []
        for group in groups:
            if group.count > 5:
                advertisings.append(
                    {
                        "count": group.count,
                        "occurences": [
                            {
                                "start_date": datetime.fromtimestamp(occ.start_sec),
                                "end_date": datetime.fromtimestamp(occ.end_sec),
                                "channel": channel,
                            }
                            for occ in group.occurrences
                        ],
                    }
                )

        with LocalCache(
            name="advertising", version=params_hash_key + "-c5"
        ) as ads_cache:
            advertising_export_folder = (
                Path(".cache")
                / "advertisings"
                / (params_hash_key + "-c5")
                / (operation_name + ".json")
            )
            await export_advertisings(advertisings, advertising_export_folder)

            print(f"{len(advertisings)} potential advertising blocks detected:")
            print(advertising_export_folder.absolute())

        # parts = []
        # for segment in segments:
        #     chunks = json.loads(chunk_cache.get(segment.identifier + ".json"))

        #     media_url = "https://example.org"
        #     # media_url = await with_exponential_backoff(
        #     #     lambda: api.generate_src_url(
        #     #         channel=dl_segment.channel,
        #     #         from_date=dl_segment.start_date,
        #     #         to_date=dl_segment.end_date + timedelta(minutes=1),
        #     #         media_format="mp4",
        #     #     ),
        #     #     label=str(dl_segment),
        #     #     base_delay=10.0,
        #     #     max_retries=10,
        #     # )

        #     parts.append(
        #         {
        #             "start_date": dl_segment.start_date.timestamp(),
        #             "end_date": (dl_segment.end_date + timedelta(minutes=1)).timestamp(),
        #             "chunks": [d for d in chunks],
        #             "media_url": media_url,
        #         }
        #     )

        # generate_weekly_viewer(
        #     output_path="week_report.html",
        #     grouping=groups,
        #     parts=parts,
        #     annotations=annotations,
        #     params_summary={"operation_name": operation_name},
        # )

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
            channel=channel,
            start_date=start_date,
            segments=partition,
            annotations=annotations,
        )
    )
