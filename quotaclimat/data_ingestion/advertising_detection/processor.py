import asyncio
import json
import logging
import os
import traceback
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Callable, Generator
from zoneinfo import ZoneInfo

from tqdm import tqdm

from .e00_partition_window import Segment, partition_week
from .e01_download_audio import download_audio
from .e02_create_chunks import Chunk, ChunkCreator
from .e03_group_chunks import ChunkGrouping
from .tools.cache import LocalCache
from .tools.mediatree import CachedMediatreeAPI
from .tools.testimony_data.extract import get_testimony_data

logger = logging.getLogger(__name__)

api = CachedMediatreeAPI()


async def with_exponential_backoff(
    coro_fn, max_retries: int = 3, base_delay: float = 2.0, label: str = ""
):
    """Retry an async callable with exponential backoff. Raises on final failure."""
    for attempt in range(1, max_retries + 1):
        try:
            return await coro_fn()
        except Exception as exc:
            if attempt == max_retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %d/%d failed%s, retrying in %.1fs: %s",
                attempt,
                max_retries,
                f" for {label}" if label else "",
                delay,
                exc,
            )
            await asyncio.sleep(delay)


###############################
#
# Stats tracking
#


@dataclass
class PipelineStats:
    dl_downloaded: int = 0
    dl_cached: int = 0
    dl_errors: int = 0
    proc_processed: int = 0
    proc_cached: int = 0
    proc_errors: int = 0
    errors: list = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            "",
            "=" * 60,
            "Pipeline finished",
            "=" * 60,
            f"  Downloads:  {self.dl_downloaded} downloaded, {self.dl_cached} cached, {self.dl_errors} errors",
            f"  Processing: {self.proc_processed} processed, {self.proc_cached} cached, {self.proc_errors} errors",
        ]
        if self.errors:
            lines.append(f"\n  {len(self.errors)} error(s):")
            for i, (stage, segment, err) in enumerate(self.errors, 1):
                lines.append(f"    [{i}] {stage} | {segment} | {err}")
        lines.append("=" * 60)
        return "\n".join(lines)


###############################
#
# Orchestration functions:
#
# Launch download workers in async and processing workers in parallel


@dataclass
class ProcessingTask:
    audio_file_path: str
    download_segment: Segment


class AudioProcessor:
    def __init__(
        self,
        segment_segment: Generator[
            Segment, None, None
        ],  # Function to generate download segments based on input parameters
        process_media: Callable[[Segment, str], bool],
        num_workers: int = 4,  # Number of processor for audio work (CPU intensive)
        max_concurrent_downloads: int = 5,  # Limit of simultaneous downloads (I/O intensive, API limits)
        max_queue_size: int = 10,  # Maximum queue size between download and processing (Memory intensive: all pending files are saved locally)
    ):
        self.num_workers = num_workers
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.queue = asyncio.Queue(maxsize=max_queue_size)

        # Materialize the generator to know the total count for progress bars
        self.segments = list(segment_segment)
        self.total = len(self.segments)

        self.stats = PipelineStats()

        self.process_media = process_media

    async def run(self):
        # Progress bars: download on top, processing below
        self.dl_bar = tqdm(
            total=self.total,
            desc="Download",
            unit="file",
            position=0,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
        )
        self.proc_bar = tqdm(
            total=self.total,
            desc="Process ",
            unit="file",
            position=1,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
        )
        self._update_postfix()

        try:
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                download = asyncio.create_task(self._download_worker())
                workers = [
                    asyncio.create_task(self._process_worker(executor, i))
                    for i in range(self.num_workers)
                ]

                await download
                await asyncio.gather(*workers)
        finally:
            self.dl_bar.close()
            self.proc_bar.close()

        tqdm.write(self.stats.summary())

        if self.stats.errors:
            raise RuntimeError(
                f"Pipeline completed with {len(self.stats.errors)} error(s). "
                "See summary above for details."
            )

    def _update_postfix(self):
        self.dl_bar.set_postfix_str(
            f"cached={self.stats.dl_cached} err={self.stats.dl_errors} queue={self.queue.qsize()}",
            refresh=False,
        )
        self.proc_bar.set_postfix_str(
            f"cached={self.stats.proc_cached} err={self.stats.proc_errors}",
            refresh=False,
        )

    async def _download_worker(self):
        """Launch downloads and queue them as they complete"""
        segments = [
            self._download_and_queue(download_segment)
            for download_segment in self.segments
        ]

        # Wait for all to finish
        await asyncio.gather(*segments)

        # End signals
        for _ in range(self.num_workers):
            await self.queue.put(None)

    async def _process_worker(self, executor: ProcessPoolExecutor, worker_id: int):
        while True:
            next_item = await self.queue.get()
            if next_item is None:
                break

            segment: Segment = next_item[0]
            audio_file_path: str = next_item[1]

            try:
                loop = asyncio.get_event_loop()
                was_cached = await loop.run_in_executor(
                    executor, self.process_media, segment, audio_file_path
                )
                if was_cached:
                    self.stats.proc_cached += 1
                else:
                    self.stats.proc_processed += 1
            except Exception:
                self.stats.proc_errors += 1
                tb = traceback.format_exc()
                self.stats.errors.append(("process", str(segment), tb))
                tqdm.write(f"\n[ERROR] Worker {worker_id} failed on {segment}:\n{tb}")

            self.proc_bar.update(1)
            self._update_postfix()

    async def _download_and_queue(
        self, segment: Segment, max_retries: int = 3, base_delay: float = 2.0
    ):
        """All-in-one: download with retry AND queue"""
        async with self.semaphore:
            try:
                audio_file_path, download_was_cached = await with_exponential_backoff(
                    lambda: download_audio(segment),
                    max_retries=max_retries,
                    base_delay=base_delay,
                    label=str(segment),
                )

                if download_was_cached:
                    self.stats.dl_cached += 1
                else:
                    self.stats.dl_downloaded += 1

                await self.queue.put((segment, audio_file_path))
            except Exception:
                tb = traceback.format_exc()
                self.stats.dl_errors += 1
                self.stats.errors.append(("download", str(segment), tb))
                tqdm.write(
                    f"\n[ERROR] Download failed for {segment} after {max_retries} attempts:\n{tb}"
                )

            self.dl_bar.update(1)
            self._update_postfix()


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
