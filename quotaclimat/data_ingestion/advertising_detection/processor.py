import asyncio
import json
import logging
import os
import traceback
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Callable, Generator
from zoneinfo import ZoneInfo

from tqdm import tqdm

from quotaclimat.data_ingestion.advertising_detection.e00_download_audio_files.download_partition import (
    ProcessingTask,
    download_audio,
)
from quotaclimat.data_ingestion.advertising_detection.e00_download_audio_files.partition_window import (
    DownloadTask,
    partition_week,
)
from quotaclimat.data_ingestion.advertising_detection.e02_create_segments import (
    Segment,
    SegmentCreator,
)
from quotaclimat.data_ingestion.advertising_detection.e03_group_segments import (
    SegmentGroupingPipeline,
)
from quotaclimat.data_ingestion.advertising_detection.tools.cache import (
    LocalCache,
)
from quotaclimat.data_ingestion.advertising_detection.tools.mediatree import (
    CachedMediatreeAPI,
)
from quotaclimat.data_ingestion.advertising_detection.tools.testimony_data.extract import (
    get_testimony_data,
)
from quotaclimat.data_ingestion.advertising_detection.tools.visualizer.weekly_viewer import (
    generate_weekly_viewer,
)

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
# Processing functions:
#
# Process already downloaded files, delete them and the end.


def process_audio(processing_task: ProcessingTask, cache: LocalCache) -> bool:
    """Returns True if processing was cached (skipped), False if actually processed."""
    file_name = (
        processing_task.channel
        + "/"
        + processing_task.start_date.strftime("%Y-%m-%d_%H-%M-%S")
        + ".json"
    )
    if cache.exists(file_name):
        return True
    else:
        segments = SegmentCreator().run(
            processing_task.audio_file_path, processing_task.start_date.timestamp()
        )
        cache.set(file_name, json.dumps([fp.to_dict() for fp in segments]))
        return False


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
            for i, (stage, task, err) in enumerate(self.errors, 1):
                lines.append(f"    [{i}] {stage} | {task} | {err}")
        lines.append("=" * 60)
        return "\n".join(lines)


###############################
#
# Orchestration functions:
#
# Launch download workers in async and processing workers in parallel


class AudioProcessor:
    def __init__(
        self,
        task_partition: Generator[
            DownloadTask, None, None
        ],  # Function to generate download tasks based on input parameters
        process_media: Callable[[ProcessingTask], bool],
        num_workers: int = 4,  # Number of processor for audio work (CPU intensive)
        max_concurrent_downloads: int = 5,  # Limit of simultaneous downloads (I/O intensive, API limits)
        max_queue_size: int = 10,  # Maximum queue size between download and processing (Memory intensive: all pending files are saved locally)
    ):
        self.num_workers = num_workers
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.queue = asyncio.Queue(maxsize=max_queue_size)

        # Materialize the generator to know the total count for progress bars
        self.tasks = list(task_partition)
        self.total = len(self.tasks)

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
        tasks = [
            self._download_and_queue(download_task) for download_task in self.tasks
        ]

        # Wait for all to finish
        await asyncio.gather(*tasks)

        # End signals
        for _ in range(self.num_workers):
            await self.queue.put(None)

    async def _process_worker(self, executor: ProcessPoolExecutor, worker_id: int):
        while True:
            task: ProcessingTask = await self.queue.get()
            if task is None:
                break

            try:
                loop = asyncio.get_event_loop()
                was_cached = await loop.run_in_executor(
                    executor, self.process_media, task
                )
                if was_cached:
                    self.stats.proc_cached += 1
                else:
                    self.stats.proc_processed += 1
            except Exception:
                self.stats.proc_errors += 1
                tb = traceback.format_exc()
                self.stats.errors.append(("process", str(task), tb))
                tqdm.write(f"\n[ERROR] Worker {worker_id} failed on {task}:\n{tb}")

            self.proc_bar.update(1)
            self._update_postfix()

    async def _download_and_queue(
        self, task: DownloadTask, max_retries: int = 3, base_delay: float = 2.0
    ):
        """All-in-one: download with retry AND queue"""
        async with self.semaphore:
            try:
                processing_task = await with_exponential_backoff(
                    lambda: download_audio(task),
                    max_retries=max_retries,
                    base_delay=base_delay,
                    label=str(task),
                )
                if processing_task.download_was_cached:
                    self.stats.dl_cached += 1
                else:
                    self.stats.dl_downloaded += 1
                await self.queue.put(processing_task)
            except Exception:
                tb = traceback.format_exc()
                self.stats.dl_errors += 1
                self.stats.errors.append(("download", str(task), tb))
                tqdm.write(
                    f"\n[ERROR] Download failed for {task} after {max_retries} attempts:\n{tb}"
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


async def processor(
    channel: str,
    start_date: str,
    partition: list[DownloadTask],
    annotations: list[dict] = [],
):
    new_workers = max(1, os.cpu_count() - 1)  # Laisser 1-2 CPUs libres pour l'OS
    cache_key = "tests2"

    with LocalCache(name="segments", version=cache_key) as segment_cache:
        process_media = partial(process_audio, cache=segment_cache)

        await AudioProcessor(
            num_workers=new_workers,
            task_partition=partition,
            process_media=process_media,
        ).run()

        segments_list = []
        for dl_task in partition:
            cache_path = (
                Path(".cache")
                / "segments"
                / cache_key
                / dl_task.channel
                / (dl_task.start_date.strftime("%Y-%m-%d_%H-%M-%S") + ".json")
            )
            print(cache_path)
            if not cache_path.exists():
                raise RuntimeError(
                    f"Cache file not found for {dl_task}. Please run the full pipeline first."
                )
            else:
                with open(cache_path, "r", encoding="utf-8") as f:
                    segments = json.load(f)
                segments_list.append([Segment.from_dict(d) for d in segments])

        pipeline = SegmentGroupingPipeline(
            similarity_threshold=0.05,
            sr=22050,
            min_matching_hashes=1,
            n_peaks_by_segment=5,
            neighborhood_peaks_filter=15,
            min_peak_amplitude=0.01,
        )

        groups = pipeline.run(segments_list)
        groups_cache_path = (
            Path(".cache") / "grouping" / cache_key / channel / start_date
        )

        # Export JSON
        groups_cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(groups_cache_path, "w", encoding="utf-8") as f:
            json.dump(groups, f, indent=2, ensure_ascii=False)

        groups_cache_path = (
            Path(".cache") / "grouping" / cache_key / channel / start_date
        )
        with open(groups_cache_path, "r", encoding="utf-8") as f:
            groups = json.load(f)

        parts = []
        for dl_task in tqdm(partition, desc="Processing partition"):
            cache_path = (
                Path(".cache")
                / "segments"
                / cache_key
                / dl_task.channel
                / (dl_task.start_date.strftime("%Y-%m-%d_%H-%M-%S") + ".json")
            )
            if not cache_path.exists():
                raise RuntimeError(
                    f"Cache file not found for {dl_task}. Please run the full pipeline first."
                )
            else:
                with open(cache_path, "r", encoding="utf-8") as f:
                    segments = json.load(f)

                media_url = "https://example.org"
                # media_url = await with_exponential_backoff(
                #     lambda: api.generate_src_url(
                #         channel=dl_task.channel,
                #         from_date=dl_task.start_date,
                #         to_date=dl_task.end_date + timedelta(minutes=1),
                #         media_format="mp4",
                #     ),
                #     label=str(dl_task),
                #     base_delay=10.0,
                #     max_retries=10,
                # )

                parts.append(
                    {
                        "start_date": dl_task.start_date.timestamp(),
                        "end_date": (
                            dl_task.end_date + timedelta(minutes=1)
                        ).timestamp(),
                        "segments": [d for d in segments],
                        "media_url": media_url,
                    }
                )

        generate_weekly_viewer(
            output_path="week_report.html",
            grouping=groups,
            parts=parts,
            annotations=annotations,
            params_summary={"channel": channel, "start_date": start_date},
        )

        groups_cache_path = (
            Path(".cache") / "grouping" / cache_key / channel / start_date
        )
        with open(groups_cache_path, "r", encoding="utf-8") as f:
            groups = json.load(f)

        advertisings = []
        for group in groups:
            if group["count"] > 5:
                advertisings.append(
                    {
                        "count": group["count"],
                        "occurences": [
                            {
                                "start_date": datetime.fromtimestamp(occ["start_sec"]),
                                "end_date": datetime.fromtimestamp(occ["end_sec"]),
                                "channel": channel,
                            }
                            for occ in group["occurrences"]
                        ],
                    }
                )

        advertising_export_folder = (
            Path(".cache") / "advertisings" / (cache_key + "-c5") / channel / start_date
        )
        await export_advertisings(advertisings, advertising_export_folder)

        print(f"{len(advertisings)} potential advertising blocks detected:")
        print(advertising_export_folder.absolute())

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

    asyncio.run(
        processor(
            channel=channel,
            start_date=start_date,
            partition=partition,
            annotations=get_testimony_data(
                channel=TESTIMONY_CHANNEL,
                from_date=partition[0].start_date,
                to_date=partition[-1].end_date,
                source_file="export.csv",
            ),
        )
    )
