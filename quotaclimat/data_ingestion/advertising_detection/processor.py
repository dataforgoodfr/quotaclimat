import asyncio
import json
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator

logger = logging.getLogger(__name__)

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
    SegmentCreator,
)
from quotaclimat.data_ingestion.advertising_detection.e03_group_segments import (
    SegmentGroupingPipeline,
)

###############################
#
# Processing functions:
#
# Process already downloaded files, delete them and the end.


def process_audio(processing_task: ProcessingTask) -> bool:
    """Returns True if processing was cached (skipped), False if actually processed."""
    cache_path = (
        Path(".cache")
        / "segments"
        / "abc"
        / processing_task.channel
        / processing_task.start_date.strftime("%Y-%m-%d_%H-%M-%S")
    )
    if cache_path.exists():
        return True
    else:
        segments = SegmentCreator().run(
            processing_task.audio_file_path, processing_task.start_date.timestamp()
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump([fp.to_dict() for fp in segments], f)
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
                was_cached = await loop.run_in_executor(executor, process_audio, task)
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
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    processing_task = await download_audio(task)
                    if processing_task.download_was_cached:
                        self.stats.dl_cached += 1
                    else:
                        self.stats.dl_downloaded += 1
                    await self.queue.put(processing_task)
                    last_exception = None
                    break
                except Exception:
                    last_exception = traceback.format_exc()
                    if attempt < max_retries:
                        delay = base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            "Download attempt %d/%d failed for %s, retrying in %.1fs",
                            attempt,
                            max_retries,
                            task,
                            delay,
                        )
                        await asyncio.sleep(delay)

            if last_exception is not None:
                self.stats.dl_errors += 1
                self.stats.errors.append(("download", str(task), last_exception))
                tqdm.write(
                    f"\n[ERROR] Download failed for {task} after {max_retries} attempts:\n{last_exception}"
                )

            self.dl_bar.update(1)
            self._update_postfix()


if __name__ == "__main__":
    import os

    channel = "tf1"
    start_date = "2025-05-05"
    partition = partition_week(
        channel=channel,
        start_date=start_date,
    )

    if True:
        new_workers = max(1, os.cpu_count() - 1)  # Laisser 1-2 CPUs libres pour l'OS

        asyncio.run(
            AudioProcessor(
                num_workers=new_workers,
                task_partition=partition,
            ).run()
        )

    else:
        segments_list = []
        for dl_task in partition:
            cache_path = (
                Path(".cache")
                / "segments"
                / "abc"
                / dl_task.channel
                / dl_task.start_date.strftime("%Y-%m-%d_%H-%M-%S")
            )
            if not cache_path.exists():
                raise RuntimeError(
                    f"Cache file not found for {dl_task}. Please run the full pipeline first."
                )
            else:
                with open(cache_path, "r", encoding="utf-8") as f:
                    segments = json.load(f)
                segments_list.push(segments)

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
            Path(".cache")
            / "grouping"
            / "abc"
            / channel
            / start_date.strftime("%Y-%m-%d_%H-%M-%S")
        )

        # Export JSON
        with open(groups_cache_path, "w", encoding="utf-8") as f:
            json.dump(groups, f, indent=2, ensure_ascii=False)
