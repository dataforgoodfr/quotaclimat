import asyncio
import logging
import os
import traceback
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Generator

from tqdm import tqdm

from .e00_partition_window import Segment
from .tools.mediatree import CachedMediatreeAPI

logger = logging.getLogger(__name__)

api = CachedMediatreeAPI()


async def download_audio(segment: Segment) -> (str, bool):
    # Check if file already exists before download to detect cache hits
    expected_path = os.path.join(
        api.export_folder,
        api._file_name(
            segment.channel,
            segment.start_date,
            segment.end_date + timedelta(minutes=1),
            "mp3",
        ),
    )
    was_cached = os.path.isfile(expected_path)

    audio_file_path = await api.download_export(
        segment.channel,
        segment.start_date,
        segment.end_date + timedelta(minutes=1),
        "mp3",
    )  # on ajoute une minute pour être sûr de couvrir toute la période

    return audio_file_path, was_cached


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
