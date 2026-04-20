import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Generator

from tqdm import tqdm

from .e00_partition_window import Segment
from .tools.mediatree import MediatreeAPI

# When running in a production stack where logs are collected, tqdm's cursor
# movement codes (\r, ANSI escapes) make all updates appear on a single line.
# Set TQDM_LOG_MODE=1 to replace progress bars with plain logger.info() lines.
_LOG_MODE = os.environ.get("TQDM_LOG_MODE", "0") == "1" or not sys.stdout.isatty()

logger = logging.getLogger(__name__)


async def download_audio(api: MediatreeAPI, segment: Segment) -> tuple[str, bool]:
    end_date = segment.end_date + timedelta(minutes=1)
    was_cached = api.export_exists(segment.channel, segment.start_date, end_date, "mp3")

    audio_file_path = await api.download_export(
        segment.channel,
        segment.start_date,
        end_date,
        "mp3",
    )

    return audio_file_path, was_cached


###############################
#
# Stats tracking
#


@dataclass
class PipelineStats:
    dl_downloaded: int = 0
    dl_cached: int = 0
    proc_processed: int = 0
    proc_cached: int = 0

    def summary(self) -> str:
        lines = [
            "",
            "=" * 60,
            "Audio pipeline finished",
            "=" * 60,
            f"  Downloads:  {self.dl_downloaded} downloaded, {self.dl_cached} cached",
            f"  Processing: {self.proc_processed} processed, {self.proc_cached} cached",
            "=" * 60,
        ]
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
        segments: Generator[Segment, None, None],
        process_media: Callable[[Segment, str], bool],
        num_workers: int = 4,
        max_concurrent_downloads: int = 5,
        max_queue_size: int = 10,
        delete_files_after_processing: bool = True,
    ):
        self.num_workers = num_workers
        self.max_concurrent_downloads = max_concurrent_downloads
        self.queue = asyncio.Queue(maxsize=max_queue_size)

        self.segments = list(segments)
        self.total = len(self.segments)

        self.stats = PipelineStats()
        self.process_media = process_media

        # Semaphore and retry are handled inside MediatreeAPI
        self.api = MediatreeAPI(max_concurrent_requests=max_concurrent_downloads)
        self.delete_files_after_processing = delete_files_after_processing

    async def run(self):
        self.dl_bar = tqdm(
            total=self.total,
            desc="Download",
            unit="file",
            position=0,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
            disable=_LOG_MODE,
        )
        self.proc_bar = tqdm(
            total=self.total,
            desc="Process ",
            unit="file",
            position=1,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
            disable=_LOG_MODE,
        )
        self._update_postfix()

        try:
            async with self.api:
                # Use threads instead of processes: the heavy CPU work
                # (librosa, numpy, scipy) releases the GIL so threads
                # give real parallelism. ProcessPoolExecutor fails in
                # Docker/Scaleway because child processes re-import the
                # entire module tree (including mediatree secrets, DB
                # models, etc.) which crashes or deadlocks silently.
                with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._download_worker())
                        for i in range(self.num_workers):
                            tg.create_task(self._process_worker(executor, i))
        finally:
            self.dl_bar.close()
            self.proc_bar.close()

        if _LOG_MODE:
            logger.info(self.stats.summary())
        else:
            tqdm.write(self.stats.summary())

    def _update_postfix(self):
        self.dl_bar.set_postfix_str(
            f"cached={self.stats.dl_cached} queue={self.queue.qsize()}",
            refresh=False,
        )
        self.proc_bar.set_postfix_str(
            f"cached={self.stats.proc_cached}",
            refresh=False,
        )

    async def _download_worker(self):
        """Download segments with backpressure from the processing queue.

        The inflight semaphore wraps the entire download+enqueue as one unit.
        A new download only starts once a previous one has been enqueued,
        so when the queue is full, downloads pause instead of piling up files on disk.
        Max files on disk ≈ max_concurrent_downloads + max_queue_size + num_workers.
        """
        inflight = asyncio.Semaphore(self.max_concurrent_downloads)

        async def _bounded_download(segment: Segment):
            async with inflight:
                await self._download_and_queue(segment)

        async with asyncio.TaskGroup() as tg:
            for segment in self.segments:
                tg.create_task(_bounded_download(segment))

        for _ in range(self.num_workers):
            await self.queue.put(None)

    async def _process_worker(self, executor: ThreadPoolExecutor, worker_id: int):
        logger.debug(f"Start processing of worker {worker_id}")
        while True:
            next_item = await self.queue.get()
            if next_item is None:
                break

            segment: Segment = next_item[0]
            audio_file_path: str = next_item[1]

            logger.debug(
                f"Start processing audio segment from {segment.start_date} to {segment.end_date}"
            )

            loop = asyncio.get_event_loop()
            was_cached = await loop.run_in_executor(
                executor, self.process_media, segment, audio_file_path
            )

            if was_cached:
                self.stats.proc_cached += 1
            else:
                self.stats.proc_processed += 1

            if self.delete_files_after_processing:
                os.remove(audio_file_path)

            self.proc_bar.update(1)
            self._update_postfix()
            if _LOG_MODE:
                logger.info(
                    "Process %d/%d (processed=%d cached=%d)",
                    self.stats.proc_processed + self.stats.proc_cached,
                    self.total,
                    self.stats.proc_processed,
                    self.stats.proc_cached,
                )

    async def _download_and_queue(self, segment: Segment):
        audio_file_path, was_cached = await download_audio(self.api, segment)

        if was_cached:
            self.stats.dl_cached += 1
        else:
            self.stats.dl_downloaded += 1

        await self.queue.put((segment, audio_file_path))
        self.dl_bar.update(1)
        self._update_postfix()
        if _LOG_MODE:
            logger.info(
                "Download %d/%d (downloaded=%d cached=%d queue=%d)",
                self.stats.dl_downloaded + self.stats.dl_cached,
                self.total,
                self.stats.dl_downloaded,
                self.stats.dl_cached,
                self.queue.qsize(),
            )
