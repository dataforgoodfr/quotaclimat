import asyncio
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Generator

from tqdm import tqdm

from .e00_partition_window import Segment
from .tools.mediatree import MediatreeAPI

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
        segments: Generator[Segment, None, None],
        process_media: Callable[[Segment, str], bool],
        num_workers: int = 4,
        max_concurrent_downloads: int = 5,
        max_queue_size: int = 10,
    ):
        self.num_workers = num_workers
        self.queue = asyncio.Queue(maxsize=max_queue_size)

        self.segments = list(segments)
        self.total = len(self.segments)

        self.stats = PipelineStats()
        self.process_media = process_media

        # Semaphore and retry are now handled inside MediatreeAPI
        self.api = MediatreeAPI(max_concurrent_requests=max_concurrent_downloads)

    async def run(self):
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
            async with self.api:
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
            self._download_and_queue(segment)
            for segment in self.segments
        ]

        await asyncio.gather(*segments)

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

    async def _download_and_queue(self, segment: Segment):
        try:
            audio_file_path, was_cached = await download_audio(self.api, segment)

            if was_cached:
                self.stats.dl_cached += 1
            else:
                self.stats.dl_downloaded += 1

            await self.queue.put((segment, audio_file_path))
        except Exception:
            tb = traceback.format_exc()
            self.stats.dl_errors += 1
            self.stats.errors.append(("download", str(segment), tb))
            tqdm.write(
                f"\n[ERROR] Download failed for {segment}:\n{tb}"
            )

        self.dl_bar.update(1)
        self._update_postfix()
