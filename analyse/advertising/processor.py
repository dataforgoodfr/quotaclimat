import asyncio
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Generator

import librosa

###############################
#
# Processing functions:
#
# Process already downloaded files, delete them and the end.


@dataclass
class ProcessingTask:
    audio_url: str
    start_sec: float
    end_sec: float
    channel: str


def process_audio(processing_task: ProcessingTask):
    print(f"Processing {processing_task}...")
    return
    y, sr = librosa.load(processing_task.audio_url, sr=16000)
    return librosa.feature.mfcc(y=y, sr=sr)


###############################
#
# Download functions:
#
# Defines the different steps, download audio file and queue them for processing.


@dataclass
class DownloadTask:
    start_sec: float
    end_sec: float
    channel: str


async def download_audio(task: DownloadTask) -> ProcessingTask:
    print(f"Downloading {task}...")
    return ProcessingTask(
        audio_url=f"/tmp/{task.channel}_{task.start_sec}_{task.end_sec}.wav",
        start_sec=task.start_sec,
        end_sec=task.end_sec,
        channel=task.channel,
    )


###############################
#
# Download task generation:
#
# Generate the list of download tasks to be processed, based on the input parameters (week, channel, etc.)


@dataclass
class ProcessorInput:
    start_date: str  # Start of the analyzed week, format iso 2026-12-31
    channel: str


def generate_download_tasks(
    input: ProcessorInput,
) -> Generator[DownloadTask, None, None]:
    for url in input.channel:
        yield DownloadTask(start_sec=0, end_sec=0, channel=url)


###############################
#
# Orchestration functions:
#
# Launch download workers in async and processing workers in parallel


class AudioProcessor:
    def __init__(
        self,
        num_workers: int = 4,  # Number of processor for audio work (CPU intensive)
        max_concurrent_downloads: int = 5,  # Limit of simultaneous downloads (I/O intensive, API limits)
        max_queue_size: int = 10,  # Maximum queue size between download and processing (Memory intensive: all pending files are saved locally)
    ):
        self.num_workers = num_workers
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.queue = asyncio.Queue(maxsize=max_queue_size)

    async def run(self, input: ProcessorInput):
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            download = asyncio.create_task(self._download_worker(input))
            workers = [
                asyncio.create_task(self._process_worker(executor, i))
                for i in range(self.num_workers)
            ]

            await download
            await asyncio.gather(*workers)

    async def _download_worker(self, input: ProcessorInput):
        """Launch downloads and queue them as they complete"""
        tasks = [
            self._download_and_queue(download_task)
            for download_task in generate_download_tasks(input)
        ]

        # Wait for all to finish
        await asyncio.gather(*tasks)

        # End signals
        for _ in range(self.num_workers):
            await self.queue.put(None)

    async def _process_worker(self, executor: ProcessPoolExecutor, worker_id: str):
        while True:
            task: DownloadTask = await self.queue.get()
            if task is None:
                break

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, process_audio, task)
            print(f"Worker {worker_id} ✓ Processed: {task}")

    async def _download_and_queue(self, task: DownloadTask):
        """All-in-one: download AND queue"""
        async with self.semaphore:
            processing_task = await download_audio(task)
            await self.queue.put(processing_task)
            print(f"✓ {processing_task.audio_url}")


if __name__ == "__main__":
    import os

    new_workers = max(1, os.cpu_count() - 2)  # Laisser 1-2 CPUs libres pour l'OS

    asyncio.run(
        AudioProcessor(num_workers=new_workers).run(
            ProcessorInput(
                channel="tf1",
                start_date="2024-01-01",
            )
        )
    )
