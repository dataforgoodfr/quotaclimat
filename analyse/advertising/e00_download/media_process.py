import asyncio
from concurrent.futures import ProcessPoolExecutor

import librosa


class AudioProcessor:
    def __init__(self, num_workers=4, max_concurrent_downloads=5):
        self.num_workers = num_workers
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.queue = asyncio.Queue(maxsize=10)

    async def download_and_queue(self, url):
        async with self.semaphore:
            file_path = await self._download_async(url)
            await self.queue.put(file_path)
            print(f"✓ {file_path}")

    async def download_worker(self, file_urls):
        tasks = [self.download_and_queue(url) for url in file_urls]

        # Attendre que tous finissent
        await asyncio.gather(*tasks)

        # Signaux de fin
        for _ in range(self.num_workers):
            await self.queue.put(None)

    async def process_worker(self, executor, worker_id):
        while True:
            file_path = await self.queue.get()
            if file_path is None:
                break

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, self._process_audio, file_path)
            print(f"Worker {worker_id} ✓ Traité: {file_path}")

    def _process_audio(self, filepath):
        print(f"Processing {filepath}...")
        return
        y, sr = librosa.load(filepath, sr=16000)
        return librosa.feature.mfcc(y=y, sr=sr)

    async def _download_async(self, url):
        await asyncio.sleep(0.1)
        return f"file_{url}.wav"

    async def run(self, file_urls):
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            download = asyncio.create_task(self.download_worker(file_urls))
            workers = [
                asyncio.create_task(self.process_worker(executor, i))
                for i in range(self.num_workers)
            ]

            await download
            await asyncio.gather(*workers)
