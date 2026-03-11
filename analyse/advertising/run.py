# ruff: noqa: E402

import os
import sys

repo_root_path = os.path.abspath(os.path.join("."))

if repo_root_path not in sys.path:
    sys.path.append(repo_root_path)

import asyncio

from analyse.advertising.s01_advertising_detection.e00_download_audio_files.partition_window import (
    partition_week,
)
from analyse.advertising.s01_advertising_detection.processor import AudioProcessor

if __name__ == "__main__":
    import os

    new_workers = max(1, os.cpu_count() - 2)  # Laisser 1-2 CPUs libres pour l'OS

    asyncio.run(
        AudioProcessor(
            num_workers=new_workers,
            task_partition=partition_week(
                channel="tf1",
                start_date="2025-06-01",
            ),
        ).run()
    )
