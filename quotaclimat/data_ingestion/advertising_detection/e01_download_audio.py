import os
from dataclasses import dataclass
from datetime import timedelta

from .e00_partition_window import DownloadTask
from .tools.mediatree import CachedMediatreeAPI

api = CachedMediatreeAPI()


@dataclass
class ProcessingTask:
    audio_file_path: str
    download_task: DownloadTask
    download_was_cached: bool = False

    @property
    def identifier(self) -> str:
        return self.download_task.identifier


async def download_audio(task: DownloadTask) -> ProcessingTask:
    # Check if file already exists before download to detect cache hits
    expected_path = os.path.join(
        api.export_folder,
        api._file_name(
            task.channel, task.start_date, task.end_date + timedelta(minutes=1)
        ),
    )
    was_cached = os.path.isfile(expected_path)

    audio_file_path = await api.download_export(
        task.channel, task.start_date, task.end_date + timedelta(minutes=1), "mp3"
    )  # on ajoute une minute pour être sûr de couvrir toute la période

    return ProcessingTask(
        audio_file_path=audio_file_path,
        download_task=task,
        download_was_cached=was_cached,
    )
