import os
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..tools.mediatree import CachedMediatreeAPI
from .partition_window import DownloadTask

api = CachedMediatreeAPI()


@dataclass
class ProcessingTask:
    audio_file_path: str
    start_date: datetime
    end_date: datetime
    channel: str
    download_was_cached: bool = False


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
        start_date=task.start_date,
        end_date=task.end_date,
        channel=task.channel,
        download_was_cached=was_cached,
    )
