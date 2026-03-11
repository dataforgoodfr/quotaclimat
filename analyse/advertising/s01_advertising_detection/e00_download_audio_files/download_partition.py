from dataclasses import dataclass
from datetime import datetime, timedelta

from .mediatree import CachedMediatreeAPI
from .partition_window import DownloadTask

api = CachedMediatreeAPI()


@dataclass
class ProcessingTask:
    audio_file_path: str
    start_date: datetime
    end_date: datetime
    channel: str


async def download_audio(task: DownloadTask) -> ProcessingTask:
    audio_file_path = await api.download_export(
        task.channel, task.start_date, task.end_date + timedelta(minutes=1)
    )  # on ajoute une minute pour être sûr de couvrir toute la période

    return ProcessingTask(
        audio_file_path=audio_file_path,
        start_date=task.start_date,
        end_date=task.end_date,
        channel=task.channel,
    )
