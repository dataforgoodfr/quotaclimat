import os
from datetime import timedelta

from .e00_partition_window import Partition
from .tools.mediatree import CachedMediatreeAPI

api = CachedMediatreeAPI()


async def download_audio(task: Partition) -> (str, bool):
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

    return audio_file_path, was_cached
