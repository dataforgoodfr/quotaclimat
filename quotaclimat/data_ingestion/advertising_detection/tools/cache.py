# with LocalCache(name="segments", version=cache_key) as cache:
#     file_name = (
#         processing_task.channel
#         + "/"
#         + processing_task.start_date.strftime("%Y-%m-%d_%H-%M-%S")
#         + ".json"
#     )
#     if cache.exists(file_name):
#         return True
#     else:
#         segments = SegmentCreator().run(
#             processing_task.audio_file_path, processing_task.start_date.timestamp()
#         )
#         cache.set(file_name, [fp.to_dict() for fp in segments])
#         return False

import os
import shutil
from pathlib import Path

GLOBAL_CACHE_FOLDER = ".cache"


class LocalCache:
    def __init__(self, name: str, version: str):
        self.cache_folder = Path(GLOBAL_CACHE_FOLDER) / name / version
        self.clean_on_exit = (
            os.environ.get("CLEAN_CACHE_ON_EXIT") or False
        )  # This is activated by default for testing purpose, should be corrected

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.clean_on_exit:
            shutil.rmtree(self.cache_folder, ignore_errors=True)

    def exists(self, file_name: str) -> bool:
        return (self.cache_folder / file_name).is_file()

    def set(self, file_name: str, data: str):
        file_path = self.cache_folder / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(data)

    def get(self, file_name: str) -> str:
        with open(self.cache_folder / file_name, "r") as f:
            return f.read()
