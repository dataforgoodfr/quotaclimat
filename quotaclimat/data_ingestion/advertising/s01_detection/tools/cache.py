import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


GLOBAL_CACHE_FOLDER = os.environ.get("GLOBAL_CACHE_FOLDER", ".cache")


class LocalCache:
    def __init__(self, name: str, version: str):
        self.cache_folder = Path(GLOBAL_CACHE_FOLDER) / name / version
        self.cache_folder.mkdir(parents=True, exist_ok=True)
        self.clean_on_exit = (
            os.environ.get("CLEAN_CACHE_ON_EXIT", "false").lower() == "true"
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.clean_on_exit:
            shutil.rmtree(self.cache_folder, ignore_errors=True)

    def exists(self, file_name: str) -> bool:
        path = self.cache_folder / file_name
        return path.is_file() and path.stat().st_size > 0

    def set(self, file_name: str, data: str):
        file_path = self.cache_folder / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(data)

    def get(self, file_name: str) -> str:
        with open(self.cache_folder / file_name, "r") as f:
            return f.read()
