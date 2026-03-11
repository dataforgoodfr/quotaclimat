from dataclasses import dataclass


@dataclass
class DownloadTask:
    start_sec: float
    end_sec: float
    channel: str
