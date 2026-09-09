from dataclasses import dataclass
from datetime import datetime


@dataclass
class Segment:
    start_date: datetime
    end_date: datetime
    channel: str

    @property
    def identifier(self) -> str:
        return f"{self.channel}_{self.start_date.strftime('%Y-%m-%d_%H-%M-%S')}"
