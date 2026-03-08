import json
from dataclasses import dataclass


@dataclass
class Segment:
    """Un segment audio avec timestamps absolus (epoch Unix en secondes)."""

    start_epoch: float = 0.0
    end_epoch: float = 0.0

    @property
    def duration_sec(self) -> float:
        return self.end_epoch - self.start_epoch

    def to_dict(self) -> dict:
        return {
            "start_epoch": self.start_epoch,
            "end_epoch": self.end_epoch,
        }


@dataclass
class TextSegment:
    """Un segment audio avec son texte transcrit et timestamps absolus (epoch Unix)."""

    text: str
    start_epoch: float = 0.0
    end_epoch: float = 0.0

    @property
    def duration_sec(self) -> float:
        return self.end_epoch - self.start_epoch

    def to_dict(self) -> dict:
        return {
            "text": self.text,
            "start_epoch": self.start_epoch,
            "end_epoch": self.end_epoch,
        }


def save_segment_groups_to_json(
    groups: list[Segment] | list[TextSegment], output_path: str
):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump([g.to_dict() for g in groups], f, ensure_ascii=False)
