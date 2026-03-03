import json
from dataclasses import dataclass


@dataclass
class Segment:
    """Un segment audio."""

    start_sec: float = 0.0
    end_sec: float = 0.0

    def to_dict(self) -> dict:
        return {
            "s": self.start_sec,
            "e": self.end_sec,
        }


@dataclass
class TextSegment:
    """Un segment audio avec son texte transcrit."""

    text: str
    start_sec: float = 0.0
    end_sec: float = 0.0

    def to_dict(self) -> dict:
        return {
            "t": self.text,
            "s": self.start_sec,
            "e": self.end_sec,
        }


def save_segment_groups_to_json(
    groups: list[Segment] | list[TextSegment], output_path: str
):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump([g.to_dict() for g in groups], f, ensure_ascii=False)
