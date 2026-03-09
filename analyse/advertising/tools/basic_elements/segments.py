import json
from dataclasses import dataclass


@dataclass
class Segment:
    """Un segment audio."""

    start_sec: float = 0.0
    end_sec: float = 0.0
    # Features acoustiques optionnelles (calculées par le rupture_detector)
    energy_mean: float = 0.0       # RMS moyen
    spectral_centroid: float = 0.0  # Centroïde spectral moyen (Hz)
    zcr: float = 0.0               # Zero-crossing rate moyen

    def to_dict(self) -> dict:
        d = {"s": self.start_sec, "e": self.end_sec}
        if self.energy_mean:
            d["rms"] = self.energy_mean
        if self.spectral_centroid:
            d["sc"] = self.spectral_centroid
        if self.zcr:
            d["zcr"] = self.zcr
        return d


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


def load_segment_groups_from_json(input_path: str) -> list[Segment]:
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return [
            Segment(
                start_sec=d["s"],
                end_sec=d["e"],
                energy_mean=d.get("rms", 0.0),
                spectral_centroid=d.get("sc", 0.0),
                zcr=d.get("zcr", 0.0),
            )
            for d in data
        ]


def load_text_segments_from_json(input_path: str) -> list[TextSegment]:
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return [
            TextSegment(text=d["t"], start_sec=d["s"], end_sec=d["e"]) for d in data
        ]
