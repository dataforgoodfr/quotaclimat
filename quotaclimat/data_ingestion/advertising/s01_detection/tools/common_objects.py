from dataclasses import asdict, dataclass
from typing import Literal


@dataclass
class Chunk:
    start_sec: float
    end_sec: float
    channel: str
    duration_sec: float
    energy_mean: float
    spectral_centroid: float
    zcr_mean: float
    peaks: list = None
    hashes: list = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


FragmentClassification = Literal[
    "already_known_ad", "new_ad", "content", "jingle", "unknown"
]


@dataclass
class Fragment:
    start_sec: float
    end_sec: float
    channel: str
    classification: FragmentClassification
    group_id: str | None = None
    chunks: list[Chunk] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Fragment":
        return cls(
            start_sec=data["start_sec"],
            end_sec=data["end_sec"],
            channel=data["channel"],
            classification=data["classification"],
            group_id=data.get("group_id"),
            chunks=[Chunk(**c) for c in data.get("chunks", [])],
        )
