from dataclasses import asdict, dataclass
from typing import Literal


@dataclass
class Fingerprint:
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


@dataclass
class Chunk:
    start_sec: float
    end_sec: float
    channel: str
    fingerprint: Fingerprint

    def to_dict(self):
        return {
            "start_sec": self.start_sec,
            "end_sec": self.end_sec,
            "channel": self.channel,
            "fingerprint": self.fingerprint.to_dict(),
        }

    @classmethod
    def from_dict(cls, d):
        fp_data = d.get("fingerprint")
        if fp_data is not None:
            return cls(
                start_sec=d["start_sec"],
                end_sec=d["end_sec"],
                channel=d["channel"],
                fingerprint=Fingerprint.from_dict(fp_data),
            )
        # Legacy flat format support
        return cls(
            start_sec=d["start_sec"],
            end_sec=d["end_sec"],
            channel=d["channel"],
            fingerprint=Fingerprint(
                duration_sec=d["duration_sec"],
                energy_mean=d["energy_mean"],
                spectral_centroid=d["spectral_centroid"],
                zcr_mean=d["zcr_mean"],
                peaks=d.get("peaks"),
                hashes=d.get("hashes"),
            ),
        )


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
            chunks=[Chunk.from_dict(c) for c in data.get("chunks", [])],
        )
