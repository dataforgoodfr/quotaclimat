import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Literal

from .e02_create_chunks import Chunk
from .e04_group_chunks import ChunkGroup


@dataclass
class Fragment:
    start_date: datetime
    end_date: datetime
    channel: str
    classification: Literal[
        "already_known_ad", "new_ad", "content", "jingle", "unknown"
    ]
    group_id: str | None = None
    chunks: list[Chunk] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Fragment":
        return cls(
            start_date=datetime.fromisoformat(data["start_date"]),
            end_date=datetime.fromisoformat(data["end_date"]),
            channel=data["channel"],
            classification=data["classification"],
            group_id=data.get("group_id"),
            chunks=[Chunk(**c) for c in data.get("chunks", [])],
        )


class FragmentsClassifier:
    def __init__(self, repetition_threshold: int = 5):
        self.repetition_threshold = repetition_threshold

    def run(self, groups: ChunkGroup) -> list[Fragment]:
        """
        Classify the chunks of audio files into fragments.
        """
        fragments: list[Fragment] = []
        for index, group in enumerate(groups):
            if group.count == 1:
                occ = group.occurrences[0]
                fragments.append(
                    Fragment(
                        start_date=datetime.fromtimestamp(occ.start_sec),
                        end_date=datetime.fromtimestamp(occ.end_sec),
                        channel=occ.channel,
                        classification="content",
                        chunks=[occ],
                    )
                )
            else:
                classification = (
                    "new_ad" if group.count >= self.repetition_threshold else "unknown"
                )
                for occ in group.occurrences:
                    fragments.append(
                        Fragment(
                            start_date=datetime.fromtimestamp(occ.start_sec),
                            end_date=datetime.fromtimestamp(occ.end_sec),
                            channel=occ.channel,
                            classification=classification,
                            group_id=f"g-{index}",
                            chunks=[occ],
                        )
                    )

        return fragments

    def params(self) -> dict:
        """Returns all constructor parameters as a dict."""
        return {
            "repetition_threshold": self.repetition_threshold,
        }

    def params_hash(self) -> str:
        """Returns a stable SHA256 hash of all constructor parameters.
        Changes when any parameter value changes, identical otherwise."""
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
