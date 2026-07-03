from dataclasses import asdict, dataclass


@dataclass
class Fingerprint:
    duration_sec: float
    energy_mean: float
    spectral_centroid: float
    zcr_mean: float
    peaks: list = None
    pairs: list = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        d.setdefault("peaks", None)
        d.setdefault("pairs", d.pop("hashes", None))
        if d["pairs"] is not None:
            d["pairs"] = [tuple(p) for p in d["pairs"]]
        return cls(**d)
