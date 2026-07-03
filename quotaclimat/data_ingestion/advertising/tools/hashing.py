import hashlib
import json


def make_params_hash(params: dict) -> str:
    """Stable 16-char hash of a parameter dict, used as a cache/DB key."""
    serialized = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]
