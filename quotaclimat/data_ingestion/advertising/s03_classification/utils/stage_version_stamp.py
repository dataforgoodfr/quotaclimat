import hashlib
from datetime import UTC, datetime

from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    llm_runner,
)


def prompt_version(prompt: llm_runner.Prompt) -> str:
    text = f"{prompt.system_prompt or ''}\n\x00{prompt.user_prompt}"
    return f"{prompt.name}_{hashlib.sha256(text.encode()).hexdigest()[:8]}"


def find_entry(prediction: list[dict] | None, stage: str) -> dict:
    return next((e for e in prediction or [] if e.get("stage") == stage), {})


def find_response(prediction: list[dict] | None, stage: str) -> dict:
    return find_entry(prediction, stage).get("raw_response") or {}


def new_stamp(stage: str, model: str | None = None, **inputs: str | None) -> dict:
    """The fields one run of `stage` writes on every result.
    `inputs` are the versions of whatever else shapes the answer (prompt, taxonomy...).
    Results sharing a `config_hash` were produced from the same inputs.
    """
    versions = {k: v for k, v in {"model": model, **inputs}.items() if v}
    payload = "\n".join(f"{k}={v}" for k, v in sorted(versions.items()))
    return {
        "stage": stage,
        "model": model,
        "run_id": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        "versions": {
            "config_hash": hashlib.sha256(payload.encode()).hexdigest()[:12],
            **versions,
        },
    }
