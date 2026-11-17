"""Calls to the LLM endpoints (OpenAI-compatible), shared by every stage that calls a model.

A stage selects the ids of the ads to process and gives ``run_stage`` a ``process_one(ad_id)``,
which loads the ad, calls the model and returns a ``StageResult``: a status, counted in the
progress bar, and the statement storing the result. ``run_stage`` runs it in ``workers``
threads and saves each result as soon as it comes back, so an interrupted run only loses the
requests in flight, and the next run resumes where it stopped.

The sector_and_brand and environmental_content stages share the VLM set up here. Other
endpoints (OCR, ASR) are set up in their stage, and go through `run_stage` too.
"""

import json
import logging
import os
import re
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import av
import yaml
from jinja2 import Environment, StrictUndefined
from openai import APIError, OpenAI
from pydantic import BaseModel, ValidationError
from sqlalchemy import Executable
from tqdm import tqdm

from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    frame_splitting,
)

MODEL_NAME = os.environ.get("VLM_MODEL_NAME")

# Warning: sampling parameters shape the answer as much as the prompt does.
# Set by the VLM_* variables (.advertising.env), the values below when unset or empty.
SAMPLING = {
    # should be 1.0 with thinking; 0.0 without thinking
    "temperature": float(os.environ.get("VLM_TEMPERATURE") or 1.0),
    "top_p": float(os.environ.get("VLM_TOP_P") or 0.9),
    "top_k": int(os.environ.get("VLM_TOP_K") or 20),
    # should be 0.0 with thinking
    "presence_penalty": float(os.environ.get("VLM_PRESENCE_PENALTY") or 0.0),
    "repetition_penalty": float(os.environ.get("VLM_REPETITION_PENALTY") or 1.0),
    "reasoning_effort": os.environ.get("VLM_REASONING_EFFORT") or "low",
    "enable_thinking": (os.environ.get("VLM_ENABLE_THINKING") or "1") == "1",
}
# In the stamp of each result, as the settings can change from one run to the next.
SAMPLING_VERSION = ",".join(f"{k}={v}" for k, v in SAMPLING.items())
EXTRA_API_PARAMS = {
    **{
        k: v for k, v in SAMPLING.items() if k not in ("temperature", "enable_thinking")
    },
    "chat_template_kwargs": {
        "enable_thinking": SAMPLING["enable_thinking"],
        "preserve_thinking": False,
    },
    "allowed_openai_params": ["reasoning_effort"],
}

# Deterministic media errors, recorded instead of being retried on every run.
VIDEO_DECODE_ERROR_MARKERS = (
    "could not open video stream",
    "could not open audio stream",
    "invalid data found when processing input",
    "could not find codec parameters",
)

# Qwen3-VL encodes a video by pairs of frames and rejects a video with only one readable frame,
# so a video with fewer readable frames is sent as images, or not at all when it has none.
MIN_VIDEO_FRAMES = 2

# Compiled once per prompt text; a variable missing from the record raises instead of rendering empty.
_template = cache(Environment(undefined=StrictUndefined).from_string)

_THINK_BLOCK = re.compile(r"<think>.*?</think>", re.DOTALL)
_CODE_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$")


@dataclass
class Prompt:
    name: str
    system_prompt: str
    user_prompt: str

    def messages(self, record: dict, media: Sequence[dict] = ()) -> list[dict]:
        text = _template(self.user_prompt).render(record)
        return [
            {"role": "system", "content": _template(self.system_prompt).render(record)},
            {"role": "user", "content": [*media, {"type": "text", "text": text}]},
        ]


def load_prompt(path: str | Path) -> Prompt:
    path = Path(path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return Prompt(path.stem, data["system_prompt"], data["user_prompt"])


@dataclass
class InferenceResult:
    status: str  # "ok" or "error"
    content: str | None = None
    error: str | None = None
    parsed: dict | None = None


def call_llm(
    client: OpenAI,
    messages: list[dict],
    schema: type[BaseModel] | None = None,
    **params: Any,
) -> InferenceResult:
    if schema is not None:
        params["response_format"] = _response_format(schema)
    try:
        response = client.chat.completions.create(messages=messages, **params)
    except APIError as e:
        return InferenceResult("error", error=f"{type(e).__name__}: {e}")

    choice = response.choices[0]
    content = choice.message.content
    if not content:
        error = f"empty response (finish_reason={choice.finish_reason})"
        return InferenceResult("error", error=error)
    if schema is None:
        return InferenceResult("ok", content)
    if choice.finish_reason == "length":
        return InferenceResult("error", content, "output truncated at max_tokens")
    data = parse_json(content)
    if data is None:
        return InferenceResult("error", content, "response is not valid JSON")
    try:
        parsed = schema.model_validate(data).model_dump(mode="json")
    except ValidationError as e:
        details = "; ".join(
            f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()
        )
        return InferenceResult("error", content, f"invalid response: {details}")
    return InferenceResult("ok", content, parsed=parsed)


@cache
def _response_format(schema: type[BaseModel]) -> dict:
    json_schema = {"name": "response", "schema": schema.model_json_schema()}
    return {"type": "json_schema", "json_schema": json_schema}


def parse_json(text: str | None) -> Any:
    if not text:
        return None
    text = _CODE_FENCE.sub("", _THINK_BLOCK.sub("", text).strip())
    candidates = [text]
    if text.startswith("{{"):
        candidates.append(text[1:])
    for open_char, close_char in ("{}", "[]"):
        start, end = text.find(open_char), text.rfind(close_char)
        if 0 <= start < end:
            candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    return None


def new_vlm_client(timeout: float = 300) -> OpenAI:
    return OpenAI(
        base_url=os.environ["VLM_API_URL"],
        api_key=os.environ["VLM_API_TOKEN"],
        timeout=timeout,
        max_retries=1,
    )

# call to the llm but adding the video content
def call_vlm(
    client: OpenAI,
    prompt: Prompt,
    record: dict,
    schema: type[BaseModel],
    video_url: str | None = None,
    max_tokens: int = 6000,
) -> InferenceResult:
    media = video_content(video_url) if video_url else []
    return call_llm(
        client,
        prompt.messages(record, media),
        schema,
        model=MODEL_NAME,
        temperature=SAMPLING["temperature"],
        max_tokens=max_tokens,
        extra_body=EXTRA_API_PARAMS,
    )


def video_content(url: str) -> list[dict]:
    """The video for the message, or its readable frames as images (see MIN_VIDEO_FRAMES)."""
    video = [{"type": "video_url", "video_url": {"url": url}}]
    try:
        frames = frame_splitting.first_frames(url, MIN_VIDEO_FRAMES)
    except av.FFmpegError:
        # Sent anyway: the server's decode error is recorded (VIDEO_DECODE_ERROR_MARKERS).
        return video
    if len(frames) >= MIN_VIDEO_FRAMES:
        return video
    logging.info(
        "%s: %d readable frame(s), sent as images instead of the video",
        urlparse(url).path,
        len(frames),
    )
    return [
        {"type": "image_url", "image_url": {"url": frame_splitting.data_url(f)}}
        for f in frames
    ]


def is_video_decode_error(error: str | None) -> bool:
    return any(marker in (error or "").lower() for marker in VIDEO_DECODE_ERROR_MARKERS)


def rejected_response(result: InferenceResult) -> tuple[dict | None, str | None]:
    """What to keep of an answer that failed validation, its JSON, or when it wasn't
    JSON at all, the start of its text appended to the error."""
    parsed = parse_json(result.content)
    if parsed is None:
        return None, f"{result.error} | raw[:400]={result.content[:400]!r}"
    return parsed, result.error


@dataclass
class StageResult:
    ad_id: str
    status: str  # counted in the progress bar
    statement: Executable | None = None  # None saves nothing: the ad stays pending
    error: str | None = None


def stage_result(
    ad_id: str, result: InferenceResult, statement: Executable | None
) -> StageResult:
    """The StageResult of a model answer, given the statement storing it."""
    if statement is None:
        status = "left_pending"  # e.g. a timeout, retried by the next run
    elif result.status == "ok":
        status = "ok"
    elif result.content:
        status = "invalid_answer"  # stored with its parse error
    else:
        status = "video_error"
    return StageResult(ad_id, status, statement, result.error)


def run_stage(
    ad_ids: Sequence[str],
    process_one: Callable[[str], StageResult],
    workers: int,
    desc: str,
    chunk_size: int = 10_000,
) -> None:
    """Runs process_one (defined per stage) on every ad in ``workers`` threads, and saves each result as soon as
    it comes back, in its own transaction.

    At most workers ads are processed at once. The ads are handed to the threads chunk_size at a time rather than all at once,
    so memory doesn’t grow with their number.
    When process_one raises, or its result can't be saved, the error is logged and the run
    goes on, nothing is saved for that ad, so the next run retries it. Ctrl-C drops the ads
    not started yet; the run then ends once the requests in flight are back, without saving
    them (Ctrl-C again to quit at once).
    """
    counts = Counter()
    with (
        ThreadPoolExecutor(max_workers=workers) as pool,
        tqdm(total=len(ad_ids), desc=desc) as progress,
    ):
        for start in range(0, len(ad_ids), chunk_size):
            chunk = ad_ids[start : start + chunk_size]
            futures = {pool.submit(process_one, ad_id): ad_id for ad_id in chunk}
            try:
                for future in as_completed(futures):
                    ad_id = futures[future]
                    try:
                        result = future.result()
                        database.save(result.statement)
                    except Exception:
                        logging.exception("[%s] %s: failed, left pending", desc, ad_id)
                        result = StageResult(ad_id, "failed")
                    if result.error:
                        logging.warning("[%s] %s: %s", desc, ad_id, result.error)
                    counts[result.status] += 1
                    progress.update()
                    progress.set_postfix(counts, refresh=False)
            except KeyboardInterrupt:
                for future in futures:
                    future.cancel()
                raise
    logging.info("[%s] done: %s", desc, dict(counts))
