"""Building blocks shared by the response models.

A response model's JSON schema constrains what the model generates, and the model then
validates the answer. Only enums are hard errors: what the model can plausibly get
cosmetically wrong is coerced, and each coercion is recorded with note.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Annotated

from pydantic import BeforeValidator, Field

MAX_TEXT_LEN = 500

# Unknown keys are dropped rather than rejected
MODEL_CONFIG = {
    "extra": "ignore",
    "str_strip_whitespace": True,
    "json_schema_extra": {"additionalProperties": False},
}

# max_length keeps the limit in the schema handed to the endpoint; the BeforeValidator trims anything that comes back over it anyway.
SHORT_TEXT = Annotated[
    str,
    BeforeValidator(lambda v: v[:MAX_TEXT_LEN] if isinstance(v, str) else v),
    Field(max_length=MAX_TEXT_LEN),
]

_anomalies: ContextVar[list[str] | None] = ContextVar("anomalies", default=None)


def note(code: str) -> None:
    """Record that a coercion happened"""
    bucket = _anomalies.get()
    if bucket is not None:
        bucket.append(code)


@contextmanager
def coercion_scope():
    """Collect the coercions noted while validating one answer."""
    bucket: list[str] = []
    token = _anomalies.set(bucket)
    try:
        yield bucket
    finally:
        _anomalies.reset(token)


def blank_to(fallback, code: str):
    """A validator reading an empty string as no value, noted as ``code``."""

    def validator(value):
        if isinstance(value, str) and not value.strip():
            note(code)
            return fallback
        return value

    return validator
