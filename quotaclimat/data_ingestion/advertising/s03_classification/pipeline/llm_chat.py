"""LLM chat helper used by classify_ad and classify_sucat"""

import json
import time
from typing import Any

from openai import OpenAI


def chat_completion_to_dict(
    client: OpenAI,
    *,
    model: str,
    messages: list[dict],
    schema: dict | None,
    timeout: int,
    retries: int,
    max_tokens: int = 3000,
    schema_name: str = "response",
) -> dict[str, Any]:
    """Chat completion that returns a parsed JSON object. Retries on transport errors only."""
    extra_body: dict[str, Any] = {
        "top_k": 0,
        "presence_penalty": 0,
        "chat_template_kwargs": {"enable_thinking": False},
    }

    if schema is not None:
        response_format: dict[str, Any] = {
            "type": "json_schema",
            "json_schema": {"name": schema_name, "schema": schema},
        }
    else:
        response_format = {"type": "json_object"}

    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0,
                max_tokens=max_tokens,
                response_format=response_format,
                extra_body=extra_body,
                timeout=timeout,
            )
            content = response.choices[0].message.content or ""
            return json.loads(content)
        except json.JSONDecodeError:
            # Don't retry parse errors
            raise
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2**attempt)
    assert last_err is not None
    raise last_err
