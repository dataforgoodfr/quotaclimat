"""Reusable per-item Mistral classification core for misinformation detection.

Kept lean (only mistralai + stdlib) so it can be imported from any image that
has the mistralai package — including the pulsar image — without pulling in
duckdb, pandas, or other heavy deps from main.py.
"""

import asyncio
import logging
from typing import Literal

from mistralai.client import Mistral
from mistralai.client.types import BaseModel
from mistralai.extra.run.context import RunContext

PRICING = {
    "mistral-small-2603": (0.15, 0.60),
    "mistral-small-3.2": (0.10, 0.30),
    "mistral-medium-2505": (0.40, 2.00),
    "mistral-large-2411": (2.00, 6.00),
}


class MisinfoResult(BaseModel):
    analysis: str
    label: Literal["oui", "non", "incertain"]
    score: float
    justification: str


async def classify_one(
    client: Mistral,
    semaphore: asyncio.Semaphore,
    system_prompt: str,
    index: int,
    total: int,
    text: str,
    model: str,
    user_prefix: str = "Analyse cet extrait et détecte une éventuelle désinformation :\n\n",
) -> tuple[MisinfoResult, int, int]:
    """Classify a single text.  Returns (result, input_tokens_est, output_tokens_est)."""
    async with semaphore:
        logging.info(f"[{index + 1}/{total}] Classifying...")
        async with RunContext(model=model, output_format=MisinfoResult) as run_ctx:
            run_result = await client.beta.conversations.run_async(
                run_ctx=run_ctx,
                instructions=system_prompt,
                inputs=[{"role": "user", "content": f"{user_prefix}{text}"}],
            )
        output_text = run_result.output_entries[0].content if run_result.output_entries else ""
        input_tokens = len(system_prompt + text) // 4
        output_tokens = len(output_text) // 4
        return run_result.output_as_model, input_tokens, output_tokens
