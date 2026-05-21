"""
Embedding backends and hybrid embedding+LLM novelty filtering.

Used by cluster_llm_v2 to compare freshly generated labels against the current
taxonomy and keep only the ones that express genuinely new claims.
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
from rrs.clustering.cost import _PRICING, _cost
from rrs.clustering.prompts import SYSTEM_PROMPT
from rrs.clustering.providers import (
    EMBEDDING_BACKEND_MISTRAL,
    EMBEDDING_BACKEND_ST,
    PROVIDER_ANTHROPIC,
    PROVIDER_MISTRAL,
)
from sentence_transformers import SentenceTransformer
from tqdm.asyncio import tqdm

_EMBEDDING_MODEL = "dangvantuan/sentence-camembert-large"
_MISTRAL_EMBED_MODEL = "mistral-embed"
_MISTRAL_EMBED_BATCH_SIZE = 64
_MISTRAL_EMBED_PRICING_PER_M = (
    0.10  # USD per million input tokens — verify at mistral.ai/technology/
)

MAX_CONCURRENT = 1


class EmbeddingBackend(ABC):
    """Common interface for embedding backends, matching SentenceTransformer.encode()."""

    @abstractmethod
    def encode(
        self,
        texts: list[str],
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
    ) -> np.ndarray: ...

    def total_cost(self) -> float:
        """Return accumulated cost in USD. Local backends always return 0."""
        return 0.0


class SentenceTransformerBackend(EmbeddingBackend):
    def __init__(self, model_name: str = _EMBEDDING_MODEL):
        self._model = SentenceTransformer(model_name)

    def encode(
        self,
        texts: list[str],
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
    ) -> np.ndarray:
        return np.array(
            self._model.encode(
                texts,
                normalize_embeddings=normalize_embeddings,
                show_progress_bar=show_progress_bar,
            )
        )


class MistralEmbeddingBackend(EmbeddingBackend):
    """Calls mistral-embed via the Mistral API in batches, returns L2-normalised embeddings."""

    def __init__(
        self,
        api_key: str,
        model: str = _MISTRAL_EMBED_MODEL,
        batch_size: int = _MISTRAL_EMBED_BATCH_SIZE,
    ):
        from mistralai import Mistral

        self._client = Mistral(api_key=api_key)
        self._model = model
        self._batch_size = batch_size
        self._input_tokens: int = 0

    def encode(
        self,
        texts: list[str],
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
    ) -> np.ndarray:
        all_embs: list[list[float]] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            response = self._client.embeddings.create(model=self._model, inputs=batch)
            if response.usage:
                self._input_tokens += response.usage.prompt_tokens or 0
            all_embs.extend(item.embedding for item in response.data)
        embs = np.array(all_embs, dtype=np.float32)
        if normalize_embeddings:
            norms = np.linalg.norm(embs, axis=1, keepdims=True)
            embs = embs / np.where(norms == 0, 1, norms)
        return embs

    def total_cost(self) -> float:
        return (self._input_tokens / 1_000_000) * _MISTRAL_EMBED_PRICING_PER_M


@dataclass
class LLMBackend:
    """Thin adapter over Mistral or Anthropic async clients."""

    provider: str
    model: str
    client: Any
    _input_tokens: int = 0
    _output_tokens: int = 0

    async def chat(self, messages: list[dict], max_tokens: int = 512) -> str:
        """Send user messages, accumulate real token usage, and return the text response."""
        if self.provider == PROVIDER_ANTHROPIC:
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=0,
                system=SYSTEM_PROMPT,
                messages=messages,
            )
            self._input_tokens += response.usage.input_tokens
            self._output_tokens += response.usage.output_tokens
            return next(b.text for b in response.content if b.type == "text")
        else:
            response = await self.client.chat.complete_async(
                model=self.model,
                max_tokens=max_tokens,
                temperature=0,
                messages=[{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            )
            if response.usage:
                self._input_tokens += response.usage.prompt_tokens or 0
                self._output_tokens += response.usage.completion_tokens or 0
            return response.choices[0].message.content

    def total_cost(self) -> float:
        """Return the total cost in USD based on accumulated token usage."""
        return _cost(self._input_tokens, self._output_tokens, self.provider)

    def cost_summary(self) -> str:
        """Return a human-readable cost summary string."""
        p = _PRICING.get(self.provider, _PRICING[PROVIDER_MISTRAL])
        cost = self.total_cost()
        return (
            f"  Provider    : {self.provider} ({self.model})\n"
            f"  Input tokens: {self._input_tokens:,}\n"
            f"  Output tokens: {self._output_tokens:,}\n"
            f"  Rates       : ${p['input']}/M input, ${p['output']}/M output\n"
            f"  Total cost  : ${cost:.4f}"
        )
