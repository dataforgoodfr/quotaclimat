"""
LLM-based text clustering — shared building blocks.

Three-step pipeline pieces consumed by cluster_llm_v2:
  1. Generate narrative labels per transcript (async)
  2. Merge/deduplicate label list (hierarchical)
  3. Classify each transcript against final labels (async)

Supports two LLM providers:
  mistral   — Mistral Small
  anthropic — Claude Haiku 4.5
"""

import ast
import asyncio
import json
import math
import os
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import anthropic
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from mistralai import Mistral
from transformers import AutoTokenizer
from tqdm.asyncio import tqdm

from rrs.clustering.get_data import TEXT_COLUMN, split_sentences
from rrs.clustering.providers import (
    PROVIDER_ANTHROPIC,
    PROVIDER_MISTRAL,
    MODEL_ANTHROPIC,
    MODEL_MISTRAL,
)
from rrs.clustering.backends import (
    LLMBackend,
    EmbeddingBackend,
    MAX_CONCURRENT,
    EMBEDDING_BACKEND_MISTRAL,
    EMBEDDING_BACKEND_ST,
    MistralEmbeddingBackend,
    SentenceTransformerBackend,
    _EMBEDDING_MODEL,
)
from rrs.clustering.prompts import (
    SYSTEM_PROMPT,
    _step1_prompt,
    _step2_prompt,
    _step3_prompt,
    _zone3_prompt,
)
from rrs.clustering.cost import _cost


_LOW_THRESHOLD = 0.40  # below → auto-keep (zone 1: different topic)
_HIGH_THRESHOLD = 0.85  # above → auto-drop (zone 2: near-exact match)
_TOP_K_CONTEXT = 10  # existing labels shown to LLM per ambiguous candidate
_CANDIDATE_DEDUP_THRESHOLD = (
    0.85  # pairwise similarity above which candidates are considered redundant
)
SEED_LABELS: list[str] = [
    "Les renouvelables augmentent le coût de l'électricité",
    "Il est inutile de réduire les rejets de gaz à effet de serre de la France",
    "Les populations sont manipulées de façon injustifiée.",
    "L'élevage est neutre voire avantageux pour le climat",
    "Les voitures électriques polluent plus que les voitures thermiques",
]


def _build_client(provider: str = PROVIDER_MISTRAL) -> LLMBackend:
    load_dotenv()
    if provider == PROVIDER_ANTHROPIC:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "ANTHROPIC_API_KEY is not set. "
                "Add it to your .env file: ANTHROPIC_API_KEY=..."
            )
        return LLMBackend(
            provider=provider,
            model=MODEL_ANTHROPIC,
            client=anthropic.AsyncAnthropic(api_key=api_key),
        )
    else:
        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "MISTRAL_API_KEY is not set. "
                "Add it to your .env file: MISTRAL_API_KEY=..."
            )
        return LLMBackend(
            provider=provider,
            model=MODEL_MISTRAL,
            client=Mistral(api_key=api_key),
        )


def build_embedding_backend(
    backend: str = EMBEDDING_BACKEND_ST,
    model_name: str = _EMBEDDING_MODEL,
    mistral_api_key: Optional[str] = None,
) -> EmbeddingBackend:
    """Instantiate the requested embedding backend."""
    if backend == EMBEDDING_BACKEND_MISTRAL:
        if not mistral_api_key:
            raise EnvironmentError(
                "MISTRAL_API_KEY must be set to use the mistral embedding backend."
            )
        return MistralEmbeddingBackend(api_key=mistral_api_key)
    return SentenceTransformerBackend(model_name)


def _parse_list_response(raw: str) -> list[str]:
    """Parse a Python/JSON list from an LLM response string.

    Strips markdown code fences, then tries json.loads (double quotes) and
    ast.literal_eval (single quotes) in order.
    """
    text = re.sub(r"```(?:python|json)?\s*", "", raw).strip()

    start, end = text.find("["), text.rfind("]") + 1
    if start == -1 or end == 0:
        print(f"  [warn] no list found in response. Raw: {text[:200]!r}")
        return []
    chunk = text[start:end]

    try:
        return [str(item) for item in json.loads(chunk)]
    except json.JSONDecodeError:
        pass
    try:
        return [str(item) for item in ast.literal_eval(chunk)]
    except (ValueError, SyntaxError):
        print(f"  [warn] failed to parse list. Raw: {raw[:200]!r}")
        return []


async def _step1_call(
    doc_id: str,
    sentences: list[str],
    backend: LLMBackend,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[str]]:
    async with semaphore:
        try:
            raw = await backend.chat(
                [{"role": "user", "content": _step1_prompt(sentences)}], max_tokens=512
            )
            return doc_id, _parse_list_response(raw)
        except Exception as exc:
            print(f"  [warn] step1 {doc_id} failed: {exc}")
            return doc_id, []


async def build_labels_from_transcripts(
    sentences_by_doc: dict,
    client: LLMBackend,
    max_concurrent: int = MAX_CONCURRENT,
) -> list[str]:
    """Step 1: Generate labels per transcript in parallel.

    Each document is processed in isolation — the prompt stays small regardless of
    how many documents have been seen. No growing label list is passed between
    documents; deduplication is handled by Step 2.
    """
    doc_ids = [doc_id for doc_id, sents in sentences_by_doc.items() if sents]
    semaphore = asyncio.Semaphore(max_concurrent)

    results = await tqdm.gather(
        *[
            _step1_call(doc_id, sentences_by_doc[doc_id], client, semaphore)
            for doc_id in doc_ids
        ],
        desc=f"Step 1 — labelling [{client.provider}]",
    )

    labels_lower: set[str] = set()
    labels: list[str] = []
    for _, returned in results:
        for label in returned:
            key = label.strip().lower()
            if key and key not in labels_lower:
                labels.append(label)
                labels_lower.add(key)

    print(f"  {len(labels)} raw labels generated across all transcripts.")
    return labels


async def _merge_labels_call(label_list: list[str], client: LLMBackend) -> list[str]:
    """Merge the flat *label_list*, returning a deduplicated result.

    Returns the input list unchanged if the response cannot be parsed.
    """
    raw = await client.chat(
        [{"role": "user", "content": _step2_prompt(label_list)}], max_tokens=4096
    )
    parsed = _parse_list_response(raw)
    if not parsed:
        print(
            f"  [warn] unparseable merge response — keeping {len(label_list)} labels unchanged."
        )
        return list(label_list)
    return parsed


def compute_target_clusters(
    n_docs: int,
    min_clusters: int = 5,
    max_clusters: int = 150,
    scale_factor: float = 1.0,
) -> int:
    """Return a target cluster count that scales with corpus size.

    Uses sqrt(n_docs) as the base heuristic, clamped to [min_clusters, max_clusters].
    """
    target = int(scale_factor * math.sqrt(n_docs))
    return max(min_clusters, min(target, max_clusters))


async def merge_labels(
    label_list: list[str],
    client: LLMBackend,
    target_clusters: int,
    batch_size: int = 30,
    max_concurrent: int = MAX_CONCURRENT,
    max_rounds: int = 20,
    log_path: Optional[Path] = None,
) -> list[str]:
    """Step 2: Hierarchical (tournament-style) merge.

    Each round splits the current label list into chunks of *batch_size*, merges
    each chunk in parallel, then flattens the results. Labels are shuffled before
    each round so that different labels end up in the same group each time.
    Stops when the label count reaches *target_clusters* or *max_rounds* is
    reached, then makes one final merge call.
    """
    labels = list(label_list)
    round_num = 0
    print(f"  Merge target: ≤{target_clusters} labels (from {len(labels)} candidates)")

    while len(labels) > target_clusters and round_num < max_rounds:
        round_num += 1
        random.shuffle(labels)
        chunks = [labels[i : i + batch_size] for i in range(0, len(labels), batch_size)]
        print(
            f"  Round {round_num}: {len(labels)} labels → {len(chunks)} parallel merge calls"
        )

        semaphore = asyncio.Semaphore(max_concurrent)

        async def _merge_with_semaphore(chunk: list[str]) -> list[str]:
            async with semaphore:
                return await _merge_labels_call(chunk, client)

        results = await asyncio.gather(*[_merge_with_semaphore(c) for c in chunks])
        labels = [label for group in results for label in group]
        print(f"    → {len(labels)} labels after round {round_num}")

        if log_path is not None:
            log_path.write_text(
                json.dumps(sorted(labels), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    print(f"  Final merge call: {len(labels)} labels")
    labels = await _merge_labels_call(labels, client)
    print(f"    → {len(labels)} labels after final merge")

    if log_path is not None:
        log_path.write_text(
            json.dumps(sorted(labels), ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return labels


# ---------------------------------------------------------------------------
# Step 3 — classify transcripts against final labels (async)
# ---------------------------------------------------------------------------


async def _step3_call(
    doc_id: str,
    sentences: list[str],
    label_list: list[str],
    backend: LLMBackend,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[str]]:
    async with semaphore:
        try:
            raw = await backend.chat(
                [{"role": "user", "content": _step3_prompt(sentences, label_list)}],
                max_tokens=512,
            )
            matched = _parse_list_response(raw)
            label_lower = {l.strip().lower(): l for l in label_list}
            valid = [
                label_lower[m.strip().lower()]
                for m in matched
                if m.strip().lower() in label_lower
            ]
            return doc_id, valid
        except Exception as exc:
            print(f"  [warn] step3 {doc_id} failed: {exc}")
            return doc_id, []


async def classify_all_transcripts(
    sentences_by_doc: dict,
    label_list: list[str],
    client: LLMBackend,
    max_concurrent: int = MAX_CONCURRENT,
) -> dict:
    """Step 3: Classify all transcripts against the final label list in parallel."""
    semaphore = asyncio.Semaphore(max_concurrent)

    results = await tqdm.gather(
        *[
            _step3_call(doc_id, sentences, label_list, client, semaphore)
            for doc_id, sentences in sentences_by_doc.items()
        ],
        desc=f"Step 3 — classifying [{client.provider}]",
    )
    return dict(results)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_sentences_by_doc(
    docs_df: pd.DataFrame,
    id_col: Optional[str],
    spacy_model: str,
    window_size: int,
    overlap_tokens: int,
) -> dict:
    """Split transcripts into sentences grouped by document ID."""
    if id_col:
        sentences_df = split_sentences(
            docs_df,
            text_column=TEXT_COLUMN,
            spacy_model=spacy_model,
            window_size=window_size,
            overlap_tokens=overlap_tokens,
        )
        print(f"  {len(sentences_df)} sentence chunks total.")
        return {
            str(doc_id): group["sentence"].tolist()
            for doc_id, group in sentences_df.groupby(id_col)
        }
    else:
        sentences_by_doc: dict[str, list[str]] = {}
        for i, row in enumerate(docs_df.itertuples()):
            text = getattr(row, TEXT_COLUMN)
            tmp = pd.DataFrame({TEXT_COLUMN: [text]})
            s_df = split_sentences(
                tmp,
                text_column=TEXT_COLUMN,
                spacy_model=spacy_model,
                window_size=window_size,
                overlap_tokens=overlap_tokens,
            )
            sentences_by_doc[str(i)] = s_df["sentence"].tolist()
        total = sum(len(v) for v in sentences_by_doc.values())
        print(f"  {total} sentence chunks total ({len(sentences_by_doc)} documents).")
        return sentences_by_doc


def _deduplicate_candidates(
    candidates: list[str],
    model: EmbeddingBackend,
    threshold: float = _CANDIDATE_DEDUP_THRESHOLD,
) -> list[str]:
    """Greedily remove near-duplicate candidates by pairwise cosine similarity.

    Iterates through the candidate list in order. A candidate is kept only if
    its similarity to every already-kept candidate is below *threshold*. This
    catches redundant labels that survive the LLM merge step because the
    tournament stops once the list fits within a single batch.
    """
    if len(candidates) <= 1:
        return candidates

    embs = np.array(
        model.encode(candidates, normalize_embeddings=True, show_progress_bar=False)
    )

    kept_indices: list[int] = [0]
    for i in range(1, len(candidates)):
        kept_embs = embs[kept_indices]
        max_sim = (kept_embs @ embs[i]).max()
        if max_sim < threshold:
            kept_indices.append(i)

    result = [candidates[i] for i in kept_indices]
    n_dropped = len(candidates) - len(result)
    print(
        f"  Candidate dedup (threshold={threshold:.2f}): "
        f"{n_dropped} near-duplicates removed, {len(result)} kept."
    )
    return result


async def _zone3_llm_call(
    candidate: str,
    close_existing: list[str],
    client: LLMBackend,
    semaphore: asyncio.Semaphore,
) -> tuple[str, bool]:
    """Returns (candidate, is_duplicate). Fails open (is_duplicate=False) on any error."""
    async with semaphore:
        try:
            raw = await client.chat(
                [{"role": "user", "content": _zone3_prompt(candidate, close_existing)}],
                max_tokens=10,
            )
            is_duplicate = raw.strip().upper().startswith("YES")
            return candidate, is_duplicate
        except Exception as exc:
            print(f"  [warn] zone3 LLM call failed for '{candidate[:60]}': {exc}")
            return candidate, False


async def _filter_by_embedding_similarity_hybrid(
    new_candidates: list[str],
    existing_labels: list[str],
    model: EmbeddingBackend,
    client: LLMBackend,
    max_concurrent: int = MAX_CONCURRENT,
    low_threshold: float = _LOW_THRESHOLD,
    high_threshold: float = _HIGH_THRESHOLD,
    top_k_context: int = _TOP_K_CONTEXT,
) -> list[str]:
    """Three-zone hybrid filter combining embedding routing with LLM stance judgment.

    Zone 1 (max_sim < low_threshold):  auto-keep  — different topic entirely
    Zone 2 (max_sim > high_threshold):  auto-drop  — near-exact embedding match
    Zone 3 (low_threshold ≤ max_sim ≤ high_threshold): LLM judgment — same topic,
        but the LLM checks whether the claim direction/stance is already covered.
    """
    if not new_candidates:
        return []
    if not existing_labels:
        return new_candidates

    new_embs = model.encode(
        new_candidates, normalize_embeddings=True, show_progress_bar=False
    )
    existing_embs = model.encode(
        existing_labels, normalize_embeddings=True, show_progress_bar=False
    )
    sim_matrix = np.array(new_embs) @ np.array(existing_embs).T
    max_sims = sim_matrix.max(axis=1)

    zone1: list[str] = []
    zone2: list[str] = []
    zone3_candidates: list[str] = []
    zone3_row_indices: list[int] = []

    for i, (candidate, max_sim) in enumerate(zip(new_candidates, max_sims)):
        if max_sim < low_threshold:
            zone1.append(candidate)
        elif max_sim > high_threshold:
            zone2.append(candidate)
        else:
            zone3_candidates.append(candidate)
            zone3_row_indices.append(i)

    print(
        f"  Hybrid filter: zone1 auto-keep={len(zone1)}, "
        f"zone2 auto-drop={len(zone2)}, "
        f"zone3 LLM={len(zone3_candidates)}"
    )

    zone3_kept: list[str] = []
    if zone3_candidates:
        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = []
        for candidate, row_idx in zip(zone3_candidates, zone3_row_indices):
            top_k_idx = np.argsort(sim_matrix[row_idx])[::-1][:top_k_context]
            close_existing = [existing_labels[j] for j in top_k_idx]
            tasks.append(_zone3_llm_call(candidate, close_existing, client, semaphore))

        results = await tqdm.gather(*tasks, desc="Zone3 — LLM stance judgment")
        for candidate, is_duplicate in results:
            if not is_duplicate:
                zone3_kept.append(candidate)

        print(
            f"  Zone3 LLM: {len(zone3_candidates) - len(zone3_kept)} dropped, "
            f"{len(zone3_kept)} kept as distinct claims."
        )

    truly_new = zone1 + zone3_kept
    print(
        f"  Hybrid filter total: {len(new_candidates) - len(truly_new)} dropped, {len(truly_new)} kept."
    )
    return truly_new
