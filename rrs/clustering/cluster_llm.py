"""
LLM-based text clustering.

Three-step pipeline:
  1. Generate narrative labels per transcript — async, no shared label list in prompt
  2. Merge/deduplicate label list — single LLM call
  3. Classify each transcript against final labels — async
  4. Save results

Supports two LLM providers selectable via --provider:
  mistral   — Mistral Small (default)
  anthropic — Claude Haiku 4.5
"""

import argparse
import ast
import asyncio
import json
import math
import os
import random
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import anthropic
import pandas as pd
from dotenv import load_dotenv
from mistralai import Mistral
from transformers import AutoTokenizer
from tqdm.asyncio import tqdm

from rrs.clustering.cluster import (
    ID_COLUMN,
    TEXT_COLUMN,
    load_from_db,
    split_sentences,
)
from rrs.clustering.get_data import write_clusters_to_db
from rrs.utils.generate_id import get_consistent_hash

PROVIDER_MISTRAL = "mistral"
PROVIDER_ANTHROPIC = "anthropic"

MODEL_MISTRAL = "mistral-small-2506"
MODEL_ANTHROPIC = "claude-haiku-4-5-20251001"

# Keep for backwards compatibility
MODEL = MODEL_MISTRAL

MAX_CONCURRENT = 1

SYSTEM_PROMPT = "You are an assistant helping editors to aggregate claims on climate change discussions."

SEED_LABELS: list[str] = [
    "Les renouvelables augmentent le coût de l'électricité",
    "Il est inutile de réduire les rejets de gaz à effet de serre de la France",
    "Les populations sont manipulées de façon injustifiée.",
    "L'élevage est neutre voire avantageux pour le climat",
    "Les voitures électriques polluent plus que les voitures thermiques",
]


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


# ---------------------------------------------------------------------------
# Token / cost estimation
# ---------------------------------------------------------------------------

# USD per million tokens — update if providers change pricing.
# Mistral: https://mistral.ai/technology/
# Anthropic: https://www.anthropic.com/pricing
_PRICING: dict[str, dict[str, float]] = {
    PROVIDER_MISTRAL: {"input": 0.15, "output": 0.60},
    PROVIDER_ANTHROPIC: {"input": 0.80, "output": 4.00},
}

_AVG_OUTPUT_TOKENS_PER_DOC = 50  # conservative estimate: a few short label strings
_TOKENIZER_REPO = "mistralai/Mistral-Small-3.2-24B-Instruct-2506"
_tokenizer = None


def _get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        _tokenizer = AutoTokenizer.from_pretrained(_TOKENIZER_REPO)
    return _tokenizer


def _estimate_tokens(text: str) -> int:
    return max(1, len(_get_tokenizer().encode(text)))


def _step1_prompt(sentences: list[str]) -> str:
    return (
        "Given these sentences from a news transcript, identify "
        "narrative(s)/concepts they express. Generate a concise, meaningful label for each distinct "
        "narrative present. Limit yourself to 1 or 2 labels.\n"
        "Rules:\n"
        "- Return ONLY a JSON list of label strings with double quotes, e.g. [\"label 1\", \"label 2\"]. No code fences.\n"
        "- Labels must describe specific claims, not generic categories.\n"
        "- Do NOT return meaningless names such as 'new_label_1' or 'unknown_topic'.\n"
        "- If no climate misinformation is present, return only []. Nothing else.\n"
        "- The labels must be in french.\n"
        f"Sentences: {sentences}"
    )


def _pricing_note(provider: str) -> str:
    p = _PRICING.get(provider, _PRICING[PROVIDER_MISTRAL])
    ref = "mistral.ai/technology/" if provider == PROVIDER_MISTRAL else "anthropic.com/pricing"
    return f"rates: ${p['input']}/M input, ${p['output']}/M output — verify at {ref}"


def _cost(total_input: float, total_output: float, provider: str) -> float:
    p = _PRICING.get(provider, _PRICING[PROVIDER_MISTRAL])
    return (total_input / 1_000_000) * p["input"] + (total_output / 1_000_000) * p["output"]


def estimate_step1_tokens(
    sentences_by_doc: dict,
    provider: str = PROVIDER_MISTRAL,
    sample_size: int = 20,
) -> None:
    """Sample documents, estimate input tokens locally, and print a cost estimate."""
    doc_ids = list(sentences_by_doc.keys())
    n_docs = len(doc_ids)
    sample = random.sample(doc_ids, min(sample_size, n_docs))

    token_counts: list[int] = []
    for doc_id in sample:
        prompt_text = SYSTEM_PROMPT + _step1_prompt(sentences_by_doc[doc_id])
        token_counts.append(_estimate_tokens(prompt_text))

    avg_input = sum(token_counts) / len(token_counts)
    total_input = avg_input * n_docs
    total_output = _AVG_OUTPUT_TOKENS_PER_DOC * n_docs
    total_cost = _cost(total_input, total_output, provider)

    print(f"\n--- Step 1 token estimate ({len(sample)}/{n_docs} docs sampled) [{provider}] ---")
    print(f"  Input tokens/doc : avg {avg_input:,.0f}  (min {min(token_counts):,}  max {max(token_counts):,})")
    print(f"  Total input tokens: ~{total_input:,.0f}")
    print(f"  Total output tokens: ~{total_output:,.0f}  (est. {_AVG_OUTPUT_TOKENS_PER_DOC} per doc)")
    print(f"  Estimated cost: ~${total_cost:.4f}  ({_pricing_note(provider)})")


def estimate_step2_tokens(
    label_list: list[str],
    batch_size: int = 30,
    provider: str = PROVIDER_MISTRAL,
) -> None:
    """Estimate step 2 cost by simulating hierarchical rounds, assuming 50% reduction per round."""
    n = len(label_list)
    rounds: list[int] = []
    while n > batch_size:
        rounds.append(math.ceil(n / batch_size))
        n = max(1, n // 2)
    rounds.append(1)

    total_calls = sum(rounds)
    sample_labels = label_list[:min(batch_size, len(label_list))]
    tokens_per_call = _estimate_tokens(SYSTEM_PROMPT + _step2_prompt(sample_labels))

    total_input = total_calls * tokens_per_call
    total_output = total_calls * (tokens_per_call // 2)
    total_cost = _cost(total_input, total_output, provider)

    print(f"\n--- Step 2 token estimate ({len(rounds)} rounds, calls per round: {rounds}) [{provider}] ---")
    print(f"  Total calls: {total_calls}")
    print(f"  Total input tokens: ~{total_input:,.0f}")
    print(f"  Total output tokens: ~{total_output:,.0f}")
    print(f"  Estimated cost: ~${total_cost:.4f}  ({_pricing_note(provider)})")


def estimate_step3_tokens(
    sentences_by_doc: dict,
    label_list: list[str],
    provider: str = PROVIDER_MISTRAL,
    sample_size: int = 20,
) -> None:
    """Estimate step 3 cost by sampling documents and estimating tokens locally."""
    doc_ids = list(sentences_by_doc.keys())
    n_docs = len(doc_ids)
    sample = random.sample(doc_ids, min(sample_size, n_docs))

    token_counts: list[int] = []
    for doc_id in sample:
        prompt_text = SYSTEM_PROMPT + _step3_prompt(sentences_by_doc[doc_id], label_list)
        token_counts.append(_estimate_tokens(prompt_text))

    avg_input = sum(token_counts) / len(token_counts)
    total_input = avg_input * n_docs
    total_output = _AVG_OUTPUT_TOKENS_PER_DOC * n_docs
    total_cost = _cost(total_input, total_output, provider)

    print(f"\n--- Step 3 token estimate ({len(sample)}/{n_docs} docs sampled) [{provider}] ---")
    print(f"  Input tokens/doc : avg {avg_input:,.0f}  (min {min(token_counts):,}  max {max(token_counts):,})")
    print(f"  Total input tokens: ~{total_input:,.0f}")
    print(f"  Total output tokens: ~{total_output:,.0f}  (est. {_AVG_OUTPUT_TOKENS_PER_DOC} per doc)")
    print(f"  Estimated cost: ~${total_cost:.4f}  ({_pricing_note(provider)})")


# ---------------------------------------------------------------------------
# Step 1 — generate labels per transcript (async, no shared label list)
# ---------------------------------------------------------------------------

async def _step1_call(
    doc_id: str,
    sentences: list[str],
    backend: LLMBackend,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[str]]:
    async with semaphore:
        try:
            raw = await backend.chat([{"role": "user", "content": _step1_prompt(sentences)}], max_tokens=512)
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
        *[_step1_call(doc_id, sentences_by_doc[doc_id], client, semaphore) for doc_id in doc_ids],
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


# ---------------------------------------------------------------------------
# Step 2 — merge/deduplicate labels (single LLM call)
# ---------------------------------------------------------------------------

def _step2_prompt(label_list: list[str]) -> str:
    return (
        "You are merging a list of French climate-discussion labels into a shorter, cleaner list.\n"
        "Group labels that share the same core subject and overall message, even if the wording differs.\n"
        "Be AGGRESSIVE: if several labels all make a similar point about the same topic, collapse them into one.\n"
        "Example: 'Le nucléaire est essentiel pour la décarbonation', "
        "'Le nucléaire est la seule énergie propre', 'Le nucléaire est préférable aux renouvelables' "
        "→ should all merge into a single label like "
        "'Le nucléaire est une énergie décarbonée supérieure aux renouvelables'.\n"
        "Rules:\n"
        "- Merge any labels that share the same subject AND a closely related claim AND are on the same side of a debate.\n"
        "- Write the merged label as a short, conversational French sentence starting with its subject.\n"
        "- Prefer fewer, broader labels over many narrow ones.\n"
        "- Do NOT keep two labels if they could reasonably be covered by one.\n"
        f"Here is the list of labels:\n{label_list}.\n"
        "Produce the final merged list as a JSON array in French, using double quotes. No code fences."
    )


async def _merge_labels_call(label_list: list[str], client: LLMBackend) -> list[str]:
    """Merge the flat *label_list*, returning a deduplicated result.

    Returns the input list unchanged if the response cannot be parsed.
    """
    raw = await client.chat([{"role": "user", "content": _step2_prompt(label_list)}], max_tokens=4096)
    parsed = _parse_list_response(raw)
    if not parsed:
        print(f"  [warn] unparseable merge response — keeping {len(label_list)} labels unchanged.")
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
    Stops when the label count reaches *target_clusters* (or 3*batch_size if not
    set) or *max_rounds* is reached, then makes one final merge call.
    """
    labels = list(label_list)
    round_num = 0
    print(f"  Merge target: ≤{target_clusters} labels (from {len(labels)} candidates)")

    while len(labels) > target_clusters and round_num < max_rounds:
        round_num += 1
        random.shuffle(labels)
        chunks = [labels[i:i + batch_size] for i in range(0, len(labels), batch_size)]
        print(f"  Round {round_num}: {len(labels)} labels → {len(chunks)} parallel merge calls")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def _merge_with_semaphore(chunk: list[str]) -> list[str]:
            async with semaphore:
                return await _merge_labels_call(chunk, client)

        results = await asyncio.gather(*[_merge_with_semaphore(c) for c in chunks])
        labels = [label for group in results for label in group]
        print(f"    → {len(labels)} labels after round {round_num}")

        if log_path is not None:
            log_path.write_text(
                json.dumps(sorted(labels), ensure_ascii=False, indent=2), encoding="utf-8"
            )

    # Final single call once everything fits in one batch
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

def _step3_prompt(sentences: list[str], label_list: list[str]) -> str:
    return (
        "Given the label list and the sentences, select all labels that describe "
        "the concepts expressed in the sentences.\n"
        f"Label list: {label_list}\n"
        f"Sentences: {sentences}\n"
        "Return ONLY a JSON array of matching label strings using double quotes starting with "
        "'[' and anding with ']'. No code fences."
        "If no labels match the sentences return '[]'."
    )


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
                [{"role": "user", "content": _step3_prompt(sentences, label_list)}], max_tokens=512
            )
            matched = _parse_list_response(raw)
            label_lower = {l.strip().lower(): l for l in label_list}
            valid = [label_lower[m.strip().lower()] for m in matched if m.strip().lower() in label_lower]
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

_OUTPUT_COLUMNS = ["case_id", "segment_id", "start", "text", "cluster_id", "cluster_text"]


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
            docs_df, text_column=TEXT_COLUMN, spacy_model=spacy_model,
            window_size=window_size, overlap_tokens=overlap_tokens,
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
                tmp, text_column=TEXT_COLUMN, spacy_model=spacy_model,
                window_size=window_size, overlap_tokens=overlap_tokens,
            )
            sentences_by_doc[str(i)] = s_df["sentence"].tolist()
        total = sum(len(v) for v in sentences_by_doc.values())
        print(f"  {total} sentence chunks total ({len(sentences_by_doc)} documents).")
        return sentences_by_doc


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run(
    output_dir: str,
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
    initial_labels: list[str] = None,
    skip_merge: bool = False,
    merge_batch_size: int = 30,
    merge_max_rounds: int = 20,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_concurrent: int = MAX_CONCURRENT,
    provider: str = PROVIDER_MISTRAL,
    target_clusters: Optional[int] = None,
    min_clusters: int = 5,
    max_clusters: int = 150,
    cluster_scale: float = 1.0,
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    client = _build_client(provider)

    # --- Load data ---
    print(f"Loading texts from database [{start_date} → {end_date}]...")
    docs_df = load_from_db(start_date=start_date, end_date=end_date)
    print(f"  {len(docs_df)} documents loaded.")
    id_col = ID_COLUMN if ID_COLUMN in docs_df.columns else None

    # --- Split all transcripts into sentences, grouped by document ---
    print(f"Splitting transcripts into sentences (spaCy: {spacy_model}, "
          f"window={window_size}, overlap={overlap_tokens})...")
    sentences_by_doc = _build_sentences_by_doc(
        docs_df, id_col, spacy_model, window_size, overlap_tokens
    )

    # --- Step 1: token estimate, then generate labels ---
    print(f"\nStep 1: Estimating token usage for {len(sentences_by_doc)} transcripts...")
    estimate_step1_tokens(sentences_by_doc, provider=provider)
    print(f"\nStep 1: Generating narrative labels (max_concurrent={max_concurrent})...")
    generated_labels = await build_labels_from_transcripts(sentences_by_doc, client, max_concurrent)

    # Merge seed labels with generated ones (dedup)
    seeds = list(initial_labels or [])
    seeds_lower = {s.strip().lower() for s in seeds}
    all_labels = seeds + [l for l in generated_labels if l.strip().lower() not in seeds_lower]
    print(f"  {len(all_labels)} candidate labels (seeds + generated).")

    (out / "labels_raw.json").write_text(
        json.dumps(all_labels, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # --- Step 2: merge similar labels, assign IDs ---
    if not skip_merge:
        effective_target = target_clusters if target_clusters is not None else compute_target_clusters(
            len(sentences_by_doc), min_clusters=min_clusters, max_clusters=max_clusters, scale_factor=cluster_scale
        )
        print(f"\nStep 2: Estimating token usage for {len(all_labels)} labels...")
        print(f"  Adaptive cluster target: {effective_target} (n_docs={len(sentences_by_doc)})")
        estimate_step2_tokens(all_labels, batch_size=merge_batch_size, provider=provider)
        print("\nStep 2: Merging semantically similar labels...")
        all_labels = await merge_labels(all_labels, client, batch_size=merge_batch_size, max_concurrent=max_concurrent, max_rounds=merge_max_rounds, log_path=out / "labels_merge_progress.json", target_clusters=effective_target)
        print(f"  {len(all_labels)} labels after merging.")

    label_to_id = {label: get_consistent_hash(label) for label in all_labels}
    labels_data = [{"id": get_consistent_hash(label), "label": label} for label in all_labels]
    labels_path = out / "labels.json"
    labels_path.write_text(json.dumps(labels_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Final labels with IDs saved to {labels_path}")

    # --- Step 3: classify all transcripts against final labels ---
    print(f"\nStep 3: Estimating token usage for {len(sentences_by_doc)} transcripts with {len(all_labels)} labels...")
    estimate_step3_tokens(sentences_by_doc, all_labels, provider=provider)
    print(f"\nStep 3: Classifying {len(sentences_by_doc)} transcripts (max_concurrent={max_concurrent})...")
    doc_to_labels = await classify_all_transcripts(sentences_by_doc, all_labels, client, max_concurrent)

    records = []
    for doc_id, matched_labels in doc_to_labels.items():
        for label in matched_labels:
            label_id = label_to_id.get(label)
            if label_id is not None:
                records.append({ID_COLUMN: doc_id, "cluster_id": label_id, "cluster_text": label})

    assignments_df = pd.DataFrame(records) if records else pd.DataFrame(columns=[ID_COLUMN, "cluster_id", "cluster_text"])

    if id_col and not assignments_df.empty:
        meta_cols = [c for c in _OUTPUT_COLUMNS if c in docs_df.columns and c != ID_COLUMN]
        meta_df = docs_df[[ID_COLUMN] + meta_cols].drop_duplicates(subset=[ID_COLUMN]).copy()
        assignments_df = assignments_df.merge(meta_df, on=ID_COLUMN, how="left")

    for col in _OUTPUT_COLUMNS:
        if col not in assignments_df.columns:
            assignments_df[col] = None
    assignments_df = assignments_df[_OUTPUT_COLUMNS]

    assignments_path = out / "transcript_label_assignments.csv"
    assignments_df.to_csv(assignments_path, index=False)
    print(f"\nTranscript-label assignments saved to {assignments_path}")

    print("\nWriting results to database...")
    write_clusters_to_db(assignments_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "LLM-based text clustering — transcript-level narrative matching (3-step pipeline). "
            f"Supports providers: {PROVIDER_MISTRAL} (default), {PROVIDER_ANTHROPIC}."
        )
    )
    parser.add_argument("--spacy-model", default="fr_core_news_sm")
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--overlap-tokens", type=int, default=30)
    parser.add_argument(
        "--provider",
        choices=[PROVIDER_MISTRAL, PROVIDER_ANTHROPIC],
        default=PROVIDER_MISTRAL,
        help=f"LLM provider to use. Default: {PROVIDER_MISTRAL}.",
    )
    parser.add_argument(
        "--max-concurrent", type=int, default=MAX_CONCURRENT,
        help=f"Max parallel LLM requests for steps 1 and 3. Default: {MAX_CONCURRENT}",
    )
    parser.add_argument(
        "--initial-labels-file",
        help="Path to a JSON list of seed labels. Overrides built-in SEED_LABELS.",
    )
    parser.add_argument(
        "--no-seeds", action="store_true",
        help="Start with no initial labels; let the LLM generate all labels from scratch.",
    )
    parser.add_argument(
        "--skip-merge", action="store_true",
        help="Skip the label-merging step (step 2).",
    )
    parser.add_argument(
        "--merge-batch-size", type=int, default=30,
        help="Number of labels per merge call in step 2. Default: 30.",
    )
    parser.add_argument(
        "--merge-max-rounds", type=int, default=20,
        help="Maximum number of hierarchical merge rounds in step 2. Default: 20.",
    )
    parser.add_argument(
        "--target-clusters", type=int, default=None,
        help="Explicit target cluster count; overrides adaptive default (sqrt(n_docs)).",
    )
    parser.add_argument(
        "--min-clusters", type=int, default=5,
        help="Minimum clusters when using adaptive target. Default: 5.",
    )
    parser.add_argument(
        "--max-clusters", type=int, default=150,
        help="Maximum clusters when using adaptive target. Default: 150.",
    )
    parser.add_argument(
        "--cluster-scale", type=float, default=1.0,
        help="Multiplier on sqrt(n_docs) for adaptive cluster target. Default: 1.0.",
    )
    parser.add_argument(
        "--output-dir", default="./bertopic_llm_output",
        help="Directory for output files. Default: ./bertopic_llm_output",
    )
    parser.add_argument(
        "--start-date", default=None, metavar="YYYY-MM-DD",
        help="Keep only records on or after this date (inclusive). Default: no lower bound.",
    )
    parser.add_argument(
        "--end-date", default=None, metavar="YYYY-MM-DD",
        help="Keep only records on or before this date (inclusive). Default: no upper bound.",
    )
    args = parser.parse_args()

    if args.no_seeds:
        initial = []
    elif args.initial_labels_file:
        initial = json.loads(Path(args.initial_labels_file).read_text(encoding="utf-8"))
    else:
        initial = SEED_LABELS

    asyncio.run(run(
        output_dir=args.output_dir,
        spacy_model=args.spacy_model,
        window_size=args.window_size,
        overlap_tokens=args.overlap_tokens,
        initial_labels=initial,
        skip_merge=args.skip_merge,
        merge_batch_size=args.merge_batch_size,
        merge_max_rounds=args.merge_max_rounds,
        start_date=date.fromisoformat(args.start_date) if args.start_date else None,
        end_date=date.fromisoformat(args.end_date) if args.end_date else None,
        max_concurrent=args.max_concurrent,
        provider=args.provider,
        target_clusters=args.target_clusters,
        min_clusters=args.min_clusters,
        max_clusters=args.max_clusters,
        cluster_scale=args.cluster_scale,
    ))
