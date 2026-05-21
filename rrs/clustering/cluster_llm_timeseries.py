"""
Timeseries LLM clustering.

Builds a label taxonomy from a one-week warmup window, then for every
subsequent day uses a one-week sliding window to regenerate candidate
clusters. New candidates are compared against the current taxonomy via
sentence-embedding cosine similarity; only candidates that are sufficiently
different (similarity below a configurable threshold) are added.

Pipeline:
  Warmup (first N days, default 7):
    1. Generate narrative labels (Step 1)
    2. Merge/deduplicate labels (Step 2)
    3. Classify warmup docs (Step 3)

  Incremental days (sliding-window approach):
    For each day:
      1b. Generate candidate labels from the past `sliding_window_days` days
      2b. Merge candidates
      3b. Filter candidates by embedding similarity to existing labels
      4b. Add genuinely novel labels to taxonomy
      5b. Classify today's docs against the updated taxonomy
"""

import argparse
import asyncio
import json
import os
from abc import ABC, abstractmethod
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from tqdm.asyncio import tqdm

from rrs.clustering.cluster import (
    ID_COLUMN,
    TEXT_COLUMN,
    load_claims,
    load_from_db,
    split_sentences,
)
from rrs.clustering.cluster_llm import (
    MAX_CONCURRENT,
    MODEL,
    PROVIDER_MISTRAL,
    PROVIDER_ANTHROPIC,
    SEED_LABELS,
    SYSTEM_PROMPT,
    LLMBackend,
    _build_client,
    _build_sentences_by_doc,
    _parse_list_response,
    build_labels_from_transcripts,
    merge_labels,
    classify_all_transcripts,
)

DATE_COLUMN = "start"

_SLIDING_WINDOW_DAYS = 7
_LOW_THRESHOLD = 0.40          # below → auto-keep (zone 1: different topic)
_HIGH_THRESHOLD = 0.85         # above → auto-drop (zone 2: near-exact match)
_TOP_K_CONTEXT = 10            # existing labels shown to LLM per ambiguous candidate
_CANDIDATE_DEDUP_THRESHOLD = 0.85  # pairwise similarity above which candidates are considered redundant
_EMBEDDING_MODEL = "dangvantuan/sentence-camembert-large"
_MISTRAL_EMBED_MODEL = "mistral-embed"
_MISTRAL_EMBED_BATCH_SIZE = 64
_MISTRAL_EMBED_PRICING_PER_M = 0.10  # USD per million input tokens — verify at mistral.ai/technology/

EMBEDDING_BACKEND_ST = "sentence-transformer"
EMBEDDING_BACKEND_MISTRAL = "mistral"


# ---------------------------------------------------------------------------
# Embedding backend abstraction
# ---------------------------------------------------------------------------

class EmbeddingBackend(ABC):
    """Common interface for embedding backends, matching SentenceTransformer.encode()."""

    @abstractmethod
    def encode(
        self,
        texts: list[str],
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
    ) -> np.ndarray:
        ...

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
            self._model.encode(texts, normalize_embeddings=normalize_embeddings, show_progress_bar=show_progress_bar)
        )


class MistralEmbeddingBackend(EmbeddingBackend):
    """Calls mistral-embed via the Mistral API in batches, returns L2-normalised embeddings."""

    def __init__(self, api_key: str, model: str = _MISTRAL_EMBED_MODEL, batch_size: int = _MISTRAL_EMBED_BATCH_SIZE):
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
            batch = texts[i:i + self._batch_size]
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


def build_embedding_backend(
    backend: str = EMBEDDING_BACKEND_ST,
    model_name: str = _EMBEDDING_MODEL,
    mistral_api_key: Optional[str] = None,
) -> EmbeddingBackend:
    """Instantiate the requested embedding backend."""
    if backend == EMBEDDING_BACKEND_MISTRAL:
        if not mistral_api_key:
            raise EnvironmentError("MISTRAL_API_KEY must be set to use the mistral embedding backend.")
        return MistralEmbeddingBackend(api_key=mistral_api_key)
    return SentenceTransformerBackend(model_name)


# ---------------------------------------------------------------------------
# Candidate self-deduplication
# ---------------------------------------------------------------------------

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

    embs = np.array(model.encode(candidates, normalize_embeddings=True, show_progress_bar=False))

    kept_indices: list[int] = [0]
    for i in range(1, len(candidates)):
        kept_embs = embs[kept_indices]          # (n_kept, dim)
        max_sim = (kept_embs @ embs[i]).max()   # scalar
        if max_sim < threshold:
            kept_indices.append(i)

    result = [candidates[i] for i in kept_indices]
    n_dropped = len(candidates) - len(result)
    print(
        f"  Candidate dedup (threshold={threshold:.2f}): "
        f"{n_dropped} near-duplicates removed, {len(result)} kept."
    )
    return result


# ---------------------------------------------------------------------------
# Hybrid embedding + LLM stance filter
# ---------------------------------------------------------------------------

def _zone3_prompt(candidate: str, close_existing: list[str]) -> str:
    labels_block = "\n".join(f"  - {lb}" for lb in close_existing)
    return (
        "You are an expert in French climate discourse analysis.\n"
        "You are given a candidate label and a list of the most semantically close existing labels "
        "from a taxonomy of French climate claims.\n\n"
        f"Candidate label:\n  \"{candidate}\"\n\n"
        f"Closest existing labels:\n{labels_block}\n\n"
        "Decide whether the candidate expresses essentially the SAME claim as at least one of the "
        "existing labels.\n"
        "Reply with a single word, no punctuation:\n"
        "  YES — the candidate is already covered by an existing label\n"
        "  NO  — the candidate expresses a distinct claim\n"
    )


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

    new_embs = model.encode(new_candidates, normalize_embeddings=True, show_progress_bar=False)
    existing_embs = model.encode(existing_labels, normalize_embeddings=True, show_progress_bar=False)
    sim_matrix = np.array(new_embs) @ np.array(existing_embs).T  # (n_new, n_existing)
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
    print(f"  Hybrid filter total: {len(new_candidates) - len(truly_new)} dropped, {len(truly_new)} kept.")
    return truly_new


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _assignments_to_records(
    doc_to_labels: dict,
    label_to_id: dict,
    day: Optional[date] = None,
) -> list[dict]:
    records = []
    for doc_id, labels in doc_to_labels.items():
        for label in labels:
            lid = label_to_id.get(label)
            if lid is not None:
                rec = {"doc_id": doc_id, "label": label, "label_id": lid}
                if day is not None:
                    rec["date"] = day.isoformat()
                records.append(rec)
    return records


def _append_csv(path: Path, records: list[dict], columns: list[str]) -> None:
    if not records:
        return
    df = pd.DataFrame(records, columns=columns)
    df.to_csv(path, mode="a", header=not path.exists(), index=False)


def _save_labels(out: Path, label_list: list[str]) -> dict:
    label_to_id = {label: i for i, label in enumerate(label_list)}
    data = [{"id": i, "label": label} for i, label in enumerate(label_list)]
    (out / "labels.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return label_to_id


def _update_evolution(
    evolution: list[dict],
    new_labels: list[str],
    existing_labels: set[str],
    label_to_id: dict,
    first_seen_date: str,
) -> list[dict]:
    for label in new_labels:
        if label not in existing_labels:
            evolution.append({
                "label": label,
                "label_id": label_to_id.get(label),
                "first_seen_date": first_seen_date,
            })
    return evolution


# ---------------------------------------------------------------------------
# Date / data helpers
# ---------------------------------------------------------------------------

def _slice_by_date(docs_df: pd.DataFrame, target_date: date, date_column: str) -> pd.DataFrame:
    dates = pd.to_datetime(docs_df[date_column], errors="coerce")
    mask = dates.dt.date == target_date
    return docs_df[mask].reset_index(drop=True)


def _slice_by_date_range(
    docs_df: pd.DataFrame,
    start: date,
    end: date,
    date_column: str,
) -> pd.DataFrame:
    dates = pd.to_datetime(docs_df[date_column], errors="coerce")
    mask = (dates.dt.date >= start) & (dates.dt.date <= end)
    return docs_df[mask].reset_index(drop=True)


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run(
    start_date: date,
    end_date: date,
    output_dir: str,
    warmup_days: int = _SLIDING_WINDOW_DAYS,
    input_path: Optional[str] = None,
    text_column: str = "claim_text",
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
    initial_labels: Optional[list[str]] = None,
    skip_merge: bool = False,
    merge_batch_size: int = 30,
    merge_max_rounds: int = 20,
    date_column: str = DATE_COLUMN,
    max_concurrent: int = MAX_CONCURRENT,
    sliding_window_days: int = _SLIDING_WINDOW_DAYS,
    low_threshold: float = _LOW_THRESHOLD,
    high_threshold: float = _HIGH_THRESHOLD,
    top_k_context: int = _TOP_K_CONTEXT,
    candidate_dedup_threshold: float = _CANDIDATE_DEDUP_THRESHOLD,
    embedding_model: str = _EMBEDDING_MODEL,
    save_daily_labels: bool = False,
    provider: str = PROVIDER_MISTRAL,
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    client = _build_client(provider)

    warmup_end = start_date + timedelta(days=warmup_days - 1)
    if warmup_end > end_date:
        warmup_end = end_date
    print(f"Date range      : {start_date} → {end_date}")
    print(f"Warmup          : {start_date} → {warmup_end} ({warmup_days} days)")
    print(f"Incremental     : {warmup_end + timedelta(days=1)} → {end_date}")
    print(f"Sliding window  : {sliding_window_days} days")
    print(f"Low threshold   : {low_threshold}  (auto-keep below)")
    print(f"High threshold  : {high_threshold}  (auto-drop above)")
    print(f"Top-K context   : {top_k_context}  (existing labels shown to LLM per zone-3 candidate)")
    print(f"Candidate dedup : {candidate_dedup_threshold}  (pairwise self-dedup threshold)")
    print(f"Embedding model : {embedding_model}")

    # --- Load all data ---
    if input_path:
        print(f"\nLoading claims from {input_path}...")
        raw_texts = load_claims(input_path, text_column)
        docs_df = pd.DataFrame({TEXT_COLUMN: raw_texts})
        id_col = None
        if date_column not in docs_df.columns:
            raise ValueError(
                f"Local file must contain a '{date_column}' column for timeseries mode."
            )
    else:
        print("\nLoading from database...")
        docs_df = load_from_db()
        id_col = ID_COLUMN if ID_COLUMN in docs_df.columns else None
        print(f"  {len(docs_df)} documents loaded.")

    # Filter to requested date range
    dates_col = pd.to_datetime(docs_df[date_column], errors="coerce")
    mask = (dates_col.dt.date >= start_date) & (dates_col.dt.date <= end_date)
    docs_df = docs_df[mask].reset_index(drop=True)
    print(f"  {len(docs_df)} documents in [{start_date}, {end_date}].")

    # --- Output paths ---
    assignments_cols = ["doc_id", "label", "label_id", "date"]
    assignments_path = out / "transcript_label_assignments.csv"
    evolution_path = out / "label_evolution.json"

    assignments_path.unlink(missing_ok=True)

    # =========================================================================
    # Phase 1 — Warmup
    # =========================================================================
    print(f"\n{'='*60}")
    print(f"WARMUP PHASE: {start_date} → {warmup_end}")
    print(f"{'='*60}")

    warmup_mask = (dates_col.dt.date >= start_date) & (dates_col.dt.date <= warmup_end)
    warmup_df = docs_df[warmup_mask].reset_index(drop=True)
    print(f"  {len(warmup_df)} warmup documents.")

    warmup_sentences = _build_sentences_by_doc(
        warmup_df, id_col, spacy_model, window_size, overlap_tokens
    )

    # Step 1
    print("\nWarmup Step 1: Generating labels from warmup transcripts...")
    generated = await build_labels_from_transcripts(warmup_sentences, client, max_concurrent)

    seeds = list(initial_labels or [])
    seeds_lower = {s.strip().lower() for s in seeds}
    all_labels: list[str] = seeds + [l for l in generated if l.strip().lower() not in seeds_lower]
    print(f"  {len(all_labels)} candidate labels (seeds + generated).")

    # Step 2
    if not skip_merge:
        print("\nWarmup Step 2: Merging labels...")
        all_labels = await merge_labels(
            all_labels, client,
            batch_size=merge_batch_size,
            max_concurrent=max_concurrent,
            max_rounds=merge_max_rounds,
            log_path=out / "labels_merge_progress.json",
        )
        print(f"  {len(all_labels)} labels after merging.")

    label_to_id = _save_labels(out, all_labels)

    evolution: list[dict] = [
        {"label": label, "label_id": i, "first_seen_date": warmup_end.isoformat()}
        for i, label in enumerate(all_labels)
    ]
    evolution_path.write_text(json.dumps(evolution, ensure_ascii=False, indent=2), encoding="utf-8")

    if save_daily_labels:
        (out / f"labels_{warmup_end.isoformat()}.json").write_text(
            (out / "labels.json").read_text(encoding="utf-8"), encoding="utf-8"
        )

    # Step 3 (warmup — classify all warmup docs)
    print("\nWarmup Step 3: Classifying warmup transcripts...")
    warmup_assignments = await classify_all_transcripts(
        warmup_sentences, all_labels, client, max_concurrent
    )
    warmup_records = _assignments_to_records(warmup_assignments, label_to_id, day=warmup_end)
    _append_csv(assignments_path, warmup_records, assignments_cols)
    print(f"  {len(warmup_records)} warmup assignment rows saved.")

    # =========================================================================
    # Phase 2 — Incremental days (sliding-window cluster discovery)
    # =========================================================================
    incremental_start = warmup_end + timedelta(days=1)
    if incremental_start > end_date:
        print("\nNo incremental days — entire range is within warmup window.")
    else:
        print(f"\nLoading embedding model '{embedding_model}'...")
        embed_model = SentenceTransformer(embedding_model)

        for current_day in _date_range(incremental_start, end_date):
            print(f"\n{'='*60}")
            print(f"DAY: {current_day.isoformat()}")
            print(f"{'='*60}")

            # Today's documents for classification
            day_df = _slice_by_date(docs_df, current_day, date_column)
            if day_df.empty:
                print("  No documents for today — skipping.")
                continue
            print(f"  {len(day_df)} documents today.")

            # --- Sliding window: past `sliding_window_days` days including today ---
            window_start = current_day - timedelta(days=sliding_window_days - 1)
            window_df = _slice_by_date_range(docs_df, window_start, current_day, date_column)
            print(f"  Sliding window [{window_start} → {current_day}]: {len(window_df)} documents.")

            window_sentences = _build_sentences_by_doc(
                window_df, id_col, spacy_model, window_size, overlap_tokens
            )
            if not window_sentences:
                print("  No sentence chunks in window — skipping cluster discovery.")
            else:
                # Step 1b — generate candidate labels from the sliding window
                print(f"\n  Step 1b: Generating candidate labels from {len(window_sentences)} window docs...")
                new_candidates = await build_labels_from_transcripts(
                    window_sentences, client, max_concurrent
                )
                # Drop exact name matches to existing labels
                known_lower = {lb.strip().lower() for lb in all_labels}
                new_candidates = [l for l in new_candidates if l.strip().lower() not in known_lower]
                print(f"  → {len(new_candidates)} novel-name candidates after exact-match filter.")

                # Step 2b — merge candidates among themselves
                if new_candidates and not skip_merge:
                    print(f"\n  Step 2b: Merging {len(new_candidates)} candidates...")
                    new_candidates = await merge_labels(
                        new_candidates, client,
                        batch_size=merge_batch_size,
                        max_concurrent=max_concurrent,
                        max_rounds=merge_max_rounds,
                        log_path=out / "labels_merge_progress.json",
                    )
                    # Re-apply exact-match filter after merging
                    new_candidates = [l for l in new_candidates if l.strip().lower() not in known_lower]
                    print(f"  → {len(new_candidates)} candidates after merge.")

                # Step 2c — pairwise self-dedup among candidates
                # Catches redundant labels that survive the LLM merge because
                # the tournament stops once the list fits in a single batch.
                if len(new_candidates) > 1:
                    new_candidates = _deduplicate_candidates(
                        new_candidates, embed_model, candidate_dedup_threshold
                    )

                # Step 3b — hybrid embedding + LLM stance filter
                if new_candidates:
                    truly_new = await _filter_by_embedding_similarity_hybrid(
                        new_candidates,
                        all_labels,
                        embed_model,
                        client,
                        max_concurrent=max_concurrent,
                        low_threshold=low_threshold,
                        high_threshold=high_threshold,
                        top_k_context=top_k_context,
                    )

                    if truly_new:
                        all_labels = list(all_labels) + truly_new
                        label_to_id = _save_labels(out, all_labels)
                        evolution = _update_evolution(
                            evolution, truly_new, {e["label"] for e in evolution},
                            label_to_id, current_day.isoformat()
                        )
                        evolution_path.write_text(
                            json.dumps(evolution, ensure_ascii=False, indent=2), encoding="utf-8"
                        )
                        print(f"  {len(truly_new)} new label(s) added to taxonomy (total: {len(all_labels)}).")
                    else:
                        print("  No genuinely new labels found for today.")

            # Step 4b — classify today's docs against the (possibly updated) taxonomy
            day_sentences = _build_sentences_by_doc(
                day_df, id_col, spacy_model, window_size, overlap_tokens
            )
            if day_sentences:
                print(f"\n  Step 4b: Classifying {len(day_sentences)} docs against {len(all_labels)} labels...")
                day_assignments = await classify_all_transcripts(
                    day_sentences, all_labels, client, max_concurrent
                )
                day_records = _assignments_to_records(day_assignments, label_to_id, day=current_day)
                _append_csv(assignments_path, day_records, assignments_cols)
                print(f"  → {len(day_records)} assignment rows saved.")

            if save_daily_labels:
                (out / f"labels_{current_day.isoformat()}.json").write_text(
                    (out / "labels.json").read_text(encoding="utf-8"), encoding="utf-8"
                )

    print(f"\n{'='*60}")
    print("DONE")
    print(f"  labels.json          : {out / 'labels.json'}")
    print(f"  label_evolution.json : {evolution_path}")
    print(f"  assignments CSV      : {assignments_path}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=(
            "Timeseries LLM clustering — one-week warmup + sliding-window incremental updates "
            "with embedding-based novelty filtering."
        )
    )
    parser.add_argument("--start-date", required=True, metavar="YYYY-MM-DD",
                        help="Start of the date range (inclusive).")
    parser.add_argument("--end-date", required=True, metavar="YYYY-MM-DD",
                        help="End of the date range (inclusive).")
    parser.add_argument("--warmup-days", type=int, default=_SLIDING_WINDOW_DAYS,
                        help=f"Number of days used for the warmup phase. Default: {_SLIDING_WINDOW_DAYS}.")
    parser.add_argument("--sliding-window-days", type=int, default=_SLIDING_WINDOW_DAYS,
                        help=f"Sliding window size (days) for incremental cluster discovery. Default: {_SLIDING_WINDOW_DAYS}.")
    parser.add_argument("--low-threshold", type=float, default=_LOW_THRESHOLD,
                        help=(
                            f"Embedding similarity below which a candidate is auto-kept (zone 1: "
                            f"different topic). Default: {_LOW_THRESHOLD}."
                        ))
    parser.add_argument("--high-threshold", type=float, default=_HIGH_THRESHOLD,
                        help=(
                            f"Embedding similarity above which a candidate is auto-dropped (zone 2: "
                            f"near-exact match). Default: {_HIGH_THRESHOLD}."
                        ))
    parser.add_argument("--top-k-context", type=int, default=_TOP_K_CONTEXT,
                        help=(
                            f"Number of closest existing labels shown to the LLM per zone-3 candidate. "
                            f"Default: {_TOP_K_CONTEXT}."
                        ))
    parser.add_argument("--candidate-dedup-threshold", type=float, default=_CANDIDATE_DEDUP_THRESHOLD,
                        help=(
                            f"Pairwise cosine similarity above which two candidates are considered "
                            f"redundant — the later one is dropped. Applied after Step 2b merge. "
                            f"Default: {_CANDIDATE_DEDUP_THRESHOLD}."
                        ))
    parser.add_argument("--embedding-model", default=_EMBEDDING_MODEL,
                        help=f"SentenceTransformer model for similarity filtering. Default: {_EMBEDDING_MODEL}.")
    parser.add_argument("--output-dir", default="./timeseries_output",
                        help="Directory for output files. Default: ./timeseries_output.")
    parser.add_argument("--input",
                        help="Path to a local claims file (.json or .csv). "
                             "If omitted, loads from the database.")
    parser.add_argument("--text-column", default="claim_text",
                        help="Column/key for claim text in a local file. Default: claim_text.")
    parser.add_argument("--date-column", default=DATE_COLUMN,
                        help=f"Dataset column that holds the date. Default: {DATE_COLUMN}.")
    parser.add_argument("--spacy-model", default="fr_core_news_sm")
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--overlap-tokens", type=int, default=30)
    parser.add_argument("--provider",
                        choices=[PROVIDER_MISTRAL, PROVIDER_ANTHROPIC],
                        default=PROVIDER_MISTRAL,
                        help=f"LLM provider to use. Default: {PROVIDER_MISTRAL}.")
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT,
                        help=f"Max parallel LLM requests. Default: {MAX_CONCURRENT}.")
    parser.add_argument("--initial-labels-file",
                        help="Path to a JSON list of seed labels. Overrides built-in SEED_LABELS.")
    parser.add_argument("--no-seeds", action="store_true",
                        help="Start with no initial labels.")
    parser.add_argument("--skip-merge", action="store_true",
                        help="Skip the label-merging step.")
    parser.add_argument("--merge-batch-size", type=int, default=10,
                        help="Number of labels per merge call. Default: 10.")
    parser.add_argument("--merge-max-rounds", type=int, default=10,
                        help="Maximum hierarchical merge rounds. Default: 10.")
    parser.add_argument("--save-daily-labels", action="store_true",
                        help="Save a per-day labels_<date>.json snapshot.")

    args = parser.parse_args()

    if args.no_seeds:
        initial = []
    elif args.initial_labels_file:
        initial = json.loads(Path(args.initial_labels_file).read_text(encoding="utf-8"))
    else:
        initial = SEED_LABELS

    asyncio.run(run(
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        warmup_days=args.warmup_days,
        output_dir=args.output_dir,
        input_path=args.input,
        text_column=args.text_column,
        spacy_model=args.spacy_model,
        window_size=args.window_size,
        overlap_tokens=args.overlap_tokens,
        initial_labels=initial,
        skip_merge=args.skip_merge,
        merge_batch_size=args.merge_batch_size,
        merge_max_rounds=args.merge_max_rounds,
        date_column=args.date_column,
        max_concurrent=args.max_concurrent,
        sliding_window_days=args.sliding_window_days,
        low_threshold=args.low_threshold,
        high_threshold=args.high_threshold,
        top_k_context=args.top_k_context,
        candidate_dedup_threshold=args.candidate_dedup_threshold,
        embedding_model=args.embedding_model,
        save_daily_labels=args.save_daily_labels,
        provider=args.provider,
    ))
