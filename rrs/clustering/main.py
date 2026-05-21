"""
LLM-based text clustering — v2: DB-aware deduplication.

Same three-step pipeline as cluster_llm.py, with an additional step between
Steps 2 and 3:

  2b. Load active clusters from the database. A cluster is considered active
      if it has had at least one case assigned within the last `expiry_days`
      days before `start_date` (default 30). Stale clusters are excluded so
      they do not suppress genuinely new labels.
  2c. Compare new clusters against the active existing ones via embedding
      similarity (hybrid three-zone filter reused from cluster_llm_timeseries).
      Near-duplicates are dropped.
  2d. The final label set for Step 3 is: surviving new labels + all active DB
      labels. Only truly new labels are written back to the database.
"""

import argparse
import asyncio
import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from rrs.clustering.backends import (
    _EMBEDDING_MODEL,
    EMBEDDING_BACKEND_MISTRAL,
    EMBEDDING_BACKEND_ST,
    MAX_CONCURRENT,
    EmbeddingBackend,
    LLMBackend,
)
from rrs.clustering.cost import (
    _cost,
    estimate_step1_tokens,
    estimate_step2_tokens,
    estimate_step3_tokens,
)
from rrs.clustering.get_data import (
    ID_COLUMN,
    get_clusters_from_db,
    get_latest_clustered_date,
    get_unprocessed_dates,
    load_from_db,
    write_clusters_to_db,
)
from rrs.clustering.providers import PROVIDER_ANTHROPIC, PROVIDER_MISTRAL
from rrs.clustering.steps import (
    _HIGH_THRESHOLD,
    _LOW_THRESHOLD,
    SEED_LABELS,
    _build_client,
    _build_sentences_by_doc,
    _deduplicate_candidates,
    _filter_by_embedding_similarity_hybrid,
    build_embedding_backend,
    build_labels_from_transcripts,
    classify_all_transcripts,
    compute_target_clusters,
    merge_labels,
)
from rrs.utils.generate_id import get_consistent_hash

_OUTPUT_COLUMNS = [
    "case_id",
    "segment_id",
    "start",
    "text",
    "cluster_id",
    "cluster_text",
]


async def _run_day(
    run_date: date,
    out: Optional[Path],
    client: "LLMBackend",
    emb_model: "EmbeddingBackend",
    spacy_model: str,
    window_size: int,
    overlap_tokens: int,
    initial_labels: list[str],
    skip_merge: bool,
    merge_batch_size: int,
    merge_max_rounds: int,
    max_concurrent: int,
    provider: str,
    target_clusters: Optional[int],
    min_clusters: int,
    max_clusters: int,
    cluster_scale: float,
    low_threshold: float,
    high_threshold: float,
    expiry_days: int,
) -> None:
    """Run the full clustering pipeline for a single day."""
    if out is not None:
        out.mkdir(parents=True, exist_ok=True)

    # --- Load cases for this day ---
    print(f"\nLoading texts from database for {run_date}...")
    docs_df = load_from_db(start_date=run_date, end_date=run_date)
    print(f"  {len(docs_df)} documents loaded.")
    if docs_df.empty:
        print("  No documents for this day — skipping.")
        return
    id_col = ID_COLUMN if ID_COLUMN in docs_df.columns else None

    # --- Split into sentences ---
    sentences_by_doc = _build_sentences_by_doc(
        docs_df, id_col, spacy_model, window_size, overlap_tokens
    )

    # --- Step 1: generate labels ---
    print(
        f"\nStep 1: Estimating token usage for {len(sentences_by_doc)} transcripts..."
    )
    estimate_step1_tokens(sentences_by_doc, provider=provider)
    print(f"\nStep 1: Generating narrative labels (max_concurrent={max_concurrent})...")
    generated_labels = await build_labels_from_transcripts(
        sentences_by_doc, client, max_concurrent
    )

    seeds = list(initial_labels or [])
    seeds_lower = {s.strip().lower() for s in seeds}
    all_labels = seeds + [
        l for l in generated_labels if l.strip().lower() not in seeds_lower
    ]
    print(f"  {len(all_labels)} candidate labels (seeds + generated).")
    if out is not None:
        (out / "labels_raw.json").write_text(
            json.dumps(all_labels, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # --- Step 2: merge similar labels ---
    if not skip_merge:
        effective_target = (
            target_clusters
            if target_clusters is not None
            else compute_target_clusters(
                len(sentences_by_doc),
                min_clusters=min_clusters,
                max_clusters=max_clusters,
                scale_factor=cluster_scale,
            )
        )
        print(f"\nStep 2: Estimating token usage for {len(all_labels)} labels...")
        print(
            f"  Adaptive cluster target: {effective_target} (n_docs={len(sentences_by_doc)})"
        )
        estimate_step2_tokens(
            all_labels, batch_size=merge_batch_size, provider=provider
        )
        print("\nStep 2: Merging semantically similar labels...")
        all_labels = await merge_labels(
            all_labels,
            client,
            target_clusters=effective_target,
            batch_size=merge_batch_size,
            max_concurrent=max_concurrent,
            max_rounds=merge_max_rounds,
            log_path=out / "labels_merge_progress.json" if out is not None else None,
        )
        print(f"  {len(all_labels)} labels after merging.")
    if out is not None:
        (out / "labels_merged.json").write_text(
            json.dumps(sorted(all_labels), ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # --- Step 2b: load active clusters from DB ---
    active_since = run_date - timedelta(days=expiry_days)
    print(
        f"\nStep 2b: Loading active clusters from database "
        f"(active since {active_since}, {expiry_days}d before {run_date})..."
    )
    existing_df = get_clusters_from_db(active_since=active_since)
    existing_labels: list[str] = existing_df["cluster_text"].dropna().tolist()
    existing_ids: dict[str, str] = dict(
        zip(existing_df["cluster_text"], existing_df["cluster_id"])
    )
    print(f"  {len(existing_labels)} existing clusters found in DB.")

    # --- Step 2c: filter new labels against existing ones ---
    print(
        f"\nStep 2c: Comparing new labels against DB clusters "
        f"(low={low_threshold}, high={high_threshold})..."
    )
    all_labels = _deduplicate_candidates(all_labels, emb_model)
    if existing_labels:
        truly_new_labels = await _filter_by_embedding_similarity_hybrid(
            new_candidates=all_labels,
            existing_labels=existing_labels,
            model=emb_model,
            client=client,
            max_concurrent=max_concurrent,
            low_threshold=low_threshold,
            high_threshold=high_threshold,
        )
    else:
        print("  No existing clusters — all new labels are kept.")
        truly_new_labels = all_labels
    print(f"  {len(truly_new_labels)} genuinely new labels after DB deduplication.")
    if out is not None:
        (out / "labels_new.json").write_text(
            json.dumps(sorted(truly_new_labels), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # --- Step 2d: build combined label set for classification ---
    combined_labels = existing_labels + truly_new_labels
    print(
        f"\nStep 2d: Combined label set: {len(existing_labels)} existing + "
        f"{len(truly_new_labels)} new = {len(combined_labels)} total."
    )

    label_to_id: dict[str, str] = {**existing_ids}
    new_labels_data = []
    for label in truly_new_labels:
        lid = get_consistent_hash(label)
        label_to_id[label] = lid
        new_labels_data.append({"id": lid, "label": label})
    if out is not None:
        (out / "labels_new_with_ids.json").write_text(
            json.dumps(new_labels_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # --- Step 3: classify all transcripts against combined label set ---
    print(
        f"\nStep 3: Estimating token usage for {len(sentences_by_doc)} transcripts "
        f"with {len(combined_labels)} labels..."
    )
    estimate_step3_tokens(sentences_by_doc, combined_labels, provider=provider)
    print(
        f"\nStep 3: Classifying {len(sentences_by_doc)} transcripts (max_concurrent={max_concurrent})..."
    )
    doc_to_labels = await classify_all_transcripts(
        sentences_by_doc, combined_labels, client, max_concurrent
    )

    records = []
    for doc_id, matched_labels in doc_to_labels.items():
        for label in matched_labels:
            label_id = label_to_id.get(label)
            if label_id is not None:
                records.append(
                    {ID_COLUMN: doc_id, "cluster_id": label_id, "cluster_text": label}
                )

    assignments_df = (
        pd.DataFrame(records)
        if records
        else pd.DataFrame(columns=[ID_COLUMN, "cluster_id", "cluster_text"])
    )
    if id_col and not assignments_df.empty:
        meta_cols = [
            c for c in _OUTPUT_COLUMNS if c in docs_df.columns and c != ID_COLUMN
        ]
        meta_df = (
            docs_df[[ID_COLUMN] + meta_cols].drop_duplicates(subset=[ID_COLUMN]).copy()
        )
        assignments_df = assignments_df.merge(meta_df, on=ID_COLUMN, how="left")
    for col in _OUTPUT_COLUMNS:
        if col not in assignments_df.columns:
            assignments_df[col] = None
    assignments_df = assignments_df[_OUTPUT_COLUMNS]

    if out is not None:
        assignments_path = out / "transcript_label_assignments.csv"
        assignments_df.to_csv(assignments_path, index=False)
        print(f"\nTranscript-label assignments saved to {assignments_path}")

    print("\nWriting results to database...")
    write_clusters_to_db(assignments_df)


async def run(
    output_dir: Optional[str] = None,
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
    initial_labels: list[str] = None,
    skip_merge: bool = False,
    merge_batch_size: int = 30,
    merge_max_rounds: int = 20,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    only_recent: bool = True,
    max_concurrent: int = MAX_CONCURRENT,
    provider: str = PROVIDER_MISTRAL,
    target_clusters: Optional[int] = None,
    min_clusters: int = 5,
    max_clusters: int = 150,
    cluster_scale: float = 1.0,
    low_threshold: float = _LOW_THRESHOLD,
    high_threshold: float = _HIGH_THRESHOLD,
    embedding_backend: str = EMBEDDING_BACKEND_ST,
    embedding_model: str = _EMBEDDING_MODEL,
    expiry_days: int = 30,
) -> None:
    # Build shared resources once across all days
    client = _build_client(provider)
    mistral_api_key = (
        os.getenv("MISTRAL_API_KEY")
        if embedding_backend == EMBEDDING_BACKEND_MISTRAL
        else None
    )
    emb_model = build_embedding_backend(
        embedding_backend, model_name=embedding_model, mistral_api_key=mistral_api_key
    )

    # Resolve date range
    if start_date is None:
        print("No start date provided — querying DB for unprocessed days...")
        days = get_unprocessed_dates()
        if only_recent:
            latest_clustered = get_latest_clustered_date()
            if latest_clustered is not None:
                print(f"  Filtering to days after latest clustered date: {latest_clustered}")
                days = [d for d in days if d > latest_clustered]
        if end_date is not None:
            days = [d for d in days if d <= end_date]
        if not days:
            print("No unprocessed days found — nothing to do.")
            return
        print(f"Found {len(days)} unprocessed day(s): {days[0]} → {days[-1]}")
    else:
        first_day = start_date
        last_day = end_date or first_day
        days = [
            first_day + timedelta(days=i)
            for i in range((last_day - first_day).days + 1)
        ]
    print(f"Running clustering for {len(days)} day(s): {days[0]} → {days[-1]}")

    base_out = Path(output_dir) if output_dir else None
    for run_date in days:
        print(f"\n{'=' * 60}")
        print(f"Processing {run_date} ({days.index(run_date) + 1}/{len(days)})")
        print(f"{'=' * 60}")
        tokens_before = (client._input_tokens, client._output_tokens)
        emb_cost_before = emb_model.total_cost()
        await _run_day(
            run_date=run_date,
            out=base_out / str(run_date) if base_out is not None else None,
            client=client,
            emb_model=emb_model,
            spacy_model=spacy_model,
            window_size=window_size,
            overlap_tokens=overlap_tokens,
            initial_labels=initial_labels,
            skip_merge=skip_merge,
            merge_batch_size=merge_batch_size,
            merge_max_rounds=merge_max_rounds,
            max_concurrent=max_concurrent,
            provider=provider,
            target_clusters=target_clusters,
            min_clusters=min_clusters,
            max_clusters=max_clusters,
            cluster_scale=cluster_scale,
            low_threshold=low_threshold,
            high_threshold=high_threshold,
            expiry_days=expiry_days,
        )
        day_input = client._input_tokens - tokens_before[0]
        day_output = client._output_tokens - tokens_before[1]
        day_llm_cost = _cost(day_input, day_output, provider)
        day_emb_cost = emb_model.total_cost() - emb_cost_before
        print(
            f"\n--- Cost for {run_date}: "
            f"${day_llm_cost:.4f} LLM ({day_input:,} in / {day_output:,} out tokens)"
            + (f" + ${day_emb_cost:.4f} embeddings" if day_emb_cost else "")
            + f" = ${day_llm_cost + day_emb_cost:.4f} total ---"
        )

    print(f"\n{'=' * 60}")
    print(f"Completed {len(days)} day(s).")
    print("\n--- Total cost across all days ---")
    print("  LLM:")
    for line in client.cost_summary().splitlines():
        print(f"  {line}")
    total_emb_cost = emb_model.total_cost()
    if total_emb_cost:
        emb_tokens = getattr(emb_model, "_input_tokens", 0)
        print(
            f"  Embeddings: ${total_emb_cost:.4f} (mistral-embed, {emb_tokens:,} tokens)"
        )
    print(f"  Grand total: ${client.total_cost() + total_emb_cost:.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "LLM-based text clustering v2 — DB-aware deduplication. "
            "New clusters are filtered against existing DB clusters before classification. "
            f"Supports providers: {PROVIDER_MISTRAL} (default), {PROVIDER_ANTHROPIC}."
        )
    )
    parser.add_argument(
        "--spacy-model", default=os.getenv("SPACY_MODEL", "fr_core_news_sm")
    )
    parser.add_argument(
        "--window-size", type=int, default=int(os.getenv("WINDOW_SIZE", "3"))
    )
    parser.add_argument(
        "--overlap-tokens", type=int, default=int(os.getenv("OVERLAP_TOKENS", "30"))
    )
    parser.add_argument(
        "--provider",
        choices=[PROVIDER_MISTRAL, PROVIDER_ANTHROPIC],
        default=os.getenv("PROVIDER", PROVIDER_ANTHROPIC),
        help=f"LLM provider to use. Default: {PROVIDER_ANTHROPIC}.",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=int(os.getenv("MAX_CONCURRENT_REQUESTS", str(MAX_CONCURRENT))),
        help=f"Max parallel LLM requests for steps 1 and 3. Default: {MAX_CONCURRENT}",
    )
    parser.add_argument(
        "--initial-labels-file",
        default=os.getenv("INITIAL_LABELS_FILE"),
        help="Path to a JSON list of seed labels. Overrides built-in SEED_LABELS.",
    )
    parser.add_argument(
        "--no-seeds",
        action="store_true",
        default=os.getenv("NO_SEEDS", "").lower() in ("1", "true", "yes"),
        help="Start with no initial labels; let the LLM generate all labels from scratch.",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        default=os.getenv("SKIP_MERGE", "").lower() in ("1", "true", "yes"),
        help="Skip the label-merging step (step 2).",
    )
    parser.add_argument(
        "--merge-batch-size",
        type=int,
        default=int(os.getenv("MERGE_BATCH_SIZE", "30")),
        help="Number of labels per merge call in step 2. Default: 30.",
    )
    parser.add_argument(
        "--merge-max-rounds",
        type=int,
        default=int(os.getenv("MERGE_MAX_ROUNDS", "20")),
        help="Maximum number of hierarchical merge rounds in step 2. Default: 20.",
    )
    parser.add_argument(
        "--target-clusters",
        type=int,
        default=int(os.getenv("TARGET_CLUSTERS"))
        if os.getenv("TARGET_CLUSTERS")
        else None,
        help="Explicit target cluster count; overrides adaptive default (sqrt(n_docs)).",
    )
    parser.add_argument(
        "--min-clusters",
        type=int,
        default=int(os.getenv("MIN_CLUSTERS", "7")),
        help="Minimum clusters when using adaptive target. Default: 7.",
    )
    parser.add_argument(
        "--max-clusters",
        type=int,
        default=int(os.getenv("MAX_CLUSTERS", "150")),
        help="Maximum clusters when using adaptive target. Default: 150.",
    )
    parser.add_argument(
        "--cluster-scale",
        type=float,
        default=float(os.getenv("CLUSTER_SCALE", "1.0")),
        help="Multiplier on sqrt(n_docs) for adaptive cluster target. Default: 1.0.",
    )
    parser.add_argument(
        "--low-threshold",
        type=float,
        default=float(os.getenv("LOW_THRESHOLD", str(_LOW_THRESHOLD))),
        help=f"Similarity below this → auto-keep new label. Default: {_LOW_THRESHOLD}.",
    )
    parser.add_argument(
        "--high-threshold",
        type=float,
        default=float(os.getenv("HIGH_THRESHOLD", str(_HIGH_THRESHOLD))),
        help=f"Similarity above this → auto-drop new label. Default: {_HIGH_THRESHOLD}.",
    )
    parser.add_argument(
        "--embedding-backend",
        choices=[EMBEDDING_BACKEND_ST, EMBEDDING_BACKEND_MISTRAL],
        default=os.getenv("EMBEDDING_BACKEND", EMBEDDING_BACKEND_MISTRAL),
        help=f"Embedding backend for similarity. '{EMBEDDING_BACKEND_MISTRAL}' uses mistral-embed via the Mistral API. Default: {EMBEDDING_BACKEND_MISTRAL}.",
    )
    parser.add_argument(
        "--embedding-model",
        default=os.getenv("EMBEDDING_MODEL", _EMBEDDING_MODEL),
        help=f"Model name for the sentence-transformer backend (ignored for mistral). Default: {_EMBEDDING_MODEL}.",
    )
    parser.add_argument(
        "--expiry-days",
        type=int,
        default=int(os.getenv("EXPIRY_DAYS", "30")),
        help="Clusters with no case assigned within this many days before start-date are excluded. Default: 30.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR"),
        help="Directory for output files. Omit to skip all file output (DB writes still happen).",
    )
    parser.add_argument(
        "--start-date",
        default=os.getenv("START_DATE"),
        metavar="YYYY-MM-DD",
        help="Keep only records on or after this date (inclusive). Default: no lower bound.",
    )
    parser.add_argument(
        "--end-date",
        default=os.getenv("END_DATE"),
        metavar="YYYY-MM-DD",
        help="Keep only records on or before this date (inclusive). Default: no upper bound.",
    )
    parser.add_argument(
        "--no-only-recent",
        action="store_true",
        default=os.getenv("ONLY_RECENT", "true").lower() in ("0", "false", "no"),
        help=(
            "When auto-discovering unprocessed days (no --start-date), also include days "
            "that predate the most recent entry in case_to_clusters. "
            "Default: off (i.e. only recent days are processed)."
        ),
    )
    args = parser.parse_args()
    if args.no_seeds:
        initial = []
    elif args.initial_labels_file:
        initial = json.loads(Path(args.initial_labels_file).read_text(encoding="utf-8"))
    else:
        initial = SEED_LABELS

    asyncio.run(
        run(
            output_dir=args.output_dir,
            only_recent=not args.no_only_recent,
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
            low_threshold=args.low_threshold,
            high_threshold=args.high_threshold,
            embedding_backend=args.embedding_backend,
            embedding_model=args.embedding_model,
            expiry_days=args.expiry_days,
        )
    )
