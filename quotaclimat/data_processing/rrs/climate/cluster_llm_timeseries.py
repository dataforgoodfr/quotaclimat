"""
Timeseries LLM clustering using Claude Haiku.

Builds a label taxonomy from a warmup window, then classifies each subsequent
day's transcripts against the evolving taxonomy. Docs that don't match any
known label are flagged as 'other', used to discover new clusters at end of
day, then reclassified against the updated taxonomy.

Pipeline:
  Warmup (first N days):
    1. Generate narrative labels (Step 1)
    2. Merge/deduplicate labels (Step 2)
    3. Classify warmup docs (Step 3)

  Incremental days:
    For each day:
      3a. Classify with 'other' escape hatch (Step 3+other)
      1b. Generate new labels from 'other' docs (Step 1)
      2b. Merge new labels into taxonomy (Step 2)
      3b. Reclassify 'other' docs against updated taxonomy (Step 3)
"""

import argparse
import asyncio
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm.asyncio import tqdm

from quotaclimat.data_processing.rrs.climate.cluster import (
    HF_DATASET,
    HF_ID_COLUMN,
    HF_TEXT_COLUMN,
    load_claims,
    load_from_hf,
    split_sentences,
)
from quotaclimat.data_processing.rrs.climate.cluster_llm import (
    MAX_CONCURRENT,
    MODEL,
    SEED_LABELS,
    SYSTEM_PROMPT,
    _build_client,
    _build_sentences_by_doc,
    _parse_list_response,
    build_labels_from_transcripts,
    merge_labels,
    classify_all_transcripts,
)

_MAX_MERGE_ATTEMPTS = 3

DATE_COLUMN = "data_item_start"

_OTHER_TOKEN = "other"


def _step3_other_prompt(sentences: list[str], label_list: list[str]) -> str:
    return (
        "Given the label list and the sentences, select all labels that describe "
        "the concepts expressed in the sentences.\n"
        "If none of the labels apply and the sentences contain a distinct new narrative, "
        f'return ["{_OTHER_TOKEN}"] instead.\n'
        f"Label list: {label_list}\n"
        f"Sentences: {sentences}\n"
        "Return ONLY a JSON array of matching label strings using double quotes. No code fences."
    )


async def _step3_other_call(
    doc_id: str,
    sentences: list[str],
    label_list: list[str],
    client,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[str], bool]:
    """Returns (doc_id, matched_labels, is_other).

    is_other=True means the doc expressed a new narrative not in label_list.
    """
    async with semaphore:
        try:
            import anthropic as _anthropic
            response = await client.messages.create(
                model=MODEL,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": _step3_other_prompt(sentences, label_list)}],
            )
            raw = next(b.text for b in response.content if b.type == "text")
            matched = _parse_list_response(raw)
            label_lower = {lb.strip().lower(): lb for lb in label_list}
            valid = [label_lower[m.strip().lower()] for m in matched if m.strip().lower() in label_lower]
            is_other = any(m.strip().lower() == _OTHER_TOKEN for m in matched) and not valid
            return doc_id, valid, is_other
        except Exception as exc:
            print(f"  [warn] step3+other {doc_id} failed: {exc}")
            return doc_id, [], False


async def classify_with_other(
    sentences_by_doc: dict,
    label_list: list[str],
    client,
    max_concurrent: int = MAX_CONCURRENT,
) -> tuple[dict, list[str]]:
    """Classify docs; split into matched assignments and 'other' doc IDs.

    Returns:
        doc_to_labels: {doc_id: [matched labels]} for docs with known labels
        other_doc_ids: list of doc_ids whose content matched nothing in label_list
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results = await tqdm.gather(
        *[
            _step3_other_call(doc_id, sentences, label_list, client, semaphore)
            for doc_id, sentences in sentences_by_doc.items()
        ],
        desc="Step 3+other — classifying",
    )
    doc_to_labels: dict[str, list[str]] = {}
    other_doc_ids: list[str] = []
    for doc_id, valid, is_other in results:
        if is_other:
            other_doc_ids.append(doc_id)
        elif valid:
            doc_to_labels[doc_id] = valid
    return doc_to_labels, other_doc_ids


# ---------------------------------------------------------------------------
# Incremental merge — absorb new candidates into a fixed existing taxonomy
# ---------------------------------------------------------------------------

def _absorb_prompt(new_candidates: list[str], existing_labels: list[str], attempt: int) -> str:
    urgency = (
        "" if attempt == 1
        else " Be more aggressive: if a candidate is even roughly similar to an existing label, map it."
        if attempt == 2
        else " This is the final attempt. Only mark a candidate as NEW if it is completely unrelated to every existing label."
    )
    return (
        "You have a fixed taxonomy of existing French climate-discussion labels and a short list of new candidate labels.\n"
        "For each new candidate, decide whether it expresses essentially the same claim as one of the existing labels.\n"
        "- If yes: return the existing label it should map to (copy it verbatim).\n"
        "- If no: return \"NEW\".\n"
        f"{urgency}\n"
        f"Existing labels: {existing_labels}\n"
        f"New candidates: {new_candidates}\n"
        "Return a JSON object mapping each new candidate string to either the matching existing label (verbatim) or \"NEW\".\n"
        "Example: {\"candidate A\": \"existing label 3\", \"candidate B\": \"NEW\"}\n"
        "No code fences."
    )


async def _try_absorb_call(
    new_candidates: list[str],
    existing_labels: list[str],
    client,
    attempt: int,
) -> dict[str, str]:
    """Single LLM call that maps each new candidate to an existing label or 'NEW'."""
    try:
        response = await client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": _absorb_prompt(new_candidates, existing_labels, attempt)}],
        )
        raw = next(b.text for b in response.content if b.type == "text")
        raw = re.sub(r"```(?:json|python)?\s*", "", raw).strip()
        try:
            mapping = json.loads(raw)
        except json.JSONDecodeError:
            print(f"  [warn] absorb attempt {attempt}: could not parse JSON — treating all as NEW.")
            return {c: "NEW" for c in new_candidates}
        return {c: str(mapping.get(c, "NEW")) for c in new_candidates}
    except Exception as exc:
        print(f"  [warn] absorb attempt {attempt} failed: {exc}")
        return {c: "NEW" for c in new_candidates}


async def absorb_new_into_existing(
    new_candidates: list[str],
    existing_labels: list[str],
    client,
    max_attempts: int = _MAX_MERGE_ATTEMPTS,
) -> list[str]:
    """Try to match each new candidate to an existing label over up to *max_attempts* rounds.

    Candidates that are successfully mapped to an existing label are discarded (already
    represented). Candidates still unmatched after all attempts are returned as-is to be
    appended to the taxonomy.

    Returns the list of genuinely new labels to add.
    """
    existing_lower = {lb.strip().lower(): lb for lb in existing_labels}
    pending = list(new_candidates)

    for attempt in range(1, max_attempts + 1):
        if not pending:
            break
        print(f"  Absorb attempt {attempt}/{max_attempts}: {len(pending)} candidate(s) remaining...")
        mapping = await _try_absorb_call(pending, existing_labels, client, attempt)
        still_new = []
        for candidate, mapped in mapping.items():
            if mapped.strip().lower() == "new" or mapped.strip().lower() not in existing_lower:
                still_new.append(candidate)
            else:
                print(f"    ✓ '{candidate}' → '{mapped}'")
        absorbed = len(pending) - len(still_new)
        print(f"    {absorbed} absorbed, {len(still_new)} still NEW.")
        pending = still_new

    if pending:
        print(f"  {len(pending)} candidate(s) added as new labels after {max_attempts} attempts.")
    return pending


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
    warmup_days: int = 7,
    input_path: Optional[str] = None,
    text_column: str = "claim_text",
    hf_dataset: str = HF_DATASET,
    hf_split: str = "train",
    hf_text_column: str = HF_TEXT_COLUMN,
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
    initial_labels: Optional[list[str]] = None,
    skip_merge: bool = False,
    merge_batch_size: int = 30,
    merge_max_rounds: int = 20,
    country: Optional[str] = "france",
    date_column: str = DATE_COLUMN,
    max_concurrent: int = MAX_CONCURRENT,
    max_merge_attempts: int = _MAX_MERGE_ATTEMPTS,
    skip_absorb: bool = False,
    save_daily_labels: bool = False,
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    client = _build_client()

    warmup_end = start_date + timedelta(days=warmup_days - 1)
    if warmup_end > end_date:
        warmup_end = end_date
    print(f"Date range  : {start_date} → {end_date}")
    print(f"Warmup      : {start_date} → {warmup_end} ({warmup_days} days)")
    print(f"Incremental : {warmup_end + timedelta(days=1)} → {end_date}")

    # --- Load all data ---
    if input_path:
        print(f"\nLoading claims from {input_path}...")
        raw_texts = load_claims(input_path, text_column)
        docs_df = pd.DataFrame({hf_text_column: raw_texts})
        id_col = None
        if date_column not in docs_df.columns:
            raise ValueError(
                f"Local file must contain a '{date_column}' column for timeseries mode."
            )
    else:
        print(f"\nLoading from HuggingFace: {hf_dataset} ({hf_split})...")
        docs_df = load_from_hf(
            hf_dataset, hf_split, hf_text_column,
            country=country,
            extra_columns=[date_column],
        )
        id_col = HF_ID_COLUMN if HF_ID_COLUMN in docs_df.columns else None
        print(f"  {len(docs_df)} documents loaded.")

    # Filter to requested date range
    dates_col = pd.to_datetime(docs_df[date_column], errors="coerce")
    mask = (dates_col.dt.date >= start_date) & (dates_col.dt.date <= end_date)
    docs_df = docs_df[mask].reset_index(drop=True)
    print(f"  {len(docs_df)} documents in [{start_date}, {end_date}].")

    # --- Shared column lists ---
    assignments_cols = ["doc_id", "label", "label_id", "date"]
    other_cols = ["doc_id", "date"]
    assignments_path = out / "transcript_label_assignments.csv"
    other_path = out / "other_docs.csv"
    evolution_path = out / "label_evolution.json"

    # Remove stale output files so we don't append to old runs
    for p in [assignments_path, other_path]:
        p.unlink(missing_ok=True)

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
        warmup_df, hf_text_column, id_col, spacy_model, window_size, overlap_tokens
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
    known_labels: set[str] = set(all_labels)

    # Record all warmup labels in evolution (first_seen = warmup_end)
    evolution: list[dict] = [
        {"label": label, "label_id": i, "first_seen_date": warmup_end.isoformat()}
        for i, label in enumerate(all_labels)
    ]
    evolution_path.write_text(json.dumps(evolution, ensure_ascii=False, indent=2), encoding="utf-8")

    if save_daily_labels:
        (out / f"labels_{warmup_end.isoformat()}.json").write_text(
            (out / "labels.json").read_text(encoding="utf-8"), encoding="utf-8"
        )

    # Step 3 (warmup — no "other" option)
    print("\nWarmup Step 3: Classifying warmup transcripts...")
    warmup_assignments = await classify_all_transcripts(
        warmup_sentences, all_labels, client, max_concurrent
    )
    # Flatten warmup assignments — use warmup_end as the "date" for all warmup docs
    warmup_records = _assignments_to_records(warmup_assignments, label_to_id, day=warmup_end)
    _append_csv(assignments_path, warmup_records, assignments_cols)
    print(f"  {len(warmup_records)} warmup assignment rows saved.")

    # =========================================================================
    # Phase 2 — Incremental days
    # =========================================================================
    incremental_start = warmup_end + timedelta(days=1)
    if incremental_start > end_date:
        print("\nNo incremental days — entire range is within warmup window.")
    else:
        for current_day in _date_range(incremental_start, end_date):
            print(f"\n{'='*60}")
            print(f"DAY: {current_day.isoformat()}")
            print(f"{'='*60}")

            day_df = _slice_by_date(docs_df, current_day, date_column)
            if day_df.empty:
                print("  No documents for this day — skipping.")
                continue

            print(f"  {len(day_df)} documents.")
            day_sentences = _build_sentences_by_doc(
                day_df, hf_text_column, id_col, spacy_model, window_size, overlap_tokens
            )
            if not day_sentences:
                print("  No sentence chunks produced — skipping.")
                continue

            # Step 3a — classify with "other" escape hatch
            print(f"  Classifying {len(day_sentences)} docs against {len(all_labels)} labels...")
            doc_to_labels, other_ids = await classify_with_other(
                day_sentences, all_labels, client, max_concurrent
            )
            print(f"  → {len(doc_to_labels)} matched, {len(other_ids)} 'other'")

            # Append matched assignments
            day_records = _assignments_to_records(doc_to_labels, label_to_id, day=current_day)
            _append_csv(assignments_path, day_records, assignments_cols)

            # Log "other" doc IDs
            if other_ids:
                other_records = [{"doc_id": d, "date": current_day.isoformat()} for d in other_ids]
                _append_csv(other_path, other_records, other_cols)

                # Step 1b — generate new labels from "other" docs
                other_sentences = {d: day_sentences[d] for d in other_ids if d in day_sentences}
                print(f"\n  Step 1b: Generating labels from {len(other_sentences)} 'other' docs...")
                new_candidates = await build_labels_from_transcripts(
                    other_sentences, client, max_concurrent
                )
                # Only keep truly new candidates
                new_candidates = [l for l in new_candidates if l.strip().lower() not in {lb.strip().lower() for lb in all_labels}]
                print(f"  → {len(new_candidates)} new candidate labels.")

                if new_candidates:
                    # Step 2b — try to absorb new candidates into the fixed existing taxonomy.
                    # The existing label list is never reduced; only genuinely unmatched
                    # candidates (after max_merge_attempts tries) are appended.
                    if not skip_merge and not skip_absorb:
                        print(f"\n  Step 2b: Trying to absorb {len(new_candidates)} candidate(s) into {len(all_labels)} existing labels...")
                        truly_new = await absorb_new_into_existing(
                            new_candidates, all_labels, client, max_attempts=max_merge_attempts
                        )
                    else:
                        truly_new = new_candidates

                    all_labels = list(all_labels) + truly_new
                    label_to_id = _save_labels(out, all_labels)
                    known_labels = set(all_labels)

                    # Update evolution with new labels
                    evolution = _update_evolution(
                        evolution, truly_new, {e["label"] for e in evolution},
                        label_to_id, current_day.isoformat()
                    )
                    evolution_path.write_text(
                        json.dumps(evolution, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                    print(f"  {len(truly_new)} new label(s) added to taxonomy.")

                    # Step 3b — reclassify "other" docs with updated taxonomy
                    print(f"  Step 3b: Reclassifying {len(other_sentences)} 'other' docs...")
                    reclassified = await classify_all_transcripts(
                        other_sentences, all_labels, client, max_concurrent
                    )
                    reclassified_records = _assignments_to_records(
                        reclassified, label_to_id, day=current_day
                    )
                    _append_csv(assignments_path, reclassified_records, assignments_cols)
                    print(f"  → {len(reclassified_records)} reclassified assignment rows saved.")

            if save_daily_labels:
                (out / f"labels_{current_day.isoformat()}.json").write_text(
                    (out / "labels.json").read_text(encoding="utf-8"), encoding="utf-8"
                )

    print(f"\n{'='*60}")
    print("DONE")
    print(f"  labels.json            : {out / 'labels.json'}")
    print(f"  label_evolution.json   : {evolution_path}")
    print(f"  assignments CSV        : {assignments_path}")
    if other_path.exists():
        print(f"  other_docs.csv         : {other_path}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=(
            "Timeseries LLM clustering — warmup taxonomy + daily incremental updates."
        )
    )
    parser.add_argument("--start-date", required=True, metavar="YYYY-MM-DD",
                        help="Start of the date range (inclusive).")
    parser.add_argument("--end-date", required=True, metavar="YYYY-MM-DD",
                        help="End of the date range (inclusive).")
    parser.add_argument("--warmup-days", type=int, default=7,
                        help="Number of days used for the warmup phase. Default: 7.")
    parser.add_argument("--output-dir", default="./timeseries_output",
                        help="Directory for output files. Default: ./timeseries_output.")
    parser.add_argument("--input",
                        help="Path to a local claims file (.json or .csv). "
                             "If omitted, loads from HuggingFace.")
    parser.add_argument("--text-column", default="claim_text",
                        help="Column/key for claim text in a local file. Default: claim_text.")
    parser.add_argument("--hf-dataset", default=HF_DATASET)
    parser.add_argument("--hf-split", default="train")
    parser.add_argument("--hf-text-column", default=HF_TEXT_COLUMN,
                        help=f"Text field in the HF dataset. Default: {HF_TEXT_COLUMN}.")
    parser.add_argument("--country", default="france",
                        help="Filter dataset to this country. Default: france. "
                             "Pass empty string to load all countries.")
    parser.add_argument("--date-column", default=DATE_COLUMN,
                        help=f"Dataset column that holds the date. Default: {DATE_COLUMN}.")
    parser.add_argument("--spacy-model", default="fr_core_news_sm")
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--overlap-tokens", type=int, default=30)
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT,
                        help=f"Max parallel Anthropic requests. Default: {MAX_CONCURRENT}.")
    parser.add_argument("--initial-labels-file",
                        help="Path to a JSON list of seed labels. Overrides built-in SEED_LABELS.")
    parser.add_argument("--no-seeds", action="store_true",
                        help="Start with no initial labels.")
    parser.add_argument("--skip-merge", action="store_true",
                        help="Skip the label-merging step.")
    parser.add_argument("--merge-batch-size", type=int, default=10,
                        help="Number of labels per merge call. Default: 30.")
    parser.add_argument("--merge-max-rounds", type=int, default=10,
                        help="Maximum hierarchical merge rounds. Default: 20.")
    parser.add_argument(
        "--no-absorb", action="store_true",
        help="Skip the absorption step — all new candidates are added to the taxonomy as-is.",
    )
    parser.add_argument(
        "--max-merge-attempts", type=int, default=_MAX_MERGE_ATTEMPTS,
        help=(
            "Max attempts to absorb a new candidate into the existing taxonomy "
            f"before adding it as a new label. Default: {_MAX_MERGE_ATTEMPTS}."
        ),
    )
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
        hf_dataset=args.hf_dataset,
        hf_split=args.hf_split,
        hf_text_column=args.hf_text_column,
        spacy_model=args.spacy_model,
        window_size=args.window_size,
        overlap_tokens=args.overlap_tokens,
        initial_labels=initial,
        skip_merge=args.skip_merge,
        merge_batch_size=args.merge_batch_size,
        merge_max_rounds=args.merge_max_rounds,
        country=args.country or None,
        date_column=args.date_column,
        max_concurrent=args.max_concurrent,
        max_merge_attempts=args.max_merge_attempts,
        skip_absorb=args.no_absorb,
        save_daily_labels=args.save_daily_labels,
    ))
