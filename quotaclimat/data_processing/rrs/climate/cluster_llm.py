"""
LLM-based text clustering using Claude Haiku.

Three-step transcript-level pipeline:
  1. Process each transcript: split into sentences, match to existing narratives or generate new ones
  2. Deduplicate/merge the label list; assign integer IDs
  3. Final classification pass: assign multiple labels per transcript (async, semaphore-limited)
  4. Report mesinfo scores per label
"""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Optional

from tqdm.asyncio import tqdm

import anthropic
import pandas as pd
from dotenv import load_dotenv

from quotaclimat.data_processing.rrs.climate.cluster import (
    HF_DATASET,
    HF_ID_COLUMN,
    HF_TEXT_COLUMN,
    load_claims,
    load_from_hf,
    split_sentences,
)

MODEL = "claude-haiku-4-5"
MAX_CONCURRENT = 10

SYSTEM_PROMPT = "You are an assistant helping editors to aggregate climate misinformation claims."

SEED_LABELS: list[str] = [
    "Les renouvelables augmentent le coût de l'électricité",
    "Il est inutile de réduire les rejets de gaz à effet de serre de la France",
    "Les populations sont manipulées de façon injustifiée.",
    "L'élevage est neutre voire avantageux pour le climat",
    "Les voitures électriques polluent plus que les voitures thermiques"
]


def _build_client() -> anthropic.AsyncAnthropic:
    import os
    load_dotenv()
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to your .env file: ANTHROPIC_API_KEY=sk-ant-..."
        )
    return anthropic.AsyncAnthropic(api_key=api_key)


def _parse_list_response(raw: str) -> list[str]:
    """Parse a Python/JSON list from an LLM response string."""
    start, end = raw.find("["), raw.rfind("]") + 1
    if start == -1 or end == 0:
        return []
    try:
        return [str(item) for item in json.loads(raw[start:end])]
    except json.JSONDecodeError:
        return []


async def _step1_call(
    sentence_list: list[str],
    given_labels: list[str],
    client: anthropic.AsyncAnthropic,
) -> list[str]:
    """Match sentences to existing labels or generate new meaningful label names."""
    user_prompt = (
        "Given the labels, and under a text classification scenario where fact-checkers "
        "aim to generalize a claim into a more abstract one, can all these texts be matched "
        "to the given labels?\n"
        "If the sentence does not match any of the labels, please generate a meaningful new label name.\n"
        f"Labels: {given_labels}\n"
        f"Sentences: {sentence_list}\n"
        "You should NOT return meaningless label names such as 'new_label_1' or 'unknown_topic_1' "
        "and only return the new label names, please return the labels as a python list."
    )
    response = await client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    raw = next(b.text for b in response.content if b.type == "text")
    return _parse_list_response(raw)


STEP1_BATCH_SIZE = 30


async def build_labels_from_transcripts(
    sentences_by_doc: dict,
    client: anthropic.AsyncAnthropic,
    initial_labels: list[str] = None,
    batch_size: int = STEP1_BATCH_SIZE,
) -> list[str]:
    """Step 1: Process transcripts in batches, gathering new labels after each batch."""
    labels = list(initial_labels or [])
    labels_lower = {l.strip().lower() for l in labels}
    doc_ids = [doc_id for doc_id, sentences in sentences_by_doc.items() if sentences]
    total = len(doc_ids)

    for batch_start in range(0, total, batch_size):
        batch = doc_ids[batch_start: batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        batch_results = await tqdm.gather(*[
            _step1_call(sentences_by_doc[doc_id], labels, client)
            for doc_id in batch
        ], desc=f"Step 1 — batch {batch_num}/{total_batches}")

        for returned in batch_results:
            for label in returned:
                key = label.strip().lower()
                if key not in labels_lower:
                    labels.append(label)
                    labels_lower.add(key)

        print(f"  Batch {batch_num}/{total_batches} done — {len(labels)} labels so far.")

    return labels


async def merge_labels(
    label_list: list[str],
    client: anthropic.AsyncAnthropic,
) -> list[str]:
    """Step 2: Merge similar/duplicate labels into a deduplicated list."""
    user_prompt = (
        "Please analyze the provided list of labels to identify entries that are strictly similar "
        "or duplicate, considering synonyms, variations in phrasing.\n"
        "The labels are climate misinformation claims and should stay phrased as such.\n"
        "Your task is to merge these similar entries into a single representative label for each. "
        "The goal is to simplify the list by reducing redundancies without losing information/granularity. "
        f"Here is the list of labels:\n{label_list}.\n"
        "Produce the final, deduplicated list as a python list."
    )
    response = await client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    raw = next(b.text for b in response.content if b.type == "text")
    return _parse_list_response(raw)


async def _step3_call(
    doc_id: str,
    sentences: list[str],
    label_list: list[str],
    client: anthropic.AsyncAnthropic,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[str]]:
    """Classify a single transcript against the final label list (returns multiple labels)."""
    user_prompt = (
        "Given the label list and the sentences, please select all the labels that describe "
        "the concepts expressed in the following sentences\n"
        f"Label list: {label_list}\n"
        f"Sentences: {sentences}\n"
        "You should only return the label names as a python list. Use only existing labels"
    )
    async with semaphore:
        try:
            response = await client.messages.create(
                model=MODEL,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
            raw = next(b.text for b in response.content if b.type == "text")
            matched = _parse_list_response(raw)
            label_lower = {l.strip().lower(): l for l in label_list}
            valid = [label_lower[m.strip().lower()] for m in matched if m.strip().lower() in label_lower]
            return doc_id, valid
        except Exception as exc:
            print(f"  [warn] transcript {doc_id} failed: {exc}")
            return doc_id, []


async def classify_all_transcripts(
    sentences_by_doc: dict,
    label_list: list[str],
    client: anthropic.AsyncAnthropic,
    max_concurrent: int = MAX_CONCURRENT,
) -> dict:
    """Step 3: Classify all transcripts in parallel; each transcript maps to multiple labels."""
    semaphore = asyncio.Semaphore(max_concurrent)

    results = await tqdm.gather(*[
        _step3_call(doc_id, sentences, label_list, client, semaphore)
        for doc_id, sentences in sentences_by_doc.items()
    ], desc="Classifying transcripts")
    return dict(results)


def _build_sentences_by_doc(
    docs_df: pd.DataFrame,
    hf_text_column: str,
    id_col: Optional[str],
    spacy_model: str,
    window_size: int,
    overlap_tokens: int,
) -> dict:
    """Split all transcripts into sentences and group them by document ID."""
    if id_col:
        sentences_df = split_sentences(
            docs_df, text_column=hf_text_column, spacy_model=spacy_model,
            window_size=window_size, overlap_tokens=overlap_tokens,
        )
        print(f"  {len(sentences_df)} sentence chunks total.")
        return {
            str(doc_id): group["sentence"].tolist()
            for doc_id, group in sentences_df.groupby(id_col)
        }
    else:
        # Local file case: split each document individually to preserve boundaries.
        # spaCy caches the loaded model so repeated calls are fast after the first.
        sentences_by_doc = {}
        for i, row in enumerate(docs_df.itertuples()):
            text = getattr(row, hf_text_column)
            tmp = pd.DataFrame({hf_text_column: [text]})
            s_df = split_sentences(
                tmp, text_column=hf_text_column, spacy_model=spacy_model,
                window_size=window_size, overlap_tokens=overlap_tokens,
            )
            sentences_by_doc[str(i)] = s_df["sentence"].tolist()
        total_sentences = sum(len(v) for v in sentences_by_doc.values())
        print(f"  {total_sentences} sentence chunks total ({len(sentences_by_doc)} documents).")
        return sentences_by_doc


async def run(
    output_dir: str,
    input_path: Optional[str] = None,
    text_column: str = "claim_text",
    hf_dataset: str = HF_DATASET,
    hf_split: str = "train",
    hf_text_column: str = HF_TEXT_COLUMN,
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
    initial_labels: list[str] = None,
    skip_merge: bool = False,
    country: Optional[str] = "france",
    max_concurrent: int = MAX_CONCURRENT,
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    client = _build_client()

    # --- Load data ---
    if input_path:
        print(f"Loading claims from {input_path}...")
        raw_texts = load_claims(input_path, text_column)
        docs_df = pd.DataFrame({hf_text_column: raw_texts})
        id_col = None
    else:
        print(f"Loading texts from HuggingFace: {hf_dataset} ({hf_split})...")
        docs_df = load_from_hf(hf_dataset, hf_split, hf_text_column, country=country)
        print(f"  {len(docs_df)} documents loaded.")
        id_col = HF_ID_COLUMN if HF_ID_COLUMN in docs_df.columns else None

    # --- Split all transcripts into sentences, grouped by document ---
    print(f"Splitting transcripts into sentences (spaCy: {spacy_model}, "
          f"window={window_size}, overlap={overlap_tokens})...")
    sentences_by_doc = _build_sentences_by_doc(
        docs_df, hf_text_column, id_col, spacy_model, window_size, overlap_tokens
    )

    # --- Step 1: build narrative label list from all transcripts (sequential) ---
    print(f"\nStep 1: Building narrative labels from {len(sentences_by_doc)} transcripts...")
    labels = await build_labels_from_transcripts(
        sentences_by_doc, client, initial_labels=initial_labels or []
    )
    print(f"  {len(labels)} candidate labels generated.")
    (out / "labels_raw.json").write_text(
        json.dumps(labels, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # --- Step 2: merge similar labels, assign IDs ---
    if not skip_merge:
        print("\nStep 2: Merging semantically similar labels...")
        labels = await merge_labels(labels, client)
        print(f"  {len(labels)} labels after merging.")

    label_to_id = {label: i for i, label in enumerate(labels)}
    labels_data = [{"id": i, "label": label} for i, label in enumerate(labels)]
    labels_path = out / "labels.json"
    labels_path.write_text(json.dumps(labels_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Final labels with IDs saved to {labels_path}")

    # --- Step 3: classify all transcripts against final labels (parallel) ---
    print(f"\nStep 3: Classifying {len(sentences_by_doc)} transcripts "
          f"(max {max_concurrent} concurrent requests)...")
    doc_to_labels = await classify_all_transcripts(
        sentences_by_doc, labels, client, max_concurrent=max_concurrent
    )

    records = []
    for doc_id, matched_labels in doc_to_labels.items():
        for label in matched_labels:
            label_id = label_to_id.get(label)
            if label_id is not None:
                records.append({"doc_id": doc_id, "label": label, "label_id": label_id})

    assignments_df = pd.DataFrame(records, columns=["doc_id", "label", "label_id"])
    assignments_path = out / "transcript_label_assignments.csv"
    assignments_df.to_csv(assignments_path, index=False)
    print(f"\nTranscript-label assignments saved to {assignments_path}")

    # --- Step 4: mesinfo statistics per label ---
    print("\nStep 4: Computing mesinfo statistics per label...")
    if id_col and "mesinfo_correct" in docs_df.columns and "mesinfo_incorrect" in docs_df.columns:
        mesinfo_df = (
            docs_df[[id_col, "mesinfo_correct", "mesinfo_incorrect"]]
            .copy()
            .assign(**{id_col: docs_df[id_col].astype(str)})
            .rename(columns={id_col: "doc_id"})
        )
        merged = assignments_df.merge(mesinfo_df, on="doc_id", how="left")
        deduped = merged.drop_duplicates(subset=["doc_id", "label_id"])

        stats = (
            deduped.groupby(["label_id", "label"])
            .agg(
                count=("doc_id", "count"),
                sum_mesinfo_correct=("mesinfo_correct", lambda x: (x == 1).sum()),
                sum_mesinfo_incorrect=("mesinfo_incorrect", lambda x: (x == 1).sum()),
            )
            .reset_index()
            .sort_values("label_id")
        )

        print("\n--- Label mesinfo statistics ---")
        print(stats.to_string(index=False))

        stats_path = out / "label_mesinfo_stats.csv"
        stats.to_csv(stats_path, index=False)
        print(f"\nLabel mesinfo stats saved to {stats_path}")
    else:
        print("  Skipping: id column or mesinfo columns not found in data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "LLM-based text clustering with Claude Haiku — "
            "transcript-level narrative matching (3-step pipeline)."
        )
    )
    parser.add_argument(
        "--input",
        help="Path to a local claims file (.json or .csv). "
             "If omitted, loads from --hf-dataset.",
    )
    parser.add_argument(
        "--text-column", default="claim_text",
        help="Column/key for claim text in a local file. Default: claim_text",
    )
    parser.add_argument("--hf-dataset", default=HF_DATASET)
    parser.add_argument("--hf-split", default="train")
    parser.add_argument(
        "--hf-text-column", default=HF_TEXT_COLUMN,
        help=f"Text field in the HF dataset. Default: {HF_TEXT_COLUMN}",
    )
    parser.add_argument(
        "--country", default="france",
        help="Filter dataset to this country. Default: france. "
             "Pass empty string to load all countries.",
    )
    parser.add_argument("--spacy-model", default="fr_core_news_sm")
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--overlap-tokens", type=int, default=30)
    parser.add_argument(
        "--max-concurrent", type=int, default=MAX_CONCURRENT,
        help=f"Max parallel Anthropic requests for step 3. Default: {MAX_CONCURRENT}",
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
        "--output-dir", default="./bertopic_llm_output",
        help="Directory for output files. Default: ./bertopic_llm_output",
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
        country=args.country or None,
        max_concurrent=args.max_concurrent,
    ))
