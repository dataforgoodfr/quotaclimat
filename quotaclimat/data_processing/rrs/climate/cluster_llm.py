"""
LLM-based text clustering using Claude Haiku.

Implements the "Text Clustering as Classification with LLMs" approach
(arXiv:2410.00927):
  1. Sample sentences → LLM generates candidate cluster labels
  2. Merge semantically similar labels
  3. Assign each sentence to the best label (via Messages Batches API)
  4. Aggregate mesinfo scores per cluster, deduplicated by document

The Batches API is used for step 3 to get 50% cost reduction and avoid
blocking on thousands of sequential requests.  The label list is
cache_control-marked so it is reused across every batch request.
"""

import argparse
import json
import random
import time
from pathlib import Path
from typing import Optional

import anthropic
import pandas as pd
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request
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
DEFAULT_N_LABELS = 15
DEFAULT_N_SAMPLE = 200  # sentences used for label generation

# Predefined seed labels.  The LLM will keep all of these and add new labels
# only for themes not already covered.  Edit or extend this list freely.
SEED_LABELS: list[str] = [
    # Climate denial / scepticism
    "Climatoscepticisme",
    "Hoax climatique / agenda vert",
    # Energy & transition
    "Coût des énergies renouvelables",
    "Transition énergétique",
    # Health misinformation
    "Désinformation sanitaire / pandémie",
    "Désinformation vaccinale",
    # Electoral fraud
    "Fraude électorale",
    # Immigration / replacement theory
    "Grand remplacement / immigration",
    # Globalist conspiracy
    "Complot mondialiste / état profond",
    # Factual climate reporting
    "Rapports scientifiques sur le climat",
    # Biodiversity / ecology
    "Biodiversité et écosystèmes",
    # Extreme weather
    "Événements météorologiques extrêmes",
    # Policy / regulation
    "Politiques environnementales",
    # Food / agriculture
    "Agriculture et alimentation durables",
]

_LABEL_GEN_SYSTEM = (
    "You are an expert at analysing text and identifying themes. "
    "You will receive a sample of sentences from a French-language media dataset "
    "covering climate, environment, and misinformation topics. "
    "Your task: start from the provided seed labels and supplement them with "
    "additional labels only for themes not already covered by the seeds."
)

_ASSIGN_SYSTEM_PREFIX = (
    "You are a text classifier. "
    "Assign the single most appropriate label to the sentence the user sends. "
    "Reply with only the label text, exactly as written in the list below. "
    'If no label fits well, reply with "Other".\n\nAvailable labels:\n'
)


def _build_client() -> anthropic.Anthropic:
    load_dotenv(Path(__file__).resolve().parents[4] / ".env")
    return anthropic.Anthropic()


def generate_labels(
    sentences: list[str],
    client: anthropic.Anthropic,
    n_sample: int = DEFAULT_N_SAMPLE,
    n_labels: int = DEFAULT_N_LABELS,
    seeds: list[str] = SEED_LABELS,
) -> list[str]:
    """Step 1 — sample sentences and ask the LLM to produce candidate labels.

    The LLM is given the seed labels as a starting point and asked to add new
    labels only for themes not already covered.  The returned list always
    includes every seed (possibly rephrased during the merge step).
    """
    sample = random.sample(sentences, min(n_sample, len(sentences)))
    sample_text = "\n---\n".join(sample)
    seeds_json = json.dumps(seeds, ensure_ascii=False)

    n_extra = max(0, n_labels - len(seeds))
    instruction = (
        f"You already have these seed labels:\n{seeds_json}\n\n"
        f"Below are {len(sample)} sentences from the dataset:\n\n"
        f"{sample_text}\n\n"
        f"Keep all seed labels as-is. "
        f"Add up to {n_extra} new labels for themes NOT already covered by the seeds. "
        f"Return the complete list (seeds + any new labels) as a JSON array of strings "
        f"and nothing else."
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=_LABEL_GEN_SYSTEM,
        messages=[{"role": "user", "content": instruction}],
    )
    raw = next(b.text for b in response.content if b.type == "text")
    start, end = raw.find("["), raw.rfind("]") + 1
    labels = [str(l) for l in json.loads(raw[start:end])]

    # Guarantee every seed is present even if the LLM dropped one
    existing_lower = {l.strip().lower() for l in labels}
    for seed in seeds:
        if seed.strip().lower() not in existing_lower:
            labels.append(seed)

    return labels


def merge_labels(
    labels: list[str],
    client: anthropic.Anthropic,
) -> list[str]:
    """Step 2 — merge semantically equivalent or highly overlapping labels."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": (
                f"Here are candidate cluster labels:\n"
                f"{json.dumps(labels, ensure_ascii=False)}\n\n"
                "Merge any that are semantically equivalent or heavily overlapping. "
                "Return the final deduplicated list as a JSON array of strings and nothing else."
            ),
        }],
    )
    raw = next(b.text for b in response.content if b.type == "text")
    start, end = raw.find("["), raw.rfind("]") + 1
    return [str(l) for l in json.loads(raw[start:end])]


def assign_labels_batch(
    sentences: list[str],
    labels: list[str],
    client: anthropic.Anthropic,
    poll_interval: int = 15,
) -> list[str]:
    """Step 3 — classify every sentence using the Messages Batches API.

    The system prompt (containing the label list) is marked with cache_control
    so it is cached once and reused across every request in the batch.
    """
    labels_block = "\n".join(f"- {l}" for l in labels)
    system_text = _ASSIGN_SYSTEM_PREFIX + labels_block

    requests = [
        Request(
            custom_id=str(i),
            params=MessageCreateParamsNonStreaming(
                model=MODEL,
                max_tokens=64,
                system=[{
                    "type": "text",
                    "text": system_text,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": sentence}],
            ),
        )
        for i, sentence in enumerate(sentences)
    ]

    print(f"  Submitting batch of {len(requests)} classification requests...")
    batch = client.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}")

    while True:
        batch = client.messages.batches.retrieve(batch.id)
        c = batch.request_counts
        print(
            f"  [{batch.processing_status}] "
            f"processing={c.processing} succeeded={c.succeeded} errored={c.errored}"
        )
        if batch.processing_status == "ended":
            break
        time.sleep(poll_interval)

    # Build a label lookup for exact matching (case-insensitive)
    label_lower = {l.strip().lower(): l for l in labels}

    assignments: list[str] = ["Other"] * len(sentences)
    for result in client.messages.batches.results(batch.id):
        idx = int(result.custom_id)
        if result.result.type == "succeeded":
            msg = result.result.message
            text = next(
                (b.text.strip() for b in msg.content if b.type == "text"), "Other"
            )
            # Exact match first, then case-insensitive, then "Other"
            if text in labels:
                assignments[idx] = text
            elif text.strip().lower() in label_lower:
                assignments[idx] = label_lower[text.strip().lower()]
            # else stays "Other"
    return assignments


def run(
    output_dir: str,
    input_path: Optional[str] = None,
    text_column: str = "claim_text",
    hf_dataset: str = HF_DATASET,
    hf_split: str = "train",
    hf_text_column: str = HF_TEXT_COLUMN,
    sentence_split: bool = True,
    spacy_model: str = "fr_core_news_sm",
    n_labels: int = DEFAULT_N_LABELS,
    n_sample: int = DEFAULT_N_SAMPLE,
    seeds: list[str] = SEED_LABELS,
    skip_merge: bool = False,
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    client = _build_client()

    # --- Load ---
    if input_path:
        print(f"Loading claims from {input_path}...")
        raw_texts = load_claims(input_path, text_column)
        if sentence_split:
            print(f"Splitting into sentences (spaCy model: {spacy_model})...")
            tmp = pd.DataFrame({"sentence": raw_texts}).rename(
                columns={"sentence": hf_text_column}
            )
            sentences_df = split_sentences(tmp, text_column=hf_text_column, spacy_model=spacy_model)
        else:
            sentences_df = pd.DataFrame({"sentence": raw_texts})
    else:
        print(f"Loading texts from HuggingFace: {hf_dataset} ({hf_split})...")
        docs_df = load_from_hf(hf_dataset, hf_split, hf_text_column)
        print(f"  {len(docs_df)} documents loaded.")
        if sentence_split:
            print(f"Splitting into sentences (spaCy model: {spacy_model})...")
            sentences_df = split_sentences(docs_df, text_column=hf_text_column, spacy_model=spacy_model)
            print(f"  {len(sentences_df)} sentences after splitting.")
        else:
            sentences_df = docs_df.rename(columns={hf_text_column: "sentence"})

    sentences = sentences_df["sentence"].tolist()
    print(f"  {len(sentences)} sentences to cluster.")

    # --- Step 1: generate candidate labels ---
    print(f"\nStep 1: Generating labels from {n_sample} sampled sentences "
          f"(seeds={len(seeds)}, target total={n_labels})...")
    labels = generate_labels(sentences, client, n_sample=n_sample, n_labels=n_labels, seeds=seeds)
    print(f"  {len(labels)} labels generated: {labels}")

    # --- Step 2: merge similar labels ---
    if not skip_merge:
        print("\nStep 2: Merging semantically similar labels...")
        labels = merge_labels(labels, client)
        print(f"  {len(labels)} labels after merging: {labels}")

    labels_with_other = labels + ["Other"]
    (out / "labels.json").write_text(
        json.dumps(labels_with_other, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  Labels saved to {out / 'labels.json'}")

    # --- Step 3: classify via Batches API ---
    print(f"\nStep 3: Classifying {len(sentences)} sentences via Batches API...")
    assignments = assign_labels_batch(sentences, labels, client)

    # --- Attach assignments to sentence metadata ---
    sentences_df = sentences_df.copy()
    sentences_df["topic_label"] = assignments
    label_to_id = {l: i for i, l in enumerate(labels_with_other)}
    sentences_df["topic_id"] = sentences_df["topic_label"].map(label_to_id)

    # --- Cluster summary ---
    topic_counts = (
        sentences_df.groupby("topic_label")
        .size()
        .rename("Count")
        .reset_index()
    )

    if HF_ID_COLUMN in sentences_df.columns:
        # Deduplicate by (document, topic) before summing mesinfo so that a
        # document contributing multiple sentences to the same cluster is only
        # counted once.
        doc_topic = sentences_df[
            [HF_ID_COLUMN, "topic_label", "mesinfo_correct", "mesinfo_incorrect"]
        ].drop_duplicates(subset=[HF_ID_COLUMN, "topic_label"])

        cluster_mesinfo = (
            doc_topic.groupby("topic_label")[["mesinfo_correct", "mesinfo_incorrect"]]
            .sum()
            .rename(columns={
                "mesinfo_correct": "sum_mesinfo_correct",
                "mesinfo_incorrect": "sum_mesinfo_incorrect",
            })
            .reset_index()
        )
        topic_summary = topic_counts.merge(cluster_mesinfo, on="topic_label", how="left")
    else:
        topic_summary = topic_counts

    # --- Save ---
    sentences_path = out / "sentences_with_topics.csv"
    sentences_df.to_csv(sentences_path, index=False)
    print(f"\nSentence-level results saved to {sentences_path}")

    summary_path = out / "topic_summary.csv"
    topic_summary.to_csv(summary_path, index=False)
    print(f"Topic summary (with mesinfo sums) saved to {summary_path}")

    print("\n--- Topic summary ---")
    print(topic_summary.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "LLM-based text clustering with Claude Haiku "
            "(arXiv:2410.00927 — 'Text Clustering as Classification with LLMs')."
        )
    )
    # Local file input (optional — defaults to HuggingFace)
    parser.add_argument(
        "--input",
        help="Path to a local claims file (.json or .csv). "
             "If omitted, loads from --hf-dataset.",
    )
    parser.add_argument(
        "--text-column", default="claim_text",
        help="Column/key for claim text in a local file. Default: claim_text",
    )
    # HuggingFace source
    parser.add_argument(
        "--hf-dataset", default=HF_DATASET,
        help=f"HuggingFace dataset name. Default: {HF_DATASET}",
    )
    parser.add_argument(
        "--hf-split", default="train",
        help="HuggingFace dataset split. Default: train",
    )
    parser.add_argument(
        "--hf-text-column", default=HF_TEXT_COLUMN,
        help=f"Text field to extract from the HF dataset. Default: {HF_TEXT_COLUMN}",
    )
    # Sentence splitting
    parser.add_argument(
        "--no-sentence-split", action="store_true",
        help="Skip sentence splitting and cluster full texts as-is.",
    )
    parser.add_argument(
        "--spacy-model", default="fr_core_news_sm",
        help="spaCy model for sentence segmentation. Default: fr_core_news_sm",
    )
    # Clustering parameters
    parser.add_argument(
        "--n-labels", type=int, default=DEFAULT_N_LABELS,
        help=f"Target total number of labels (seeds + new). Default: {DEFAULT_N_LABELS}",
    )
    parser.add_argument(
        "--n-sample", type=int, default=DEFAULT_N_SAMPLE,
        help=f"Number of sentences sampled for label generation. Default: {DEFAULT_N_SAMPLE}",
    )
    parser.add_argument(
        "--seeds-file",
        help="Path to a JSON file containing a list of seed label strings. "
             "Overrides the built-in SEED_LABELS constant.",
    )
    parser.add_argument(
        "--no-seeds", action="store_true",
        help="Disable seed labels entirely; let the LLM generate all labels from scratch.",
    )
    parser.add_argument(
        "--skip-merge", action="store_true",
        help="Skip the label-merging step (step 2).",
    )
    # Output
    parser.add_argument(
        "--output-dir", default="./bertopic_llm_output",
        help="Directory for output files. Default: ./bertopic_llm_output",
    )
    args = parser.parse_args()

    if args.no_seeds:
        seeds = []
    elif args.seeds_file:
        seeds = json.loads(Path(args.seeds_file).read_text(encoding="utf-8"))
    else:
        seeds = SEED_LABELS

    run(
        output_dir=args.output_dir,
        input_path=args.input,
        text_column=args.text_column,
        hf_dataset=args.hf_dataset,
        hf_split=args.hf_split,
        hf_text_column=args.hf_text_column,
        sentence_split=not args.no_sentence_split,
        spacy_model=args.spacy_model,
        n_labels=args.n_labels,
        n_sample=args.n_sample,
        seeds=seeds,
        skip_merge=args.skip_merge,
    )
