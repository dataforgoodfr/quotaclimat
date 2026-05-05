import argparse
import json
import pandas as pd
from pathlib import Path
from typing import Optional

from bertopic import BERTopic
from bertopic.vectorizers import ClassTfidfTransformer
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer

# French stopwords — spacy is the cleanest source for French.
# Falls back to a hardcoded minimal set if spacy is not installed.
try:
    from spacy.lang.fr.stop_words import STOP_WORDS as _fr_stopwords
    FRENCH_STOPWORDS = list(_fr_stopwords)
except ImportError:
    print(
        "Warning: spacy not found. Using a minimal French stopword list.\n"
        "For better results: pip install spacy && python -m spacy download fr_core_news_sm"
    )
    FRENCH_STOPWORDS = [
        "le", "la", "les", "un", "une", "des", "de", "du", "et", "en",
        "à", "au", "aux", "ce", "se", "on", "il", "elle", "ils", "elles",
        "nous", "vous", "je", "tu", "que", "qui", "ne", "pas", "plus",
        "par", "sur", "dans", "avec", "est", "sont", "été", "être",
        "avoir", "fait", "tout", "mais", "ou", "donc", "or", "ni", "car",
    ]

SEED_TOPIC_LIST = [
    # Fraude électorale
    ["élection volée", "fraude électorale", "scrutin truqué", "bulletins falsifiés",
     "machines à voter", "résultats manipulés", "vote frauduleux"],
    # Désinformation vaccinale
    ["vaccin ARNm", "effets secondaires", "vaccin expérimental", "obligation vaccinale",
     "micropuce vaccin", "vaccin mortel", "pass sanitaire"],
    # Immigration / théorie du grand remplacement
    ["grand remplacement", "invasion migratoire", "submersion migratoire",
     "frontières ouvertes", "immigration illégale", "remplacés", "identité nationale"],
    # Désinformation sanitaire / pandémie
    ["virus laboratoire", "pandémie artificielle", "gripette", "masques inutiles",
     "ivermectine", "hydroxychloroquine", "complot sanitaire", "5G coronavirus"],
    # Climatoscepticisme / énergie
    ["réchauffement climatique mensonge", "hoax climatique", "agenda vert",
     "énergies renouvelables coût", "prix électricité", "transition énergétique faillite",
     "CO2 naturel", "climategate"],
    # État profond / complot globaliste
    ["état profond", "mondialistes", "nouvel ordre mondial", "élites corrompues",
     "gouvernement mondial", "agenda caché", "WEF complot", "Klaus Schwab"],
]

HF_DATASET = "DataForGood/climateguard-training"
HF_TEXT_COLUMN = "data_item_plaintext"
HF_ID_COLUMN = "task_completion_aggregate_id"
HF_META_COLUMNS = [HF_ID_COLUMN, "mesinfo_correct", "mesinfo_incorrect"]


def _get_hf_token() -> Optional[str]:
    from dotenv import load_dotenv
    import os
    load_dotenv(Path(__file__).resolve().parents[4] / ".env")
    return os.getenv("HF_TOKEN")


def load_from_hf(
    dataset_name: str = HF_DATASET,
    split: str = "train",
    text_column: str = HF_TEXT_COLUMN,
) -> pd.DataFrame:
    """Load the HuggingFace dataset and return a DataFrame with the text and metadata columns."""
    from datasets import load_dataset  # optional dep; imported lazily

    token = _get_hf_token()
    ds = load_dataset(dataset_name, split=split, token=token)

    missing = [c for c in [text_column] + HF_META_COLUMNS if c not in ds.column_names]
    if missing:
        raise ValueError(
            f"Columns not found: {missing}. Available: {ds.column_names}"
        )

    df = ds.select_columns([text_column] + HF_META_COLUMNS).to_pandas()
    df = df[df[text_column].notna() & (df[text_column] != "")]
    # float16 → float32 to avoid pandas arithmetic surprises
    for col in ["mesinfo_correct", "mesinfo_incorrect"]:
        df[col] = df[col].astype("float32")
    return df.reset_index(drop=True)


def split_sentences(
    df: pd.DataFrame,
    text_column: str = HF_TEXT_COLUMN,
    spacy_model: str = "fr_core_news_sm",
) -> pd.DataFrame:
    """Expand each row into one row per sentence, carrying metadata columns forward.

    Returns a DataFrame with columns: sentence, task_completion_aggregate_id,
    mesinfo_correct, mesinfo_incorrect.  Short fragments (< 20 chars) are
    dropped to avoid noise from headers/timestamps/etc.
    """
    import spacy

    try:
        nlp = spacy.load(spacy_model, disable=["ner", "lemmatizer"])
    except OSError:
        print(f"spaCy model '{spacy_model}' not found — downloading...")
        spacy.cli.download(spacy_model)
        nlp = spacy.load(spacy_model, disable=["ner", "lemmatizer"])

    meta_cols = [c for c in HF_META_COLUMNS if c in df.columns]
    rows = []
    texts = df[text_column].tolist()
    meta = df[meta_cols].to_dict("records")

    for doc, record in zip(nlp.pipe(texts, batch_size=64), meta):
        for sent in doc.sents:
            text = sent.text.strip()
            if len(text) >= 20:
                rows.append({"sentence": text, **record})

    return pd.DataFrame(rows)


def load_claims(path: str, text_column: str = "claim_text") -> list[str]:
    """Load claims from a JSON or CSV file."""
    p = Path(path)

    if p.suffix == ".json":
        with open(p) as f:
            data = json.load(f)
        # Accept either a list of strings or a list of dicts
        if isinstance(data, list):
            if isinstance(data[0], str):
                return data
            elif isinstance(data[0], dict):
                return [d[text_column] for d in data]
        raise ValueError("JSON must be a list of strings or list of dicts.")

    elif p.suffix == ".csv":
        df = pd.read_csv(p)
        if text_column not in df.columns:
            raise ValueError(
                f"Column '{text_column}' not found. Available: {list(df.columns)}"
            )
        return df[text_column].dropna().tolist()

    else:
        raise ValueError(f"Unsupported file type: {p.suffix}. Use .json or .csv")


# ---------------------------------------------------------------------------
# 3. Build the BERTopic model
# ---------------------------------------------------------------------------

def build_model(
    n_neighbors: int = 10,
    min_cluster_size: int = 5,
    embedding_model: str = "dangvantuan/sentence-camembert-large",
) -> BERTopic:
    """
    Assemble the BERTopic pipeline tuned for French disinformation claims.

    Embedding model choice:
    - "dangvantuan/sentence-camembert-large"  ← default, best semantic precision
      for French. Fine-tuned from CamemBERT on French sentence pairs.
    - "paraphrase-multilingual-mpnet-base-v2" ← swap in if you later add claims
      in other languages, or if CamemBERT is too slow on your hardware.

    Other key choices:
    - UMAP n_neighbors=10: smaller → finer local structure, good for short texts
    - HDBSCAN min_cluster_size=5: low threshold so small but coherent claim
      clusters aren't merged into noise (-1)
    - ClassTfidfTransformer(reduce_frequent_words=True): suppresses near-
      universal words that would otherwise dominate topic keywords
    - CountVectorizer ngram_range=(1,2): captures bigrams like "fraude électorale"
    - French stopwords via spacy (falls back to minimal list if spacy missing)
    """

    umap_model = UMAP(
        n_neighbors=n_neighbors,
        n_components=5,
        min_dist=0.0,        # 0.0 = tighter clusters (better for clustering)
        metric="cosine",
        random_state=42,
    )

    hdbscan_model = HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=3,        # Lower = more points rescued from noise
        metric="euclidean",
        cluster_selection_method="eom",
        prediction_data=True, # Required for soft-clustering / new doc inference
    )

    vectorizer_model = CountVectorizer(
        ngram_range=(1, 2),
        stop_words=FRENCH_STOPWORDS,
        min_df=2,             # Ignore terms that appear in fewer than 2 docs
    )

    ctfidf_model = ClassTfidfTransformer(
        reduce_frequent_words=True,  # Down-weights words common across all topics
    )

    model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        ctfidf_model=ctfidf_model,
        seed_topic_list=SEED_TOPIC_LIST,
        nr_topics="auto",    # Automatically merge overly similar topics post-hoc
        calculate_probabilities=True,
        verbose=True,
    )

    return model


# ---------------------------------------------------------------------------
# 4. Run & save results
# ---------------------------------------------------------------------------

def run(
    output_dir: str,
    input_path: Optional[str] = None,
    text_column: str = "claim_text",
    hf_dataset: str = HF_DATASET,
    hf_split: str = "train",
    hf_text_column: str = HF_TEXT_COLUMN,
    sentence_split: bool = True,
    spacy_model: str = "fr_core_news_sm",
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # --- Load ---
    if input_path:
        print(f"Loading claims from {input_path}...")
        raw_texts = load_claims(input_path, text_column)
        sentences_df = pd.DataFrame({"sentence": raw_texts})
        if sentence_split:
            print(f"Splitting into sentences (spaCy model: {spacy_model})...")
            sentences_df = split_sentences(
                sentences_df.rename(columns={"sentence": hf_text_column}),
                text_column=hf_text_column,
                spacy_model=spacy_model,
            )
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

    claims = sentences_df["sentence"].tolist()
    print(f"  {len(claims)} claims to cluster.")

    # --- Fit ---
    print("Building and fitting BERTopic model...")
    model = build_model()
    topics, probs = model.fit_transform(claims)

    # --- Attach topic assignments to sentence metadata ---
    sentences_df = sentences_df.copy()
    sentences_df["topic_id"] = topics
    sentences_df["top_probability"] = [
        float(p.max()) if hasattr(p, "max") else float(p) for p in probs
    ]
    sentences_df["topic_label"] = [
        model.get_topic(t)[0][0] if t != -1 else "OUTLIER" for t in topics
    ]
    sentences_df["topic_keywords"] = [
        ", ".join([w for w, _ in model.get_topic(t)[:5]]) if t != -1 else ""
        for t in topics
    ]

    # --- Topic summary ---
    topic_info = model.get_topic_info()
    print("\n--- Topic summary ---")
    print(topic_info[["Topic", "Count", "Name"]].to_string(index=False))

    # --- Per-document mesinfo sums per cluster ---
    # Deduplicate by (topic_id, task_completion_aggregate_id) so that documents
    # contributing multiple sentences to the same cluster are counted only once.
    if HF_ID_COLUMN in sentences_df.columns:
        doc_topic = (
            sentences_df[[HF_ID_COLUMN, "topic_id", "mesinfo_correct", "mesinfo_incorrect"]]
            .drop_duplicates(subset=[HF_ID_COLUMN, "topic_id"])
        )
        cluster_mesinfo = (
            doc_topic.groupby("topic_id")[["mesinfo_correct", "mesinfo_incorrect"]]
            .sum()
            .rename(columns={
                "mesinfo_correct": "sum_mesinfo_correct",
                "mesinfo_incorrect": "sum_mesinfo_incorrect",
            })
            .reset_index()
        )
        topic_info = topic_info.merge(cluster_mesinfo, left_on="Topic", right_on="topic_id", how="left")
        topic_info.drop(columns=["topic_id"], inplace=True)

    # --- Save CSVs ---
    csv_path = out / "sentences_with_topics.csv"
    sentences_df.to_csv(csv_path, index=False)
    print(f"\nSentence-level results saved to {csv_path}")

    topic_csv_path = out / "topic_summary.csv"
    topic_info.to_csv(topic_csv_path, index=False)
    print(f"Topic summary (with mesinfo sums) saved to {topic_csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seeded BERTopic clustering for French disinformation claims."
    )
    # Local file input (optional — if omitted, loads from HuggingFace)
    parser.add_argument(
        "--input",
        help="Path to local claims file (.json or .csv). "
             "If omitted, loads from --hf-dataset instead."
    )
    parser.add_argument(
        "--text-column", default="claim_text",
        help="Column/key name for claim text in a local file. Default: claim_text"
    )
    # HuggingFace source
    parser.add_argument(
        "--hf-dataset", default=HF_DATASET,
        help=f"HuggingFace dataset name. Default: {HF_DATASET}"
    )
    parser.add_argument(
        "--hf-split", default="train",
        help="HuggingFace dataset split. Default: train"
    )
    parser.add_argument(
        "--hf-text-column", default=HF_TEXT_COLUMN,
        help=f"Field to extract from the HF dataset. Default: {HF_TEXT_COLUMN}"
    )
    # Sentence splitting
    parser.add_argument(
        "--no-sentence-split", action="store_true",
        help="Skip sentence splitting and cluster full texts as-is."
    )
    parser.add_argument(
        "--spacy-model", default="fr_core_news_sm",
        help="spaCy model for sentence segmentation. Default: fr_core_news_sm"
    )
    # Output
    parser.add_argument(
        "--output-dir", default="./bertopic_output",
        help="Directory for output files. Default: ./bertopic_output"
    )
    args = parser.parse_args()

    run(
        output_dir=args.output_dir,
        input_path=args.input,
        text_column=args.text_column,
        hf_dataset=args.hf_dataset,
        hf_split=args.hf_split,
        hf_text_column=args.hf_text_column,
        sentence_split=not args.no_sentence_split,
        spacy_model=args.spacy_model,
    )