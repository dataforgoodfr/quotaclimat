import argparse
import json
import pandas as pd
import numpy as np
import spacy
from pathlib import Path
from typing import Optional

from rrs.utils.generate_id import get_consistent_hash

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance
from bertopic.vectorizers import ClassTfidfTransformer
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from rrs.clustering.get_data import get_data_from_db

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
    ["ZFE", "produisent", "aucun effet écologique", "réduisent pas", "émissions"],
    ["énergies", "renouvelables", "variables", "exploser", "prix", "électricité"],
    ["voitures thermiques" "pas" "problème", "environnement", "récentes"],
    ["agriculture", "élevage", "inoffensifs", "bons", "environnement"],
    ["réchauffement climatique", "scientifiques", "désaccord"],
    ["énergies", "renouvelables", "blackouts", "sécurité"],
    ["climat", "fluctué", "naturelle"],
    ["France",  "moins", "gaz", "effet de serre"],
    ["éoliennes", "négatif", "désastre",  "biodiversité"],
    ["voitures", "électriques",  "polluent"],
    ["Réduire", "émissions", "aucun impact", "climat"],
    ["énergies", "renouvelables", "inefficaces", "inutiles", "intermittence"],
    ["décarbonation", "intérêts", "financiers"],
    ["données", "scientifiques", "changement climatique", "falsifiées", "exagérées", "manipuler"],
    ["origine", "réchauffement", "climatique", "incertaine", "insignifiante"],
    ["France", "nucléaire", "suffit", "décarboné"],
    ["climatisation", "bonne", "solution", "adaptation"],
    ["soutien", "État", "énergies", "renouvelables"],
]
# SEED_TOPIC_LIST = [
#     ["renouvelables", "augmentent", "coût", "électricité"],
#     ["inutile", "réduire", "rejets", "gaz effet de serre", "France"],
#     ["élevage", "neutre", "avantageux", "climat"],
#     ["voitures électriques", "polluent plus", "voitures thermiques"],
# ]

TEXT_COLUMN = "text"
ID_COLUMN = "case_id"


def load_from_db(
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    """Load data from the RRS database and return a DataFrame."""
    df = get_data_from_db(start_date=start_date, end_date=end_date)
    print(df.head())
    df = df[df[TEXT_COLUMN].notna() & (df[TEXT_COLUMN] != "")]
    return df.reset_index(drop=True)


def split_sentences(
    df: pd.DataFrame,
    text_column: str = TEXT_COLUMN,
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
) -> pd.DataFrame:
    """Expand each row into overlapping sentence-window chunks.

    Each chunk contains *window_size* consecutive sentences joined by a space.
    *overlap_tokens* controls how many tokens consecutive chunks share: the
    algorithm walks back from the end of the current window, accumulating
    sentences until their combined token count reaches *overlap_tokens*, then
    starts the next window at that sentence.  Overlap is therefore always a
    whole-sentence boundary — no sentence is ever split mid-way.

    Short fragments (< 20 chars) are filtered before windowing.
    Metadata is taken from the source document row (never from individual
    sentences) so that case_id remains document-scoped.
    Windows never cross document boundaries.

    window_size=1, overlap_tokens=0 (defaults) → one sentence per row (original behaviour).
    """
    try:
        nlp = spacy.load(spacy_model, disable=["ner", "lemmatizer"])
    except OSError:
        print(f"spaCy model '{spacy_model}' not found — downloading...")
        spacy.cli.download(spacy_model)
        nlp = spacy.load(spacy_model, disable=["ner", "lemmatizer"])

    meta_cols = [c for c in [ID_COLUMN] if c in df.columns]
    rows = []
    texts = df[text_column].tolist()
    meta = df[meta_cols].to_dict("records")

    for doc, record in zip(nlp.pipe(texts, batch_size=64), meta):
        # Keep (text, token_count) for each sentence that passes the length filter.
        sentences: list[tuple[str, int]] = [
            (span.text.strip(), len(span))
            for span in doc.sents
            if len(span.text.strip()) >= 20
        ]
        if not sentences:
            continue

        i = 0
        while i < len(sentences):
            window = sentences[i: i + window_size]
            rows.append({"sentence": " ".join(t for t, _ in window), **record})

            if overlap_tokens == 0:
                i += window_size
            else:
                # Walk back from the end of the window until we have accumulated
                # at least overlap_tokens tokens — that suffix becomes the prefix
                # of the next window.
                token_sum = 0
                suffix_len = 0
                for _, n_tok in reversed(window):
                    token_sum += n_tok
                    suffix_len += 1
                    if token_sum >= overlap_tokens:
                        break
                # Advance by (window sentences - overlap suffix), at least 1.
                i += max(1, len(window) - suffix_len)

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
    nr_topics: Optional[int] = None,
    representation: str = "keybert",
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
        min_samples=1,        # Lower = more points rescued from noise
        metric="euclidean",
        cluster_selection_method="leaf",  # "leaf" produces more, finer-grained clusters than "eom"
        prediction_data=True, # Required for soft-clustering / new doc inference
    )

    vectorizer_model = CountVectorizer(
        ngram_range=(1, 3),
        stop_words=FRENCH_STOPWORDS,
        # Require 3+ character tokens — eliminates 2-char function words (le, de,
        # la, et, il, on, ...) before stop_words matching even runs.
        token_pattern=r"(?u)\b[^\d\W]{3,}\b",
        min_df=2,             # Ignore terms that appear in fewer than 2 docs
    )

    ctfidf_model = ClassTfidfTransformer(
        reduce_frequent_words=True,  # Down-weights words common across all topics
    )

    if representation == "keybert":
        representation_model = KeyBERTInspired()
    elif representation == "mmr":
        representation_model = MaximalMarginalRelevance(diversity=0.3)
    else:
        representation_model = None

    model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        ctfidf_model=ctfidf_model,
        representation_model=representation_model,
        seed_topic_list=SEED_TOPIC_LIST,
        nr_topics=nr_topics,  # None = no merging; int = merge down to that many topics
        calculate_probabilities=True,
        verbose=True,
    )

    return model


# ---------------------------------------------------------------------------
# 4. Threshold-based automatic topic merging
# ---------------------------------------------------------------------------

def merge_similar_topics(
    model: BERTopic,
    docs: list[str],
    threshold: float = 0.92,
) -> BERTopic:
    """Merge topic pairs whose embedding cosine similarity exceeds *threshold*.

    Unlike nr_topics="auto" (fixed internal threshold, opaque) this lets you
    tune aggressiveness directly:
      - 0.95+ : only near-duplicate topics are merged (conservative)
      - 0.90  : similar but not identical topics merged (BERTopic "auto" territory)
      - 0.85  : more aggressive — use when you still have too many topics
    """
    embeddings = np.array(model.topic_embeddings_)
    # Index 0 is the outlier topic (-1); skip it
    topic_ids = [t for t in model.get_topics() if t != -1]
    if len(topic_ids) < 2:
        return model

    # Embeddings for real topics only (offset by 1 because index 0 = outlier)
    real_embeddings = embeddings[[t + 1 for t in topic_ids]]
    sim = cosine_similarity(real_embeddings)

    merged = set()
    groups: list[list[int]] = []

    for i, ti in enumerate(topic_ids):
        if ti in merged:
            continue
        group = [ti]
        for j, tj in enumerate(topic_ids):
            if i >= j or tj in merged:
                continue
            if sim[i, j] >= threshold:
                group.append(tj)
                merged.add(tj)
        merged.add(ti)
        if len(group) > 1:
            groups.append(group)

    if not groups:
        print(f"  No topic pairs exceed similarity threshold {threshold:.2f} — nothing merged.")
        return model

    print(f"  Merging {len(groups)} group(s) of similar topics (threshold={threshold:.2f}):")
    for g in groups:
        print(f"    {g}")
    model.merge_topics(docs, groups)
    return model


# ---------------------------------------------------------------------------
# 5. Run & save results
# ---------------------------------------------------------------------------

def run(
    output_dir: str,
    input_path: Optional[str] = None,
    text_column: str = TEXT_COLUMN,
    sentence_split: bool = True,
    spacy_model: str = "fr_core_news_sm",
    window_size: int = 1,
    overlap_tokens: int = 0,
    nr_topics: Optional[int] = None,
    merge_threshold: Optional[float] = 0.92,
    year: Optional[int] = 2025,
    month: Optional[int] = 7,
    reduce_outliers_strategy: Optional[str] = "distributions",
    reduce_outliers_threshold: float = 0.0,
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
                sentences_df.rename(columns={"sentence": text_column}),
                text_column=text_column,
                spacy_model=spacy_model,
                window_size=window_size,
                overlap_tokens=overlap_tokens,
            )
    else:
        print("Loading texts from database...")
        docs_df = load_from_db()
        if year is not None or month is not None:
            before = len(docs_df)
            dates = pd.to_datetime(docs_df["start"], errors="coerce", utc=True)
            mask = pd.Series(True, index=docs_df.index)
            if year is not None:
                mask &= dates.dt.year == year
            if month is not None:
                mask &= dates.dt.month == month
            docs_df = docs_df[mask]
            label = f"year={year}, month={month}"
            print(f"  Date filter ({label}): {len(docs_df)}/{before} rows kept.")
        print(f"  {len(docs_df)} documents loaded.")
        if sentence_split:
            print(f"Splitting into sentences (spaCy model: {spacy_model}, "
                  f"window={window_size}, overlap_tokens={overlap_tokens})...")
            sentences_df = split_sentences(
                docs_df,
                text_column=text_column,
                spacy_model=spacy_model,
                window_size=window_size,
                overlap_tokens=overlap_tokens,
            )
            print(f"  {len(sentences_df)} chunks after splitting.")
        else:
            sentences_df = docs_df.rename(columns={text_column: "sentence"})

    claims = sentences_df["sentence"].tolist()
    print(f"  {len(claims)} claims to cluster.")

    # --- Fit ---
    print("Building and fitting BERTopic model...")
    model = build_model(
        n_neighbors=20,
        min_cluster_size=5,
        embedding_model="dangvantuan/sentence-camembert-large",
        nr_topics=nr_topics,
    )
    topics, probs = model.fit_transform(claims)

    # --- Threshold-based automatic merging ---
    if merge_threshold is not None:
        n_before = len([t for t in model.get_topics() if t != -1])
        print(f"\nAuto-merging similar topics (threshold={merge_threshold:.2f}, topics before: {n_before})...")
        model = merge_similar_topics(model, claims, threshold=merge_threshold)
        topics = model.topics_
        probs = model.probabilities_
        n_after = len([t for t in model.get_topics() if t != -1])
        print(f"  Topics after merging: {n_after}")

    # --- Reduce outliers ---
    if reduce_outliers_strategy is not None:
        n_outliers_before = sum(t == -1 for t in topics)
        print(f"\nReducing outliers (strategy='{reduce_outliers_strategy}', "
              f"threshold={reduce_outliers_threshold}, outliers before: {n_outliers_before})...")
        new_topics = model.reduce_outliers(
            claims,
            topics,
            strategy=reduce_outliers_strategy,
            threshold=reduce_outliers_threshold,
        )
        model.update_topics(
            claims,
            topics=new_topics,
            vectorizer_model=model.vectorizer_model,
            representation_model=model.representation_model,
        )
        topics = new_topics
        n_outliers_after = sum(t == -1 for t in topics)
        print(f"  Outliers after: {n_outliers_after}")

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
    print()

    # --- Save CSVs ---
    csv_path = out / "sentences_with_topics.csv"
    sentences_df.to_csv(csv_path, index=False)
    print(f"\nSentence-level results saved to {csv_path}")

    topic_csv_path = out / "topic_summary.csv"
    topic_info.to_csv(topic_csv_path, index=False)
    print(f"Topic summary saved to {topic_csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seeded BERTopic clustering for French disinformation claims."
    )
    parser.add_argument(
        "--input",
        help="Path to local claims file (.json or .csv). If omitted, loads from the database."
    )
    parser.add_argument(
        "--text-column", default="claim_text",
        help="Column/key name for claim text in a local file. Default: claim_text"
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
    parser.add_argument(
        "--window-size", type=int, default=3,
        help="Number of sentences per chunk. Default: 1 (no windowing)."
    )
    parser.add_argument(
        "--overlap-tokens", type=int, default=30,
        help="Number of tokens shared between consecutive windows (whole-sentence granularity). Default: 0."
    )
    # Output
    parser.add_argument(
        "--output-dir", default="./bertopic_output",
        help="Directory for output files. Default: ./bertopic_output"
    )
    parser.add_argument(
        "--nr-topics", type=int, default=None,
        help="Merge topics down to this many after fitting. Omit to use --merge-threshold instead."
    )
    parser.add_argument(
        "--merge-threshold", type=float, default=0.92,
        help="Cosine similarity threshold for automatic topic merging (0–1). "
             "Higher = less merging, more topics. Use --no-merge to disable. Default: 0.92"
    )
    parser.add_argument(
        "--no-merge", action="store_true",
        help="Disable all automatic merging after fitting."
    )
    # Outlier reduction
    parser.add_argument(
        "--reduce-outliers-strategy", default="distributions",
        choices=["distributions", "embeddings", "c-tf-idf", "topic_similarity"],
        help="Strategy for reduce_outliers(). Default: distributions."
    )
    parser.add_argument(
        "--reduce-outliers-threshold", type=float, default=0.0,
        help="Minimum probability for outlier reassignment (0–1). Default: 0.0 (reassign all)."
    )
    parser.add_argument(
        "--no-reduce-outliers", action="store_true",
        help="Disable outlier reduction entirely."
    )
    parser.add_argument(
        "--year", type=int, default=2025,
        help="Keep only records whose start falls in this year. Default: 2025. "
             "Pass 0 to disable year filtering."
    )
    parser.add_argument(
        "--month", type=int, default=7,
        help="Keep only records whose start falls in this month (1–12). Default: 7 (July). "
             "Pass 0 to disable month filtering."
    )
    args = parser.parse_args()

    run(
        output_dir=args.output_dir,
        input_path=args.input,
        text_column=args.text_column,
        sentence_split=not args.no_sentence_split,
        spacy_model=args.spacy_model,
        window_size=args.window_size,
        overlap_tokens=args.overlap_tokens,
        nr_topics=args.nr_topics,
        merge_threshold=None if args.no_merge else args.merge_threshold,
        year=args.year or None,
        month=args.month or None,
        reduce_outliers_strategy=None if args.no_reduce_outliers else args.reduce_outliers_strategy,
        reduce_outliers_threshold=args.reduce_outliers_threshold,
    )