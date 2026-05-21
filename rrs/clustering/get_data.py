import os
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import spacy

import psycopg
from dotenv import load_dotenv

from rrs.utils.generate_id import get_consistent_hash

CLIMATE_SUBJECT_ID = get_consistent_hash("climate")
TEXT_COLUMN = "text"
ID_COLUMN = "case_id"

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def _conninfo() -> str:
    return (
        f"host={os.getenv('RRS_PG_HOST', 'localhost')} "
        f"port={os.getenv('RRS_PG_PORT', 5432)} "
        f"dbname={os.getenv('RRS_PG_DATABASE', 'rrs_db')} "
        f"user={os.getenv('RRS_PG_USER', 'user')} "
        f"password={os.getenv('RRS_PG_PASSWORD', 'supersecret')}"
    )


def get_data_from_db(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    conditions = []
    params: list = []
    if start_date is not None:
        conditions.append("c.start >= %s")
        params.append(start_date)
    if end_date is not None:
        conditions.append("c.start < %s")
        params.append(end_date + timedelta(days=1))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT
            c.case_id,
            c.segment_id,
            c.start,
            c.text
        FROM cases c
        {where}
    """
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)


def get_clusters_from_db(
    active_since: Optional[date] = None,
) -> pd.DataFrame:
    """Return clusters from the database.

    If active_since is given, only clusters that have had at least one case
    assigned on or after that date are returned.
    Columns: cluster_id, cluster_text.
    """
    if active_since is not None:
        query = """
            SELECT cl.cluster_id, cl.cluster_text
            FROM clusters cl
            JOIN case_to_clusters ctc ON ctc.cluster_id = cl.cluster_id
            JOIN cases c ON c.case_id = ctc.case_id
            GROUP BY cl.cluster_id, cl.cluster_text
            HAVING MAX(c.start) >= %s
        """
        params: list = [active_since]
    else:
        query = "SELECT cluster_id, cluster_text FROM clusters"
        params = []
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)


def write_clusters_to_db(assignments_df: pd.DataFrame) -> None:
    """Upsert clusters and case→cluster mappings into the database.

    Expects assignments_df to have at least: case_id, cluster_id, cluster_text.
    """
    if assignments_df.empty:
        print("  No assignments to write to DB.")
        return

    clusters = (
        assignments_df[["cluster_id", "cluster_text"]]
        .dropna(subset=["cluster_id"])
        .drop_duplicates(subset=["cluster_id"])
    )
    mappings = (
        assignments_df[["case_id", "cluster_id"]]
        .dropna(subset=["case_id", "cluster_id"])
        .drop_duplicates()
    )

    case_ids = mappings["case_id"].unique().tolist()

    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO clusters (cluster_id, subject_id, cluster_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (cluster_id) DO UPDATE
                    SET subject_id = EXCLUDED.subject_id,
                        cluster_text = EXCLUDED.cluster_text
                """,
                [
                    (row.cluster_id, CLIMATE_SUBJECT_ID, row.cluster_text)
                    for row in clusters.itertuples()
                ],
            )
            cur.execute(
                "DELETE FROM case_to_clusters WHERE case_id = ANY(%s)",
                (case_ids,),
            )
            cur.executemany(
                """
                INSERT INTO case_to_clusters (case_id, cluster_id)
                VALUES (%s, %s)
                """,
                [(row.case_id, row.cluster_id) for row in mappings.itertuples()],
            )
        conn.commit()

    print(
        f"  Written {len(clusters)} cluster(s) and {len(mappings)} case→cluster mapping(s) to DB."
    )


def load_from_db(
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    """Load data from the RRS database and return a DataFrame."""
    df = get_data_from_db(start_date=start_date, end_date=end_date)
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
        sentences: list[tuple[str, int]] = [
            (span.text.strip(), len(span))
            for span in doc.sents
            if len(span.text.strip()) >= 20
        ]
        if not sentences:
            continue

        i = 0
        while i < len(sentences):
            window = sentences[i : i + window_size]
            rows.append({"sentence": " ".join(t for t, _ in window), **record})

            if overlap_tokens == 0:
                i += window_size
            else:
                token_sum = 0
                suffix_len = 0
                for _, n_tok in reversed(window):
                    token_sum += n_tok
                    suffix_len += 1
                    if token_sum >= overlap_tokens:
                        break
                i += max(1, len(window) - suffix_len)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    get_data_from_db()
