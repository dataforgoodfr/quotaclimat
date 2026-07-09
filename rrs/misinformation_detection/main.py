import argparse
import asyncio
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional
from urllib.parse import quote

import duckdb
import pandas as pd
import psycopg
from dotenv import load_dotenv

from mistralai.client import Mistral
from mistralai.client.types import BaseModel
from mistralai.extra.run.context import RunContext
from rrs.dictionary.upsert_subjects import subject_id as make_subject_id
from rrs.misinformation_detection.definitions import build_system_prompt
from rrs.utils.generate_id import get_consistent_hash

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

# ── S3 config ────────────────────────────────────────────────────────────────

REGION = "fr-par"


def _get_secret(name: str) -> str:
    value = os.environ.get(name, "")
    if value and os.path.exists(value):
        with open(value) as f:
            return f.read().strip()
    return value


# ── DB helpers ────────────────────────────────────────────────────────────────

def _rrs_conninfo() -> str:
    return (
        f"host={os.getenv('RRS_PG_HOST', 'localhost')} "
        f"port={os.getenv('RRS_PG_PORT', 5432)} "
        f"dbname={os.getenv('RRS_PG_DATABASE', 'rrs_db')} "
        f"user={os.getenv('RRS_PG_USER', 'user')} "
        f"password={os.getenv('RRS_PG_PASSWORD', 'password')}"
    )


def _rrs_dsn() -> str:
    user = quote(os.getenv("RRS_PG_USER", "user"), safe="")
    password = quote(os.getenv("RRS_PG_PASSWORD", "password"), safe="")
    host = os.getenv("RRS_PG_HOST", "localhost")
    port = os.getenv("RRS_PG_PORT", "5432")
    database = os.getenv("RRS_PG_DATABASE", "rrs_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


# ── Segment loading ───────────────────────────────────────────────────────────

def load_segments(
    subject: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load segments from the RRS DB and enrich them with plaintext from S3."""
    sid = make_subject_id(subject)

    with psycopg.connect(_rrs_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT segment_id, channel_name, channel_title, channel_program,
                       start, s3_uri, n_keywords, keywords, url_mediatree
                FROM segments
                WHERE subject_id = %s
                  AND start >= %s
                  AND start < %s
                ORDER BY start
                """,
                (sid, start_date, end_date + timedelta(days=1)),
            )
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

    if not rows:
        logging.warning(f"No segments found for subject '{subject}' in {start_date} – {end_date}.")
        return pd.DataFrame()

    segments = pd.DataFrame(rows, columns=cols)
    logging.info(f"Loaded {len(segments)} segment(s) from DB.")

    segments = _enrich_with_plaintext(segments)
    return segments


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    access_key = _get_secret("BUCKET")
    secret_key = _get_secret("BUCKET_SECRET")
    bucket_name = os.environ.get("BUCKET_NAME")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='{REGION}';
        SET s3_endpoint='s3.{REGION}.scw.cloud';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_url_style='path';
    """)


def _enrich_with_plaintext(segments: pd.DataFrame) -> pd.DataFrame:
    """Join segments with S3 parquet files to retrieve plaintext for each segment."""
    # Collect all unique S3 directory URIs (each is a day/channel folder)
    uris = segments["s3_uri"].dropna().unique().tolist()
    if not uris:
        segments["plaintext"] = None
        return segments

    con = duckdb.connect()
    _configure_s3(con)

    globs = [uri.rstrip("/") + "/*.parquet" for uri in uris]
    existing = con.sql(
        f"SELECT file FROM glob([{', '.join(repr(g) for g in globs)}])"
    ).fetchall()
    existing_files = [row[0] for row in existing]

    if not existing_files:
        logging.warning("No parquet files found on S3 for the loaded segments.")
        segments["plaintext"] = None
        return segments

    file_list = ", ".join(repr(f) for f in existing_files)
    con.sql(
        f"CREATE OR REPLACE VIEW parquet_source AS "
        f"SELECT * FROM read_parquet([{file_list}], union_by_name=true)"
    )

    con.register("segments_lookup", segments[["segment_id", "channel_name", "start"]])
    texts = con.sql("""
        SELECT s.segment_id, p.plaintext
        FROM segments_lookup s
        JOIN parquet_source p
          ON p.channel_name = s.channel_name
         AND p.start = s.start
    """).df()
    con.close()

    enriched = segments.merge(texts, on="segment_id", how="left")
    missing = enriched["plaintext"].isna().sum()
    if missing:
        logging.warning(f"{missing} segment(s) had no matching plaintext in S3.")
    return enriched


# ── LLM classification ────────────────────────────────────────────────────────

PRICING = {
    "mistral-small-2603": (0.15, 0.60),
    "mistral-small-3.2": (0.10, 0.30),
    "mistral-medium-2505": (0.40, 2.00),
    "mistral-large-2411": (2.00, 6.00),
}


class MisinfoResult(BaseModel):
    label: Literal["oui", "non", "incertain"]
    score: float
    justification: str


async def _classify_one(
    client: Mistral,
    semaphore: asyncio.Semaphore,
    system_prompt: str,
    index: int,
    total: int,
    text: str,
    model: str,
) -> tuple[MisinfoResult, int, int]:
    async with semaphore:
        logging.info(f"[{index + 1}/{total}] Classifying...")
        async with RunContext(model=model, output_format=MisinfoResult) as run_ctx:
            run_result = await client.beta.conversations.run_async(
                run_ctx=run_ctx,
                instructions=system_prompt,
                inputs=[
                    {
                        "role": "user",
                        "content": f"Analyse cet extrait et détecte une éventuelle désinformation :\n\n{text}",
                    }
                ],
            )
        output_text = run_result.output_entries[0].content if run_result.output_entries else ""
        input_tokens = len(system_prompt + text) // 4
        output_tokens = len(output_text) // 4
        return run_result.output_as_model, input_tokens, output_tokens


async def classify_segments(
    df: pd.DataFrame,
    subject: str,
    model: str,
    concurrency: int,
) -> pd.DataFrame:
    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        raise EnvironmentError("MISTRAL_API_KEY environment variable is not set")

    system_prompt = build_system_prompt(subject)
    client = Mistral(api_key=api_key)
    semaphore = asyncio.Semaphore(concurrency)

    async def _skip():
        return None

    texts = df["plaintext"].tolist()
    tasks = [
        _classify_one(client, semaphore, system_prompt, i, len(df), str(t), model)
        if not (pd.isna(t) or str(t).strip() == "")
        else _skip()
        for i, t in enumerate(texts)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    labels, scores, justifications = [], [], []
    total_input = total_output = 0
    for i, result in enumerate(results):
        if result is None:
            labels.append(None)
            scores.append(None)
            justifications.append(None)
        elif isinstance(result, Exception):
            logging.error(f"  Error on row {i}: {result}")
            labels.append("error")
            scores.append(None)
            justifications.append(str(result))
        else:
            misinfo, in_tok, out_tok = result
            labels.append(misinfo.label)
            scores.append(misinfo.score)
            justifications.append(misinfo.justification)
            total_input += in_tok
            total_output += out_tok

    price_in, price_out = PRICING.get(model, (0.0, 0.0))
    cost = (total_input * price_in + total_output * price_out) / 1_000_000
    if price_in:
        logging.info(
            f"Tokens (estimated) — input: {total_input}, output: {total_output} | "
            f"Estimated cost: ${cost:.4f} USD"
        )
    else:
        logging.info(
            f"Tokens (estimated) — input: {total_input}, output: {total_output} "
            f"(no pricing data for '{model}')"
        )

    out = df[["segment_id", "plaintext", "channel_name", "channel_title", "start"]].copy()
    out["subject_id"] = get_consistent_hash(subject)
    out["label"] = labels
    out["score"] = scores
    out["justification"] = justifications
    return out


# ── DB persistence ───────────────────────────────────────────────────────────

def save_cases_to_db(result: pd.DataFrame) -> None:
    """Upsert misinformation classification results into the RRS cases table."""
    if result.empty:
        return

    cases = result.copy()
    cases["case_id"] = cases.apply(
        lambda r: get_consistent_hash(f"{r['segment_id']}_{r['subject_id']}"), axis=1
    )
    cases["model_score"] = cases["label"]
    cases["model_reason"] = cases.apply(
        lambda r: f"[confidence={r['score']:.2f}] {r['justification']}"
        if r["score"] is not None
        else r["justification"],
        axis=1,
    )
    cases["text"] = cases["plaintext"]
    cases["url_labelstudio"] = None
    cases["is_labeled"] = False
    cases["mesinfo_choice"] = None

    batch = cases[
        [
            "case_id",
            "segment_id",
            "subject_id",
            "start",
            "model_score",
            "model_reason",
            "text",
            "url_labelstudio",
            "is_labeled",
            "mesinfo_choice",
        ]
    ]

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{_rrs_dsn()}' AS rrs (TYPE POSTGRES);")
    con.register("cases_batch", batch)
    con.execute("""
        INSERT INTO rrs.cases (
            case_id, segment_id, subject_id, start,
            model_score, model_reason, text,
            url_labelstudio, is_labeled, mesinfo_choice,
            created_at, updated_at
        )
        SELECT
            case_id, segment_id, subject_id, start,
            model_score, model_reason, text,
            url_labelstudio, is_labeled, mesinfo_choice,
            now() AT TIME ZONE 'utc',
            now() AT TIME ZONE 'utc'
        FROM cases_batch
        ON CONFLICT (case_id, segment_id, subject_id) DO UPDATE SET
            model_score     = EXCLUDED.model_score,
            model_reason    = EXCLUDED.model_reason,
            text            = EXCLUDED.text,
            is_labeled      = EXCLUDED.is_labeled,
            mesinfo_choice  = EXCLUDED.mesinfo_choice,
            updated_at      = now() AT TIME ZONE 'utc'
    """)
    con.close()
    logging.info(f"  {len(batch)} case(s) upserted into DB.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run misinformation detection on segments loaded from the RRS DB."
    )
    parser.add_argument(
        "--subject",
        default=os.getenv("SUBJECT", "insecurity"),
        help="Subject name to analyse (default: insecurity, env: SUBJECT)",
    )
    parser.add_argument(
        "--days-prior",
        type=int,
        default=int(os.getenv("DAYS_PRIOR", "1")),
        help="Number of days before end-date to use as start-date (default: 1, env: DAYS_PRIOR)",
    )
    parser.add_argument(
        "--start-date",
        default=os.getenv("START_DATE"),
        metavar="YYYY-MM-DD",
        help="Start date inclusive — overrides --days-prior if set (env: START_DATE)",
    )
    parser.add_argument(
        "--end-date",
        default=os.getenv("END_DATE") or None,
        metavar="YYYY-MM-DD",
        help="End date inclusive (default: today, env: END_DATE)",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_PATH"),
        help="Optional path to also save results as CSV (env: OUTPUT_PATH)",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("MISTRAL_MODEL", "mistral-small-2603"),
        help="Mistral model ID (default: mistral-small-2603, env: MISTRAL_MODEL)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("CONCURRENCY", "5")),
        help="Max concurrent API calls (default: 5, env: CONCURRENCY)",
    )
    args = parser.parse_args()

    end = date.fromisoformat(args.end_date) if args.end_date else date.today()
    start = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else end - timedelta(days=args.days_prior)
    )

    segments = load_segments(subject=args.subject, start_date=start, end_date=end)
    if segments.empty:
        logging.warning("Nothing to process.")
        return

    result = asyncio.run(
        classify_segments(
            df=segments,
            subject=args.subject,
            model=args.model,
            concurrency=args.concurrency,
        )
    )

    misinfo = result[result["label"] == "oui"].copy()
    logging.info(f"{len(misinfo)}/{len(result)} segment(s) classified as misinformation — saving to DB.")
    save_cases_to_db(misinfo)

    if args.output:
        result.to_csv(args.output, index=False)
        logging.info(f"Results also saved to {args.output}")


if __name__ == "__main__":
    main()
