import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date
import pandas as pd
import numpy as np

import quotaclimat.data_processing.thematic_extracts.thematic_utils as utils

# only local env
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)


def read_barometre_keywords(con: duckdb.DuckDBPyConnection,
                            keywords_to_look_for: list[str],
                            start_date: date, end_date: date,
                            country: str, channels: list[str] | None,
                            source_table: str, postgres_db: str) -> pd.DataFrame:
    """Reads barometre.keywords to retrieve (start, channel) of every thematic chunk to keep,
    on every channel of the country, or only on channels when given"""
    keywords_list = keywords_to_look_for
    kewords_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in keywords_list)
    # no channels: IN (k.channel_name) keeps every channel (channel_name is NOT NULL)
    channels_sql = ("k.channel_name" if channels is None
                    else ", ".join("'" + c.replace("'", "''") + "'" for c in channels))
    try:
        out = con.sql(f"""
            WITH exploded AS (
                SELECT k.id, k.channel_name, k.channel_title,
                    k.start as start,
                    k.plaintext,
                    unnest(from_json(k.keywords_with_timestamp::JSON,
                                        '[{{"keyword":"VARCHAR","theme":"VARCHAR"}}]')) AS kwt
                FROM {postgres_db}.{source_table} k
                WHERE k.start >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                    AND k.start <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                    AND k.country = '{country}'
                    AND k.channel_name IN ({channels_sql})
            )
            SELECT e.id, e.channel_name, e.channel_title, e.start, e.plaintext,
                string_agg(DISTINCT e.kwt.keyword, ',' ORDER BY e.kwt.keyword) AS keywords_found,
                string_agg(DISTINCT e.kwt.theme, ',' ORDER BY e.kwt.theme) AS themes
            FROM exploded e
            WHERE e.kwt.keyword IN ({kewords_sql})
            GROUP BY e.id, e.channel_name, e.channel_title, e.start, e.plaintext
            ORDER BY e.start, e.channel_name
        """).df()
        
        ## Legacy name
        out["channel_title"] = out["channel_title"].replace("France Info", "France Info TV")
        return out
    
    except Exception as e:
        logging.error(f"[{source_table}] ERROR READING KEYWORDS TABLE:")
        logging.error(e)
        raise e
    
    
### DESTINATION ### 
def get_max_start_from_thematic_keywords(con: duckdb.DuckDBPyConnection, 
                                        postgres_db: str, 
                                        destination_table: str,
                                        schema: str, period: str) -> date:
    """Get the maximum start date of a period in the destination table, or None if it has no rows."""
    try:
        result = con.sql(
            f"SELECT CAST(MAX(start) AT TIME ZONE 'Europe/Paris' AS DATE) "
            f"FROM {postgres_db}.{schema}.{destination_table} WHERE period = '{period}'"
        ).fetchone()
        return result[0] if result and result[0] else None
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR GETTING MAX START DATE: {e}")
        raise e
    

def create_thematic_keywords_table(con: duckdb.DuckDBPyConnection, 
                                    dry_run: bool,
                                    destination_table: str, 
                                    postgres_db: str, schema: str,
                                    users_to_grant: list[str] | None = None) -> None:
    """The database is already attached by configure_barometre_source: we only prepare the table."""
    try:
        logging.info(f"[{destination_table}] CONFIGURATION: Using {postgres_db}")
        if not dry_run:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {postgres_db}.{schema}.{destination_table} (
                    this_id VARCHAR NOT NULL,
                    channel_name VARCHAR NOT NULL,
                    channel_title VARCHAR,
                    start TIMESTAMPTZ NOT NULL,
                    thematic_chunk BOOLEAN NOT NULL,
                    keywords_found VARCHAR,
                    themes VARCHAR,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    plaintext_normalized VARCHAR NOT NULL,
                    source_keywords_id VARCHAR,
                    country VARCHAR NOT NULL,
                    period VARCHAR NOT NULL,
                    PRIMARY KEY (this_id)
                )
            """)
            logging.info(f"[{destination_table}] TABLE CREATED OR ALREADY EXISTED")
            if users_to_grant:
                con.execute(f"CALL postgres_execute('{postgres_db}', 'GRANT SELECT ON {schema}.{destination_table} TO {', '.join(users_to_grant)}')")
                logging.info(f"[{destination_table}] SELECT GRANTED TO {', '.join(users_to_grant)}")

    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CONNECTING:")
        logging.error(e)
        raise e

### S3 ###
def _configure_s3(con: duckdb.DuckDBPyConnection, 
                  s3_endpoint_url: str, s3_region: str, 
                  s3_access_key: str, s3_secret_key: str) -> None:
    endpoint = s3_endpoint_url.removeprefix("https://").removeprefix("http://")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            SET s3_region='{s3_region}';
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{s3_access_key}';
            SET s3_secret_access_key='{s3_secret_key}';
            SET s3_url_style='path';
        """)
        logging.info("[S3] CONFIGURATION OK")

    except Exception as e:
        logging.error("[S3] CONFIGURATION ERROR:")
        logging.error(e)
        raise e


def read_day_from_s3(con: duckdb.DuckDBPyConnection, day: date, channels: list[str], country: str,
                     s3_bucket_name: str) -> pd.DataFrame:
    """Read parquet files for a specific day and channels: channel_name, start, plaintext."""
    if country != "france":
        globs = ", ".join(
            f"'s3://{s3_bucket_name}/country={country}/year={day.year}/month={day.month}/day={day.day}"
            f"/channel={ch}/*.parquet'"
            for ch in channels
        )
    else:
        globs = ", ".join(
            f"'s3://{s3_bucket_name}/year={day.year}/month={day.month}/day={day.day}"
            f"/channel={ch}/*.parquet'"
            for ch in channels
        )
    try:
        files = [r[0] for r in con.sql(f"SELECT file FROM glob([{globs}])").fetchall()]
        if not files:
            raise FileNotFoundError(f"No parquet found on S3 for {day}")
        # Channels without any files on that day
        missing = [ch for ch in channels if not any(f"/channel={ch}/" in f for f in files)]
        if missing:
            logging.warning(f"[S3] No S3 data for {day}: {', '.join(missing)}")

        file_list = ", ".join(f"'{f}'" for f in files)
        df = con.sql(
            f"SELECT channel_name, start, plaintext FROM read_parquet([{file_list}], union_by_name=true)"
        ).df()

        # Mediatree API uses sud-radio or sudradio: standardize before deduplicating
        df["channel_name"] = df["channel_name"].replace("sudradio", "sud-radio")
        
        if df.empty:
            logging.warning(f"[S3] No S3 chunks for {day}")
            raise FileNotFoundError(f"No S3 chunks for {day}")
        else:
            logging.info(f"[S3] {day}: {len(df)} chunks read from S3")

    except Exception as e:
        logging.error(f"[S3] ERROR READING S3 FOR {day}: {e}")
        raise e
    df = df.drop_duplicates(subset=["channel_name", "start"], keep="last").reset_index(drop=True)

    df["start"] = df["start"].dt.tz_convert("Europe/Paris")

    return df

### GETTING ADJACENT CHUNKS ###
def extract_adjacent_chunks_day(keywords_day_df: pd.DataFrame, s3_day_df: pd.DataFrame,
                                gap: pd.Timedelta) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Keeps thematic chunks (from the keywords table) and their adjacents (within gap) fetched from s3.
    Also returns the number of thematic chunks whose plaintext differs from S3,
    and the number of thematic chunks missing from S3 per channel."""
    try:
        gap_ns = pd.Timedelta(gap).value  # integer, nanoseconds
        keys = ["start", "channel_name"]
        #normalising in Paris time
        keywords_day_df = keywords_day_df.assign(start=pd.to_datetime(keywords_day_df["start"]).dt.tz_convert("Europe/Paris"))
        s3_day_df = s3_day_df.assign(start=pd.to_datetime(s3_day_df["start"]).dt.tz_convert("Europe/Paris"))

        # All chunks coming from the keywords table are thematic chunks
        kw = (
            keywords_day_df[keys + ["id", "channel_title", "plaintext", "keywords_found", "themes"]]
            .drop_duplicates(subset=keys)
            .rename(columns={"id": "source_keywords_id"})
            .assign(thematic_chunk=True)
        )

        # S3 chunks within gap of at least one thematic chunk (the chunk itself, 0 gap, is included)
        parts = []
        for channel, s3_chan in s3_day_df.groupby("channel_name"):
            kw_starts = (
                pd.to_datetime(kw.loc[kw["channel_name"] == channel, "start"])
                .dt.as_unit("ns").astype("int64").to_numpy()
            )
            if len(kw_starts) == 0:
                continue
            s3_starts = (
                pd.to_datetime(s3_chan["start"])
                .dt.as_unit("ns").astype("int64").to_numpy()
            )
            ecart = np.abs(s3_starts[:, None] - kw_starts[None, :])
            parts.append(s3_chan[(ecart <= gap_ns).any(axis=1)])

        if not parts:
            raise ValueError(
                f"No chunk in S3 found on {len(kw)} thematic chunks: "
                f"missing channels in S3 or timezone shifting?"
            )
        near = pd.concat(parts, ignore_index=True)

        # Thematic chunks should be in S3. On old days, the keywords table was filled before S3 was written,
        # with another program window (e.g. fr3-idf JT 12h on 2024-01-07): counted per channel and logged,
        # kept with the source table plaintext
        found = kw.merge(near[keys], on=keys, how="left", indicator=True)
        missing_s3 = found[found["_merge"] == "left_only"]
        n_missing_s3 = missing_s3["channel_name"].value_counts().to_dict()
        if missing_s3.empty:
            logging.info("[EXTRACT_ADJACENT_CHUNKS] 0 thematic chunk missing from S3")
        else:
            logging.warning(
                f"[EXTRACT_ADJACENT_CHUNKS] {len(missing_s3)} thematic chunks missing from S3, "
                f"source table plaintext kept. First 5:\n"
                + "\n".join(f"{row['channel_name']} {row['start']} (source id {row['source_keywords_id']})"
                            for _, row in missing_s3.head(5).iterrows())
            )

        # For the same channel and start time, source plaintext should match S3 plaintext.
        # Mediatree can deliver two versions of the same chunk (a different 30 s segment missing in each):
        # counted and logged, the source table plaintext is kept for thematic chunks
        check = kw.merge(near, on=keys, suffixes=("_kw", "_s3"))
        mismatch = check[check["plaintext_kw"] != check["plaintext_s3"]]
        if mismatch.empty:
            logging.info("[EXTRACT_ADJACENT_CHUNKS] 0 plaintext mismatch between source table and S3")
        else:
            examples = []
            for _, row in mismatch.head(5).iterrows():
                kw_text, s3_text = row["plaintext_kw"], row["plaintext_s3"]
                # first character where both plaintexts differ
                diverge = next((i for i, (a, b) in enumerate(zip(kw_text, s3_text)) if a != b),
                               min(len(kw_text), len(s3_text)))
                examples.append(
                    f"{row['channel_name']} {row['start']} (source id {row['source_keywords_id']}): "
                    f"diverge at char {diverge}\n"
                    f"  source table: {kw_text[max(0, diverge - 50):diverge + 100]!r}\n"
                    f"  S3:           {s3_text[max(0, diverge - 50):diverge + 100]!r}"
                )
            logging.warning(
                f"[EXTRACT_ADJACENT_CHUNKS] {len(mismatch)} plaintext mismatch between source table and S3 "
                f"at the same channel and start time, source table plaintext kept. First 5:\n"
                + "\n".join(examples)
            )

        # Pure adjacents: in S3 near a thematic chunk, absent from the keywords table
        pure = near.merge(kw[keys], on=keys, how="left", indicator=True)
        pure = pure[pure["_merge"] == "left_only"].drop(columns="_merge")
        pure = pure.assign(source_keywords_id=None, keywords_found=None, themes=None,
                           thematic_chunk=False)

        out = pd.concat([kw, pure], ignore_index=True)

        # id = primary key: channel + start UTC TZ so no doubles on winter/summer swithcing
        out["this_id"] = (out["channel_name"] + "_"
                  + out["start"].dt.tz_convert("UTC").dt.strftime("%Y%m%d%H%M%S").str.replace("+", "", regex=False))
        # adjacent chunks (from s3) take the title of their channel, known from its thematic chunks of the day
        titles = (kw.dropna(subset=["channel_title"]).drop_duplicates("channel_name")
                  .set_index("channel_name")["channel_title"])
        out["channel_title"] = out["channel_name"].map(titles)
        # plaintext was only needed to compare the source with S3: only the normalized form is ingested,
        # like title / snippet / body of the articles
        out["plaintext_normalized"] = out["plaintext"].map(utils.normalize_text)
        cols = ["this_id", "channel_name", "channel_title", "start",
                "thematic_chunk",
                "source_keywords_id", "keywords_found", "themes", "plaintext_normalized"]
        n_mismatch = mismatch["channel_name"].value_counts().to_dict()
        return out.sort_values(["channel_name", "start"])[cols].reset_index(drop=True), n_mismatch, n_missing_s3

    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ERROR EXTRACTING ADJACENT CHUNKS: {e}")
        raise e
    
    
### CHECKING RESULTS
def check_keywords(con: duckdb.DuckDBPyConnection, postgres_db: str, destination_table: str,
                   schema: str, gap: pd.Timedelta, dry_run: bool, source_table: str,
                   n_plaintext_mismatch: dict[str, int] | None = None,
                   n_missing_s3: dict[str, int] | None = None) -> pd.DataFrame | None:
    """Per channel report on the destination table, using gap as the extraction does (starts are not always on a 2 min grid).
    - n_expected_adj: sides (before / after) of thematic chunks with no other thematic chunk within gap
    - n_missing_adj (missing_prev + missing_next) / pct_missing_adj: expected sides with no chunk within gap.
      Program edges are legitimate misses (~105 per day in France, every news program edge for Belgian TV)
    - orphans: adjacent chunks with no thematic chunk of the same channel within gap
    - n_missing_s3: thematic chunks of this run missing from S3 (counted per channel by extract_adjacent_chunks_day),
      only this run: a resumed run does not count the days ingested before
    - n_plaintext_mismatch: thematic chunks of this run whose plaintext differs from S3 (counted per channel by
      extract_adjacent_chunks_day), only this run as well
    Both counts are logged even in dry run, with a per channel table. The table report needs the rows
    of this run in the table: skipped in dry run (returns None)"""
    n_plaintext_mismatch = n_plaintext_mismatch or {}
    if n_plaintext_mismatch:
        logging.warning(
            f"[CHECK] {sum(n_plaintext_mismatch.values())} plaintext mismatch between {source_table} and S3 during this run, "
            f"{source_table} plaintext kept: see the [EXTRACT_ADJACENT_CHUNKS] warnings of each day"
        )
    else:
        logging.info(f"[CHECK] 0 plaintext mismatch between {source_table} and S3 during this run")
    n_missing_s3 = n_missing_s3 or {}
    if n_missing_s3:
        logging.warning(
            f"[CHECK] {sum(n_missing_s3.values())} thematic chunks of {source_table} missing from S3 during this run, "
            f"{source_table} plaintext kept: see the [EXTRACT_ADJACENT_CHUNKS] warnings of each day"
        )
    else:
        logging.info(f"[CHECK] 0 thematic chunk of {source_table} missing from S3 during this run")
    if n_missing_s3 or n_plaintext_mismatch:
        # per channel table, also in dry run (the table report below needs ingested rows)
        s3_df = (pd.DataFrame({"n_missing_s3": pd.Series(n_missing_s3, dtype="int64"),
                               "n_plaintext_mismatch": pd.Series(n_plaintext_mismatch, dtype="int64")})
                 .fillna(0).astype(int).rename_axis("channel_name").reset_index()
                 .sort_values(["n_missing_s3", "n_plaintext_mismatch", "channel_name"],
                              ascending=[False, False, True]))
        logging.warning(f"[CHECK] S3 differences per channel during this run:\n{s3_df.to_string(index=False)}")
    if dry_run:
        logging.info(f"[CHECK] DRY_RUN: nothing ingested, no check on {destination_table}")
        return None

    gap_sql = f"INTERVAL '{int(pd.Timedelta(gap).total_seconds())} seconds'"
    try:
        report = con.sql(f"""
            WITH base AS MATERIALIZED (
                SELECT channel_name, start, thematic_chunk
                FROM {postgres_db}.{schema}.{destination_table}
            ),
            t AS (
                SELECT channel_name, start, thematic_chunk,
                    lag(start)  OVER (PARTITION BY channel_name ORDER BY start) AS prev_start,
                    lead(start) OVER (PARTITION BY channel_name ORDER BY start) AS next_start,
                    max(CASE WHEN thematic_chunk THEN start END) OVER (
                        PARTITION BY channel_name ORDER BY start
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_thematic,
                    min(CASE WHEN thematic_chunk THEN start END) OVER (
                        PARTITION BY channel_name ORDER BY start
                        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_thematic
                FROM base
            ),
            flags AS (
                SELECT *,
                    thematic_chunk AND coalesce(prev_start < start - {gap_sql}, true) AS missing_prev,
                    thematic_chunk AND coalesce(next_start > start + {gap_sql}, true) AS missing_next,
                    -- a side covered by another thematic chunk expects no adjacent
                    thematic_chunk AND coalesce(prev_thematic < start - {gap_sql}, true) AS expected_prev,
                    thematic_chunk AND coalesce(next_thematic > start + {gap_sql}, true) AS expected_next,
                    NOT thematic_chunk
                        AND coalesce(start - prev_thematic > {gap_sql}, true)
                        AND coalesce(next_thematic - start > {gap_sql}, true) AS orphan
                FROM t
            ),
            per_channel AS (
                SELECT channel_name,
                    count(*) AS n_chunks,
                    count(*) FILTER (WHERE thematic_chunk) AS n_thematic,
                    count(*) FILTER (WHERE NOT thematic_chunk) AS n_adjacent,
                    count(*) FILTER (WHERE missing_prev) AS missing_prev,
                    count(*) FILTER (WHERE missing_next) AS missing_next,
                    count(*) FILTER (WHERE expected_prev) + count(*) FILTER (WHERE expected_next) AS n_expected_adj,
                    count(*) FILTER (WHERE orphan) AS orphans
                FROM flags
                GROUP BY channel_name
            )
            SELECT channel_name, n_chunks, n_thematic, n_adjacent,
                missing_prev, missing_next,
                n_expected_adj,
                missing_prev + missing_next AS n_missing_adj,
                round(100.0 * (missing_prev + missing_next) / nullif(n_expected_adj, 0), 2) AS pct_missing_adj,
                orphans
            FROM per_channel
            ORDER BY n_missing_adj DESC, channel_name
        """).df()
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CHECKING TABLE: {e}")
        raise e

    report["n_missing_s3"] = report["channel_name"].map(n_missing_s3).fillna(0).astype(int)
    report["n_plaintext_mismatch"] = report["channel_name"].map(n_plaintext_mismatch).fillna(0).astype(int)
    total = report.drop(columns=["channel_name", "pct_missing_adj"]).sum()
    pct_total = (round(100 * total["n_missing_adj"] / total["n_expected_adj"], 2)
                 if total["n_expected_adj"] else None)
    logging.info(
        f"[CHECK] {destination_table}: {total['n_chunks']} chunks "
        f"({total['n_thematic']} thematic, {total['n_adjacent']} adjacent) on {len(report)} channels\n"
        f"thematic chunks without neighbour: {total['missing_prev']} before, {total['missing_next']} after\n"
        f"missing adjacents (within gap): {total['n_missing_adj']} / {total['n_expected_adj']} expected ({pct_total} %). Up to ~105 per day (France) can be program edges, "
        f"and chunks can be missing in S3.\n"
        f"orphan adjacents: {total['orphans']}\n"
        f"thematic chunks missing from S3 (this run): {total['n_missing_s3']}\n"
        f"plaintext mismatch with S3 (this run): {total['n_plaintext_mismatch']}"
    )
    with_missing = report[(report["n_missing_adj"] > 0) | (report["n_missing_s3"] > 0)
                          | (report["n_plaintext_mismatch"] > 0)]
    if not with_missing.empty:
        logging.info(f"[CHECK] channels with missing adjacents, thematic chunks missing from S3 or plaintext mismatch:\n"
                     f"{with_missing.to_string(index=False)}")
    return report