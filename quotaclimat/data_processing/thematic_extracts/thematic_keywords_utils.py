import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date, timedelta
import pandas as pd
import numpy as np

# only local env
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)


def read_barometre_keywords(con: duckdb.DuckDBPyConnection,
                            keywords_to_look_for: list[str],
                            start_date: date, end_date: date,
                            country: str,
                            source_table: str, postgres_db: str) -> pd.DataFrame:
    """Reads barometre.keywords to retrieve (start, channel) of every thematic chunk to keep"""
    keywords_list = keywords_to_look_for
    kewords_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in keywords_list)
    try:
        out = con.sql(f"""
            WITH exploded AS (
                SELECT k.id, k.channel_name,
                    k.start as start,
                    k.plaintext,
                    unnest(from_json(k.keywords_with_timestamp::JSON,
                                        '[{{"keyword":"VARCHAR","theme":"VARCHAR"}}]')) AS kwt
                FROM {postgres_db}.{source_table} k
                WHERE k.start >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                    AND k.start <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                    AND k.country = '{country}'
            )
            SELECT e.id, e.channel_name, e.start, e.plaintext,
                string_agg(DISTINCT e.kwt.keyword, ',' ORDER BY e.kwt.keyword) AS keywords_found,
                string_agg(DISTINCT e.kwt.theme, ',' ORDER BY e.kwt.theme) AS themes
            FROM exploded e
            WHERE e.kwt.keyword IN ({kewords_sql})
            GROUP BY e.id, e.channel_name, e.start, e.plaintext
            ORDER BY e.start, e.channel_name
    """).df()
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
                                    postgres_db: str, schema: str) -> None:
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
                    plaintext VARCHAR NOT NULL,
                    source_keywords_id VARCHAR,
                    country VARCHAR NOT NULL,
                    period VARCHAR NOT NULL,
                    PRIMARY KEY (this_id)
                )
            """)
            logging.info(f"[{destination_table}] TABLE CREATED OR ALREADY EXISTED")

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
                                gap: pd.Timedelta, country: str, channel_titles: dict[str, str]) -> pd.DataFrame:
    """Keeps thematic chunks (from the keywords table) and their adjacents (within gap) fetched from s3"""
    try:
        gap_ns = pd.Timedelta(gap).value  # integer, nanoseconds
        keys = ["start", "channel_name"]
        #normalising in Paris time
        keywords_day_df = keywords_day_df.assign(start=pd.to_datetime(keywords_day_df["start"]).dt.tz_convert("Europe/Paris"))
        s3_day_df = s3_day_df.assign(start=pd.to_datetime(s3_day_df["start"]).dt.tz_convert("Europe/Paris"))

        # All chunks coming from the keywords table are thematic chunks
        kw = (
            keywords_day_df[keys + ["id", "plaintext", "keywords_found", "themes"]]
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

        # All thematic chunks must be in S3
        n_found = len(kw.merge(near[keys], on=keys))
        if n_found != len(kw):
            raise ValueError(f"{len(kw) - n_found} thematic chunks missing from S3")

        # For the same channel and start time, source plaintext must match S3 plaintext
        check = kw.merge(near, on=keys, suffixes=("_kw", "_s3"))
        mismatch = check[check["plaintext_kw"] != check["plaintext_s3"]]
        if not mismatch.empty:
            raise ValueError(
                f"{len(mismatch)} thematic chunks have mismatched plaintext between source table and S3 "
                f"at the same channel and start time: issue is likely caused by a timezone offset. "
                f"Example: {mismatch[['channel_name', 'start']].iloc[0].to_dict()}"
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
        out["channel_title"] = out["channel_name"].map(channel_titles[country])
        cols = ["this_id", "channel_name", "channel_title", "start",
                "thematic_chunk",
                "source_keywords_id", "keywords_found", "themes", "plaintext"]
        return out.sort_values(["channel_name", "start"])[cols].reset_index(drop=True)

    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ERROR EXTRACTING ADJACENT CHUNKS: {e}")
        raise e
    
    
### CHECKING RESULTS
def check_keywords(con: duckdb.DuckDBPyConnection, postgres_db: str, destination_table: str,
                   schema: str, gap: pd.Timedelta,
                   step: timedelta = timedelta(minutes=2)) -> pd.DataFrame:
    """Report on the whole destination table, per channel:
    - missing_prev / missing_next: thematic chunks with no chunk of the same channel within gap
      before / after (neighbour absent from S3, or lost)
    - n_expected_adj: distinct slots start +/- step of thematic chunks, not themselves thematic
      (a slot shared by two thematic chunks counts once)
    - n_missing_adj / pct_missing_adj: expected slots absent from the table. Upper bound of
      legitimate ones: ~105 program edges per day in France (program_metadata grid, July 2026:
      2 for news channels, ~6 for generalist ones, 34 for rfi), reached only if every edge had a
      thematic chunk. Beyond program edges, chunks can also be missing in S3.
    - orphans: adjacent chunks with no thematic chunk of the same channel within gap
    All thematic chunks are in the table (a day missing one in S3 is not ingested), so expected
    adjacents can be rebuilt from it. A missing slot can still be a slot with no broadcast."""
    gap_sql = f"INTERVAL '{int(pd.Timedelta(gap).total_seconds())} seconds'"
    step_sql = f"INTERVAL '{int(pd.Timedelta(step).total_seconds())} seconds'"
    try:
        report = con.sql(f"""
            WITH base AS MATERIALIZED (
                SELECT channel_name, start, thematic_chunk
                FROM {postgres_db}.{schema}.{destination_table}
            ),
            slots AS (
                SELECT DISTINCT channel_name, start - {step_sql} AS slot FROM base WHERE thematic_chunk
                UNION
                SELECT DISTINCT channel_name, start + {step_sql} AS slot FROM base WHERE thematic_chunk
            ),
            adj AS (
                SELECT s.channel_name,
                    count(*) AS n_expected_adj,
                    count(*) FILTER (WHERE b.start IS NULL) AS n_missing_adj
                FROM slots s
                LEFT JOIN base b ON b.channel_name = s.channel_name AND b.start = s.slot
                WHERE b.thematic_chunk IS DISTINCT FROM true
                GROUP BY s.channel_name
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
                    count(*) FILTER (WHERE orphan) AS orphans
                FROM flags
                GROUP BY channel_name
            )
            SELECT p.channel_name, p.n_chunks, p.n_thematic, p.n_adjacent,
                p.missing_prev, p.missing_next,
                coalesce(a.n_expected_adj, 0) AS n_expected_adj,
                coalesce(a.n_missing_adj, 0) AS n_missing_adj,
                round(100.0 * a.n_missing_adj / nullif(a.n_expected_adj, 0), 2) AS pct_missing_adj,
                p.orphans
            FROM per_channel p
            LEFT JOIN adj a USING (channel_name)
            ORDER BY n_missing_adj DESC, p.channel_name
        """).df()
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CHECKING TABLE: {e}")
        raise e

    total = report.drop(columns=["channel_name", "pct_missing_adj"]).sum()
    pct_total = (round(100 * total["n_missing_adj"] / total["n_expected_adj"], 2)
                 if total["n_expected_adj"] else None)
    logging.info(
        f"[CHECK] {destination_table}: {total['n_chunks']} chunks "
        f"({total['n_thematic']} thematic, {total['n_adjacent']} adjacent) on {len(report)} channels\n"
        f"thematic chunks without neighbour: {total['missing_prev']} before, {total['missing_next']} after\n"
        f"missing adjacents: {total['n_missing_adj']} / {total['n_expected_adj']} expected ({pct_total} %). Up to ~105 per day (France) can be program edges, "
        f"and chunks can be missing in S3.\n"
        f"orphan adjacents: {total['orphans']}"
    )
    with_missing = report[report["n_missing_adj"] > 0]
    if not with_missing.empty:
        logging.info(f"[CHECK] channels with missing adjacents:\n{with_missing.to_string(index=False)}")
    return report