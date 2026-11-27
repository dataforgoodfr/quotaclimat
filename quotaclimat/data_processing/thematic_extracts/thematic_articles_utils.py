
import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date
import pandas as pd

import quotaclimat.data_processing.thematic_extracts.thematic_utils as utils


# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)

   
def create_thematic_articles_table(con: duckdb.DuckDBPyConnection, 
                                    dry_run: bool,
                                    destination_table: str, 
                                    postgres_db: str, schema:str = "public",
                                    users_to_grant: list[str] | None = None) -> None:
    """The database is already attached by configure_barometre_source: we only prepare the table."""
    try:
        logging.info(f"[{destination_table}] CONFIGURATION: Using {postgres_db}")
        # <!> Asserting source_table won't be cleared
        if not dry_run:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {postgres_db}.{schema}.{destination_table} (
                    an VARCHAR NOT NULL,
                    source_code VARCHAR NOT NULL,
                    source_name VARCHAR NOT NULL,
                    publication_datetime TIMESTAMPTZ NOT NULL,
                    title_normalized VARCHAR,
                    snippet_normalized VARCHAR,
                    body_normalized VARCHAR,
                    word_count INT,
                    keywords_found VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    period VARCHAR NOT NULL,
                    PRIMARY KEY (an)
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


def read_factiva_articles(con: duckdb.DuckDBPyConnection,
                                    keywords_to_look_for: list[str],
                                    start_date: date, end_date: date,
                                    source_table: str, postgres_db: str,
                                    exclusions: list[str] | None = None) -> tuple[pd.DataFrame, int]:
    """Reads factiva_articles and keeps articles whose snippet + title + body,
    normalized (lower, accents removed, apostrophes and hyphens replaced by a space so that
    "l'incendie" matches "incendie" and "Kunming-Montréal" matches "kunming montreal"),
    match at least one expression.
    exclusions: expressions masked in that text before searching the keywords
    (e.g. "cessez le feu" for the keyword "feu"): an article whose keywords only appear
    inside exclusions is dropped, and keywords_found does not list them.
    keywords_found lists the distinct matches in normalized form, sorted and separated by ','.
    Only the normalized title, snippet and body are returned (join on an to get the originals).
    Returns the articles and the number of articles dropped by the exclusions."""
    if not keywords_to_look_for:
        raise ValueError("keywords_to_look_for est vide")

    pattern = utils.expressions_pattern(keywords_to_look_for)
    # copy of the text with the exclusions replaced by MASK, only used for step 2 (txt itself without exclusions)
    txt_searched = (utils.mask_exclusions_sql("txt", utils.expressions_pattern(exclusions))
                    if exclusions else "txt")

    try:
        out = con.sql(f"""
            WITH normalized AS (
                SELECT a."An" AS an,
                    a.source_code AS source_code,
                    a.source_name AS source_name,
                    a.publication_datetime AS publication_datetime,
                    {utils.normalize_sql("a.title")} AS title_normalized,
                    {utils.normalize_sql("a.snippet")} AS snippet_normalized,
                    {utils.normalize_sql("a.body")} AS body_normalized,
                    a.word_count as word_count
                FROM {postgres_db}.{source_table} a
                WHERE a.publication_datetime >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                    AND a.publication_datetime <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                    AND duplicate_status <> 'DUP'
            ),
            txt AS (
                -- searched text: normalizing then concatenating gives the same text as the other way round
                SELECT *, concat_ws(' ', snippet_normalized, title_normalized, body_normalized) AS txt
                FROM normalized
            ),
            searched AS (
                -- step 1: every article with a keyword, exclusions included (search on txt, not masked)
                -- txt_searched: masked copy, not a filter, not returned
                SELECT *, {txt_searched} AS txt_searched
                FROM txt
                WHERE regexp_matches(txt, '{pattern}')
            )
            SELECT an, source_code, source_name, publication_datetime,
                title_normalized, snippet_normalized, body_normalized, word_count,
                -- keywords found outside exclusions only
                array_to_string(list_sort(list_distinct(regexp_extract_all(txt_searched, '{pattern}'))), ',') AS keywords_found,
                -- step 2 (flag): no keyword left once the exclusions are masked
                NOT regexp_matches(txt_searched, '{pattern}') AS only_exclusions
            FROM searched
        """).df()
        # step 2: drop the flagged articles, then the flag column (the output keeps its columns)
        n_excluded = int(out["only_exclusions"].sum())
        if n_excluded:
            logging.info(f"[{source_table}] {start_date}: {n_excluded} articles dropped "
                         "(keywords only inside exclusions)")
        return out[~out["only_exclusions"]].drop(columns="only_exclusions").reset_index(drop=True), n_excluded
    except Exception as e:
        logging.error(f"[{source_table}] ERROR READING FACTIVA ARTICLES:")
        logging.error(e)
        raise e
    
    
### DESTINATION ### 
def get_max_start_from_thematic_articles(con: duckdb.DuckDBPyConnection, postgres_db: str,
                                        destination_table: str,
                                        destination_schema: str, period: str) -> date:
    """Get the maximum publication date of a period in the destination table, or None if it has no rows."""
    try:
        result = con.sql(
            f"SELECT CAST(MAX(publication_datetime) AT TIME ZONE 'Europe/Paris' AS DATE) "
            f"FROM {postgres_db}.{destination_schema}.{destination_table} WHERE period = '{period}'"
        ).fetchone()
        if result and result[0]:
            return result[0]
        return None
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR GETTING MAX PUBLICATION DATE: {e}")
        raise e


def check_articles(con: duckdb.DuckDBPyConnection, postgres_db: str, destination_table: str,
                   schema: str = "public", n_excluded: int = 0) -> pd.DataFrame:
    """Report on the whole destination table: number of articles per media (source_code, source_name).
    n_excluded: articles dropped by the exclusions during this run (a resumed run does not count
    the days ingested before)."""
    try:
        report = con.sql(f"""
            SELECT source_code, source_name, count(*) AS n_articles
            FROM {postgres_db}.{schema}.{destination_table}
            GROUP BY source_code, source_name
            ORDER BY n_articles DESC, source_code
        """).df()
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CHECKING TABLE: {e}")
        raise e

    logging.info(
        f"[CHECK] {destination_table}: {int(report['n_articles'].sum())} articles on {len(report)} media\n"
        f"articles dropped, keywords only inside exclusions (this run): {n_excluded}\n"
        f"{report.to_string(index=False)}"
    )
    return report
