
import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date
import pandas as pd
import numpy as np
import sys
from urllib.parse import quote
import re
import unicodedata
import json
import hashlib


# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)

   
def create_thematic_articles_table(con: duckdb.DuckDBPyConnection, 
                                    dry_run: bool,
                                    destination_table: str, 
                                    postgres_db: str, schema:str = "public") -> None:
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
                    title VARCHAR,
                    snippet VARCHAR,
                    body VARCHAR,
                    word_count INT,
                    keywords_found VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    period VARCHAR NOT NULL,
                    PRIMARY KEY (an)
                )
            """)
            logging.info(f"[{destination_table}] TABLE CREATED OR ALREADY EXISTED")

    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CONNECTING:")
        logging.error(e)
        raise e


def strip_accents_py(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s)
                   if not unicodedata.combining(c))


def read_factiva_articles(con: duckdb.DuckDBPyConnection,
                                    keywords_to_look_for: list[str],
                                    start_date: date, end_date: date,
                                    source_table: str, postgres_db: str,) -> pd.DataFrame:
    """Reads factiva_articles and keeps articles whose snippet + title + body,
    normalized (lower, accents removed, apostrophes and hyphens replaced by a space so that
    "l'incendie" matches "incendie" and "Kunming-Montréal" matches "kunming montreal"),
    match at least one expression.
    keywords_found lists the distinct matches in normalized form, sorted and separated by ','."""
    if not keywords_to_look_for:
        raise ValueError("keywords_to_look_for est vide")

    # apostrophes and hyphen -> space, same class as in the SQL below
    expressions = [re.sub(r"['’‘‛ʼʹ´`′＇-]", " ", strip_accents_py(e)) for e in keywords_to_look_for]
    # escape regex special characters so that a keyword is searched literally ("c.o.p" must not match "cxoxp")
    expressions = [re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", e) for e in expressions]
    # longest first: with alternation, the regex keeps the first alternative that matches,
    # so "crue" listed before "crue centennale" would hide the longer expression in keywords_found
    expressions = sorted(expressions, key=len, reverse=True)
    # whole expressions only: word boundaries on both sides
    pattern = r"(?i)\b(?:" + "|".join(expressions) + r")\b"

    try:
        out = con.sql(f"""
            WITH txt AS (
                SELECT a."An" AS an,
                    a.source_code AS source_code,
                    a.source_name AS source_name,
                    a.publication_datetime AS publication_datetime,
                    a.title AS title,
                    a.snippet AS snippet,
                    a.body AS body,
                    regexp_replace(lower(strip_accents(concat_ws(' ', a.snippet, a.title, a.body))), '[''’‘‛ʼʹ´`′＇-]', ' ', 'g') AS txt,
                    a.word_count as word_count               
                FROM {postgres_db}.{source_table} a
                WHERE a.publication_datetime >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                    AND a.publication_datetime <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                    AND duplicate_status <> 'DUP'
            )
            SELECT an, source_code, source_name, publication_datetime, title, snippet, body, word_count,
                array_to_string(list_sort(list_distinct(regexp_extract_all(txt, '{pattern}'))), ',') AS keywords_found
            FROM txt
            WHERE regexp_matches(txt, '{pattern}')
        """).df()
        return out
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
                   schema: str = "public") -> pd.DataFrame:
    """Report on the whole destination table: number of articles per media (source_code, source_name)."""
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
        f"{report.to_string(index=False)}"
    )
    return report
