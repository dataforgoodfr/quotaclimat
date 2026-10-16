
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

   
def configure_barometre_thematic_articles(con: duckdb.DuckDBPyConnection, 
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
                    source_code VARCHAR,
                    source_name VARCHAR,
                    publication_datetime DATE,
                    title VARCHAR,
                    snippet VARCHAR,
                    body VARCHAR,
                    word_count INT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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


def read_barometre_factiva_articles(con: duckdb.DuckDBPyConnection,
                                    keywords_to_look_for: list[str],
                                    start_date: date, end_date: date,
                                    source_table: str, postgres_db: str,) -> pd.DataFrame:
    """Reads factiva_articles and keeps articles whose snippet + title + body,
    normalized (lower, accents removed), match at least one expression.
    mots_trouves lists every match in normalized form, repeated if it occurs several times."""
    if not keywords_to_look_for:
        raise ValueError("keywords_to_look_for est vide")

    expressions = [re.compile(r"['’]").sub("", strip_accents_py(e)) for e in keywords_to_look_for]
    expressions = [re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", e) for e in expressions]
    # whole expressions only: word boundaries on both sides, tolerant to
    # multiple spaces / line breaks between the words of an expression
    pattern = r"(?i)\b(?:" + "|".join(expressions) + r")\b"
    pattern_sql = pattern.replace("'", "''")

    try:
        out = con.sql(f"""
            WITH txt AS (
                SELECT a."An" AS an,
                    a."Source Code" AS source_code,
                    a."Source Name" AS source_name,
                    CAST(a."Publication Datetime" AT TIME ZONE 'Europe/Paris' AS DATE) AS publication_datetime,
                    a."Title" AS title,
                    a."Snippet" AS snippet,
                    a."Body" AS body,
                    replace(replace(lower(strip_accents(concat_ws(' ', a."Snippet", a."Title", a."Body"))), '''', ''), '’', '') AS txt,
                    a."Word Count"  as word_count               
                FROM {postgres_db}.{source_table} a
                WHERE a."Publication Datetime" >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                    AND a."Publication Datetime" <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                    AND "Duplicate Status" <> 'DUP'
                    AND "Word Count" >= 100
            )
            SELECT an, source_code, source_name, publication_date, title, snippet, body
            FROM txt
            WHERE regexp_matches(txt, '{pattern_sql}')
        """).df()
        print(len(out))
        out.to_csv("csv.csv")
        return out
    except Exception as e:
        logging.error(f"[{source_table}] ERROR READING FACTIVA ARTICLES:")
        logging.error(e)
        raise e
    
    
### DESTINATION ### 
def get_max_start_date_from_thematic_articles(con: duckdb.DuckDBPyConnection, postgres_db: str,
                                        destination_table: str,
                                        destination_schema: str = "public") -> date:
    """Get the maximum publication date from the destination table, or None if empty."""
    try:
        result = con.sql(
            f"SELECT MAX(publication_date) FROM {postgres_db}.{destination_schema}.{destination_table}"
        ).fetchone()
        if result and result[0]:
            return result[0]
        return None
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR GETTING MAX PUBLICATION DATE: {e}")
        raise e
