import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date
import pandas as pd
import numpy as np
import sys
import hashlib
import json
import re
import unicodedata
from datetime import date
import yaml


### KEYWORDS ###
def get_keywords(yml_file: str, thematic: str) -> list[str]:
    with open(yml_file, "r", encoding="utf-8") as f:
        kw = yaml.safe_load(f)

    if kw is None:
        raise ValueError(f"Empty file : {yml_file}")
    if thematic not in kw:
        raise KeyError(
            f"Missing theme '{thematic}' {yml_file}. "
            f"Available keys : {list(kw.keys())}"
        )
        
    value = kw[thematic]
    if isinstance(value, dict):  # catégories → liste plate
        value = [m for mots in value.values() for m in mots]
    return value
   
    
### SOURCE ###
def configure_barometre_source(con: duckdb.DuckDBPyConnection, source_table: str,
                               postgres_user: str, postgres_password: str, 
                               postgres_host: str, postgres_port: str, 
                               postgres_db: str) -> None:
    """Attach the Barometre database (once: serves both as source and destination)."""
    barometre_dsn=f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
    try:
        # Install and load the Postgres extension for DuckDB
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{barometre_dsn}' AS {postgres_db} (TYPE POSTGRES);")
        logging.info(f"[{source_table}] CONNECTION OK")

    except Exception as e:
        logging.error(f"[{source_table}] CONNECTION ERROR:")
        logging.error(e)
        raise e
    
    
def clear_destination_table(con: duckdb.DuckDBPyConnection,
                            dry_run: bool,
                            destination_table: str,
                            postgres_db: str, schema: str) -> None:
    """Empties the destination table (new run only). The table must already exist."""
    try:
        if dry_run:
            logging.warning(f"[{destination_table}] DRY_RUN: table would be cleared")
            return
        n_before = con.execute(
            f"SELECT count(*) FROM {postgres_db}.{schema}.{destination_table}"
        ).fetchone()[0]

        con.execute(f"DELETE FROM {postgres_db}.{schema}.{destination_table}")

        logging.warning(f"[{destination_table}] CLEARED: {n_before} rows deleted")

    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CLEARING:")
        logging.error(e)
        raise