import logging
import duckdb
import re
import yaml

POSTGRES_MAX_IDENTIFIER_BYTES = 63
MEDIA_TYPE_SHORT = {"audiovisuel": "keywords", "presse": "articles"}
# format produced by make_destination_table_name: only these tables can be cleared
DESTINATION_TABLE_PATTERN = (
    r"[a-z0-9_]+_(?:" + "|".join(MEDIA_TYPE_SHORT.values()) + r")_[a-z]+"
)


### TEXT NORMALIZATION ###
# lower, œ/æ -> oe/ae, French accents -> plain letter, apostrophes and hyphen -> space.
# normalize_text (python, for keywords) and normalize_sql (duckdb, for texts) must stay identical.
ACCENTS_FROM, ACCENTS_TO = "àâäéèêëîïôöùûüÿç", "aaaeeeeiioouuuyc"
SEPARATORS = "'’‘‛ʼʹ´`′＇-"

def normalize_text(s: str) -> str:
    """Normalizes a python string (keywords, keywords plaintext). None stays None, as in normalize_sql."""
    if s is None:
        return None
    s = s.lower().replace("œ", "oe").replace("æ", "ae").translate(str.maketrans(ACCENTS_FROM, ACCENTS_TO))
    return re.sub(f"[{SEPARATORS}]", " ", s)


def normalize_sql(col: str) -> str:
    """Returns the duckdb expression that normalizes a text column (NULL stays NULL)."""
    separators_sql = SEPARATORS.replace("'", "''")
    return f"""regexp_replace(
                        translate(
                            replace(replace(lower({col}), 'œ', 'oe'), 'æ', 'ae'),
                            '{ACCENTS_FROM}',
                            '{ACCENTS_TO}'
                        ),
                        '[{separators_sql}]', ' ', 'g'
                    )"""


### KEYWORDS ###
def get_keywords(yml_file: str, thematic: str, media_type: str, language: str) -> list[str]:
    """Returns the keywords of a thematic, as declared under
    [thematic]["keywords"][language][media_type] (media_type: "audiovisuel" or "presse")."""
    with open(yml_file, "r", encoding="utf-8") as f:
        kw = yaml.safe_load(f)
        
    if kw is None:
        raise ValueError(f"Empty file : {yml_file}")
    if thematic not in kw:
        raise KeyError(
            f"Missing theme '{thematic}' {yml_file}. "
            f"Available keys : {list(kw.keys())}"
        )
    keywords_by_language = kw[thematic].get("keywords") or {}
    if language not in keywords_by_language:
        raise KeyError(
            f"Missing language '{language}' in '{thematic}' / keywords {yml_file}. "
            f"Available keys : {list(keywords_by_language.keys())}"
        )
    if media_type not in keywords_by_language[language]:
        raise KeyError(
            f"Missing media type '{media_type}' in '{thematic}' / keywords / '{language}' {yml_file}. "
            f"Available keys : {list(keywords_by_language[language].keys())}"
        )
    return keywords_by_language[language][media_type]


### PERIODS ###
PERIOD_KEYS = {"id", "start", "end"}

def get_periods(yml_file: str, thematic: str, country: str) -> list[dict]:
    """Returns the periods of a thematic for a country, as declared under
    [thematic]["periodes"][country]: a list of {"id": str, "start": date, "end": date}.

    Raises if a period has missing or extra keys, an id outside [a-z0-9_], start >= end (end is excluded)
    or if two periods overlap (a chunk or an article can only belong to one period).
    """
    with open(yml_file, "r", encoding="utf-8") as f:
        kw = yaml.safe_load(f)

    if kw is None:
        raise ValueError(f"Empty file : {yml_file}")
    if thematic not in kw:
        raise KeyError(
            f"Missing theme '{thematic}' {yml_file}. "
            f"Available keys : {list(kw.keys())}"
        )
    periods_by_country = kw[thematic].get("periodes") or {}
    if country not in periods_by_country:
        raise KeyError(
            f"No periods for '{thematic}' / '{country}' in {yml_file}. "
            f"Available countries : {list(periods_by_country.keys())}"
        )
    periods = periods_by_country[country]
    if not periods:
        raise ValueError(f"Empty period list for '{thematic}' / '{country}' in {yml_file}")

    for p in periods:
        if not isinstance(p, dict) or set(p) != PERIOD_KEYS:
            raise ValueError(f"Period {p} of '{thematic}' / '{country}' must have exactly the keys {sorted(PERIOD_KEYS)}")
        if not re.fullmatch(r"[a-z0-9_]+", str(p["id"])):
            raise ValueError(f"Invalid period id '{p['id']}': only [a-z0-9_] allowed")
        if p["start"] >= p["end"]:
            raise ValueError(f"Period '{p['id']}': start {p['start']} is not before end {p['end']}")

    # end is excluded: a period may start on the end of the previous one
    by_start = sorted(periods, key=lambda p: p["start"])
    for prev, p in zip(by_start, by_start[1:]):
        if p["start"] < prev["end"]:
            raise ValueError(f"Periods '{prev['id']}' and '{p['id']}' of '{thematic}' / '{country}' overlap")
    return periods

    
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
    
    
def clear_destination_table(con, dry_run, destination_table, postgres_db, schema):
    """Empties the destination table (MODE=reset only). The table must already exist and its name
    must match the make_destination_table_name format, so that no other table can be cleared."""
    if not re.fullmatch(DESTINATION_TABLE_PATTERN, destination_table):
        raise ValueError(
            f"Refusing to clear '{destination_table}': name does not match {DESTINATION_TABLE_PATTERN}"
        )
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
    
    
def destination_table_exists(con: duckdb.DuckDBPyConnection,
                             destination_table: str,
                             postgres_db: str, schema: str) -> bool:
    """Returns True if the destination table exists in the attached database."""
    n = con.execute("""
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name = ?
    """, [postgres_db, schema, destination_table]).fetchone()[0]

    return n > 0


def make_destination_table_name(thematic: str, media_type: str,  country_short: str) -> str:
    """{thematic}_{media_type_code}_{country}, e.g. incendies_keywords_fr: one table gathers
    all the periods of the thematic (column period).
    Raises instead of letting Postgres silently truncate a name over 63 bytes."""

    if media_type not in MEDIA_TYPE_SHORT:
        raise ValueError(f"Media type not in {MEDIA_TYPE_SHORT.keys()}")

    table_name = f"{thematic}_{MEDIA_TYPE_SHORT[media_type]}_{country_short}"
    if not re.fullmatch(r"[a-z0-9_]+", table_name):
        raise ValueError(f"Invalid table name '{table_name}': only [a-z0-9_] allowed")
    n_bytes = len(table_name.encode("utf-8"))
    if n_bytes > POSTGRES_MAX_IDENTIFIER_BYTES:
        raise ValueError(
            f"Table name '{table_name}' is {n_bytes} bytes, Postgres max is {POSTGRES_MAX_IDENTIFIER_BYTES}"
        )
    return table_name