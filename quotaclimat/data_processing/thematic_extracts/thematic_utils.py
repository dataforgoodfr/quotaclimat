import logging
import duckdb
import re
import yaml

from quotaclimat.data_processing.mediatree.i8n.country import BELGIUM, BELGIUM_FLANDERS, FRANCE

# key: COUNTRY env variable of the jobs and periodes key in thematic_jobs_config.yml
# - country: value of the country column in public.keywords and of the country partition in s3
# - country_short: suffix of the destination table name
# - language: keywords key in thematic_jobs_config.yml
# - channels: channels to scan in public.keywords
COUNTRIES = {
    "france": {"country": "france", "country_short": "fr", "language": "french", 
                "channels": FRANCE.channels},  # d8 (C8) excluded
    # LN24, LATROIS and CANALZ are legacy channels
    "belgiumfr": {"country": "belgium", "country_short": "befr", "language": "french",
                  "channels": [c for c in BELGIUM.channels if c not in ("LN24", "LATROIS", "CANALZ")]},
    "belgiumnl": {"country": "belgium", "country_short": "benl", "language": "dutch",
                  "channels": BELGIUM_FLANDERS.channels + ["atv", "tvl"]},
}

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


def expressions_pattern(expressions: list[str]) -> str:
    """Regex (python and duckdb) matching any of the expressions, normalized, as whole expressions.
    Special characters are escaped ("c.o.p" must not match "cxoxp") and the longest expressions
    come first: with alternation, the regex keeps the first alternative that matches, so "crue"
    listed before "crue centennale" would hide the longer expression."""
    normalized = {normalize_text(e) for e in expressions}
    escaped = [re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", e) for e in normalized]
    escaped = sorted(escaped, key=lambda e: (-len(e), e))
    return r"(?i)\b(?:" + "|".join(escaped) + r")\b"


### EXCLUSIONS ###
# An exclusion expression (e.g. "cessez le feu") is replaced by MASK in the normalized text before
# searching the keywords again: a chunk or an article whose keywords only appear inside exclusions is dropped.
# MASK is not a space: no keyword can be formed by the words around the removed expression.
MASK = " # "


def mask_exclusions(text_normalized: str | None, exclusions_regex: str) -> str | None:
    """Replaces every exclusion of a normalized text by MASK (None stays None)."""
    if text_normalized is None:
        return None
    return re.sub(exclusions_regex, MASK, text_normalized)


def mask_exclusions_sql(col: str, exclusions_regex: str) -> str:
    """Duckdb expression replacing every exclusion of a normalized text column by MASK."""
    return f"regexp_replace({col}, '{exclusions_regex}', '{MASK}', 'g')"


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


def get_exclusions(yml_file: str, thematic: str, language: str) -> list[str]:
    """Returns the exclusion expressions of a thematic, as declared under
    [thematic]["keywords"][language]["exclusions"] (shared by audiovisuel and presse).
    The key is optional: no key or an empty list gives [] (nothing excluded).

    Raises if an exclusion is not a non-empty string, or if it is equal to a keyword of the
    language after normalization (every occurrence of that keyword would be masked)."""
    with open(yml_file, "r", encoding="utf-8") as f:
        kw = yaml.safe_load(f)

    if kw is None:
        raise ValueError(f"Empty file : {yml_file}")
    if thematic not in kw:
        raise KeyError(
            f"Missing theme '{thematic}' {yml_file}. "
            f"Available keys : {list(kw.keys())}"
        )
    by_media = (kw[thematic].get("keywords") or {}).get(language) or {}
    exclusions = by_media.get("exclusions") or []
    if not isinstance(exclusions, list):
        raise ValueError(f"Exclusions of '{thematic}' / '{language}' must be a list")
    for e in exclusions:
        if not isinstance(e, str) or not e.strip():
            raise ValueError(f"Invalid exclusion {e!r} in '{thematic}' / '{language}': non-empty text expected")

    keywords = {normalize_text(k) for media, kws in by_media.items() if media != "exclusions"
                for k in (kws or [])}
    equal_to_keyword = sorted(e for e in exclusions if normalize_text(e) in keywords)
    if equal_to_keyword:
        raise ValueError(
            f"Exclusions of '{thematic}' / '{language}' equal to a keyword: {equal_to_keyword}"
        )
    return exclusions


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
                               postgres_db: str, use_ctid_scan: bool = True) -> None:
    """Attach the Barometre database (once: serves both as source and destination).

    use_ctid_scan=False: by default duckdb splits a Postgres scan into ctid ranges of
    pg_pages_per_task pages, so a filtered read goes through every page of the table and
    Postgres cannot use an index on the filtered column. Without it, duckdb sends one query
    with the filters, which uses the index (scan on one connection only)."""
    barometre_dsn=f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
    try:
        # Install and load the Postgres extension for DuckDB
        con.execute("INSTALL postgres; LOAD postgres;")
        if not use_ctid_scan:
            con.execute("SET pg_use_ctid_scan = false")
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