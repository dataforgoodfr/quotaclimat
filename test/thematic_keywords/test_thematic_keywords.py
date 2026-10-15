import json
from datetime import date
from unittest.mock import MagicMock, patch
import duckdb
import pandas as pd
import pytest
import logging
from types import SimpleNamespace

from quotaclimat.data_processing.mediatree.thematic_keywords import thematic_keyword as tk
from quotaclimat.data_processing.mediatree.thematic_keywords import thematic_keywords_utils as utils

UTILS_MODULE = "quotaclimat.data_processing.mediatree.thematic_keywords.thematic_keywords_utils"
SOURCE_ALIAS = "barometre_test"
SOURCE_TABLE = "public.keywords"
GAP = "2 minutes 30 seconds"

FAKE_THEME_KEYWORDS = {
    "adaptation_climatique_solution": [
        {"keyword": "bassin de récupération", "language": "french", "high_risk_of_false_positive": False},
    ],
    "ressources_solutions": [
        {"keyword": "bassin de récupération", "language": "french", "high_risk_of_false_positive": False},
    ],
}
FAKE_TITLES = {"tf1": "TF1", "france2": "France 2"}
FAKE_CHANNEL_TITLES = {"france": FAKE_TITLES}   # same shape as CHANNEL_TITLES: {country: {channel: title}}

# Arguments of read_barometre_keywords that never change in these tests
READ_ARGS = dict(country="france", source_table=SOURCE_TABLE, postgres_db=SOURCE_ALIAS)


def make_cfg(**overrides) -> dict:
    """Builds the values normally set as module globals by load_env() (which is not called in tests).
    Every global read by run() is defined; tests override only what they need."""
    cfg = {
        "START_DATE": date(2026, 7, 1),
        "END_DATE": date(2026, 7, 4),
        "KEYWORDS": ["incendie"],
        "COUNTRY": "france",
        "LANGUAGE": "french",
        "DRY_RUN": False,
        "SOURCE_TABLE": SOURCE_TABLE,
        "DESTINATION_TABLE": None,
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "pw",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": SOURCE_ALIAS,
        "S3_ACCESS_KEY": "k",
        "S3_SECRET_KEY": "s",
        "S3_ENDPOINT_URL": "http://s3",
        "S3_BUCKET_NAME": "b",
        "S3_REGION": "r",
    }
    cfg.update(overrides)
    return cfg


# ---------- get_keywords_in_ome ----------

@patch(f"{UTILS_MODULE}.THEME_KEYWORDS", FAKE_THEME_KEYWORDS)
def test_get_keywords_in_ome_filters_absent_words_and_language():
    """Verify that a valid matching word is returned, that an absent word makes the function raise
    (instead of being silently discarded), and that a word in another language than requested raises too."""
    result = utils.get_keywords_in_ome(["bassin de récupération"], language="french")
    assert result == ["bassin de récupération"]

    with pytest.raises(ValueError):
        utils.get_keywords_in_ome(["bassin de récupération", "blabla"], language="french")

    with pytest.raises(ValueError):
        utils.get_keywords_in_ome(["bassin de récupération"], language="english")


# ---------- read_barometre_keywords ----------

@pytest.fixture
def con():
    _con = duckdb.connect()
    _con.execute("SET TimeZone='UTC'")   # session != Paris: detects any regression to session timezone bug
    _con.execute(f"ATTACH ':memory:' AS {SOURCE_ALIAS}")
    _con.execute(f"CREATE SCHEMA {SOURCE_ALIAS}.public")
    _con.execute(f"""
        CREATE TABLE {SOURCE_ALIAS}.public.keywords (
            id VARCHAR,
            channel_name VARCHAR,
            start TIMESTAMPTZ,          -- was TIMESTAMP
            country VARCHAR,
            keywords_with_timestamp VARCHAR,
            plaintext VARCHAR
        )
    """)
    yield _con
    _con.close()


def _insert(con, id_, kwts, start="2026-03-30 12:00:00+02:00", country="france", plaintext="bonjour"):
    """Inserts a test chunk (channel tf1) into the fake keywords table, with the list
    of keywords and themes `kwts` JSON-serialized, at the given date, country, and plaintext."""
    con.execute(
        f"INSERT INTO {SOURCE_ALIAS}.public.keywords VALUES (?, ?, ?, ?, ?, ?)",
        [id_, "tf1", start, country, json.dumps(kwts, ensure_ascii=False), plaintext],
    )


def test_read_barometre_keywords_keeps_only_chunks_with_searched_keywords(con):
    """Verify that every chunk containing a searched keyword is returned (whatever its theme),
    and that chunks without any searched keyword are excluded."""
    searched = ["bassin de récupération"]

    # kept: searched keyword
    _insert(con, "1", [{"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"}])
    # excluded: non-searched keyword only
    _insert(con, "2", [{"keyword": "blabla", "theme": "adaptation_climatique_solution"}])
    # kept: searched keyword with several themes
    _insert(con, "3", [
        {"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"},
        {"keyword": "bassin de récupération", "theme": "ressources_solutions"},
    ])
    # kept: searched keyword next to a non-searched one
    _insert(con, "4", [
        {"keyword": "blabla", "theme": "ressources_solutions"},
        {"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"},
    ])

    result = utils.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), **READ_ARGS
    )

    assert set(result["id"]) == {"1", "3", "4"}
    assert result["id"].is_unique
    assert {"plaintext", "keywords", "themes"} <= set(result.columns)


def test_read_barometre_keywords_returns_plaintext_keywords_and_themes(con):
    """Verify that reading returns the chunk's plaintext, searched keywords, and
    themes (direct and _indirectes) concatenated by a comma without spaces, sorted,
    and that non-searched keywords and their themes are left out."""
    searched = ["incendie", "canadair"]
    _insert(con, "1", [
        {"keyword": "incendie", "theme": "climat_consequences"},
        {"keyword": "canadair", "theme": "climat_consequences"},
        {"keyword": "incendie", "theme": "ressources_indirectes"},
        {"keyword": "blabla", "theme": "autre"},
    ], plaintext="un incendie, un canadair")

    result = utils.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), **READ_ARGS
    )

    row = result.iloc[0]
    assert row["plaintext"] == "un incendie, un canadair"
    assert row["keywords"] == "canadair,incendie"
    assert row["themes"] == "climat_consequences,ressources_indirectes"


def test_read_barometre_keywords_filters_on_date_window_and_country(con):
    """Verify that reading respects the [start_date, end_date) window and country:
    a chunk the day before, at end date (excluded), or in another country is discarded."""
    searched = ["bassin de récupération"]
    kw = [{"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"}]

    _insert(con, "in", kw, start="2026-03-30 23:59:00+02:00")
    _insert(con, "before", kw, start="2026-03-29 23:59:00+02:00")
    _insert(con, "end_excluded", kw, start="2026-03-31 00:00:00+02:00")   # end_date excluded
    _insert(con, "other_country", kw, country="belgium")
    # 00:30 Paris on 30th = 22:30 UTC on 29th: inside in Paris, outside in UTC
    _insert(con, "paris_morning", kw, start="2026-03-30 00:30:00+02:00")
    # 00:30 Paris on 31st = 22:30 UTC on 30th: outside in Paris, inside in UTC
    _insert(con, "paris_next_day", kw, start="2026-03-31 00:30:00+02:00")

    result = utils.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), **READ_ARGS
    )

    assert set(result["id"]) == {"in", "paris_morning"}


def test_read_barometre_keywords_dedups_keywords_and_themes(con):
    """Verify that a word present with several themes appears only once in keywords,
    that themes lists the distinct themes sorted (direct and _indirectes), and that a searched
    word with only _indirectes themes still appears in keywords."""
    searched = ["incendie", "inondation"]
    _insert(con, "1", [
        {"keyword": "incendie", "theme": "climat_consequences"},
        {"keyword": "incendie", "theme": "autre_theme"},
        {"keyword": "incendie", "theme": "ressources_indirectes"},
        {"keyword": "inondation", "theme": "ressources_indirectes"},
    ])

    result = utils.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), **READ_ARGS
    )

    assert len(result) == 1
    row = result.iloc[0]
    assert row["keywords"] == "incendie,inondation"
    assert row["themes"] == "autre_theme,climat_consequences,ressources_indirectes"


# ---------- read_day_from_s3: test 14 ----------

class _FakeS3Con:
    """Fake DuckDB connection: glob returns fixed files, read_parquet returns a fixed DataFrame."""

    def __init__(self, files, df):
        """Stores the files and DataFrame to return."""
        self.files, self.df = files, df

    def sql(self, query):
        """Responds according to whether the query is a glob or a parquet read."""
        if "glob(" in query:
            return SimpleNamespace(fetchall=lambda: [(f,) for f in self.files])
        return SimpleNamespace(df=lambda: self.df.copy())


def test_read_day_from_s3_keeps_last_row_for_duplicate_channel_and_start():
    """Verify that two S3 rows with the same (channel_name, start) are deduplicated
    by keeping the last one."""
    df = pd.DataFrame({
        "channel_name": ["tf1", "tf1", "tf1"],
        "start": pd.to_datetime(["2026-03-30 12:00:00"] * 2 + ["2026-03-30 12:01:00"])
                   .tz_localize("Europe/Paris"),
        "plaintext": ["ancien", "recent", "autre"],
    })
    con = _FakeS3Con(["s3://b/year=2026/month=3/day=30/channel=tf1/a.parquet"], df)

    result = utils.read_day_from_s3(con, date(2026, 3, 30), ["tf1"], "france", "b")

    assert len(result) == 2
    assert set(result["plaintext"]) == {"recent", "autre"}
    assert result["start"].dt.tz is None
    assert list(result["start"]) == [pd.Timestamp("2026-03-30 12:00:00"), pd.Timestamp("2026-03-30 12:01:00")]


# ---------- extract_adjacent_chunks_day ----------

def _core(*starts, channel="tf1", plaintexts=None, keywords="incendie", themes="climat_consequences"):
    """Builds a thematic chunks DataFrame (id, channel_name, start, plaintext, keywords, themes)
    for a single channel. Default plaintext is 'core', IDs are id_0, id_1..."""
    return pd.DataFrame({
        "id": [f"id_{i}" for i in range(len(starts))],
        "channel_name": [channel] * len(starts),
        "start": pd.to_datetime(list(starts)),
        "plaintext": plaintexts or ["core"] * len(starts),
        "keywords": [keywords] * len(starts),
        "themes": [themes] * len(starts),
    })


def _s3(rows):
    """Builds a DataFrame mimicking S3 read (channel_name, start, plaintext)
    from a list of tuples (channel, date, text)."""
    return pd.DataFrame({
        "channel_name": [r[0] for r in rows],
        "start": pd.to_datetime([r[1] for r in rows]),
        "plaintext": [r[2] for r in rows],
    })


def test_extract_adjacent_chunks_day_no_duplicate_this_id():
    """Verify that there are no duplicates in the this_id column (primary key)."""
    core = _core("2026-03-30 14:00:00")
    s3 = _s3([
        ("tf1", "2026-03-30 14:00:00", "core"),
        ("tf1", "2026-03-30 14:02:00", "t2"),
        ("tf1", "2026-03-30 13:58:00", "t3"),
        ("tf1", "2026-03-30 14:03:00", "t4"),
        ("france2", "2026-03-30 14:02:00", "t5"),
    ])

    result = utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                           country="france", channel_titles=FAKE_CHANNEL_TITLES)
    assert not result["this_id"].duplicated().any()


def test_extract_adjacent_chunks_day_number_of_chunks_kept():
    """Verify that only chunks within 2 min 30 s of a thematic chunk, on the same channel,
    are kept: core, t2 and t3. t4 is 3 min away from core, t5 is on another channel."""
    core = _core("2026-03-30 12:00:00")
    s3 = _s3([
        ("tf1", "2026-03-30 12:00:00", "core"),
        ("tf1", "2026-03-30 12:01:00", "t2"),
        ("tf1", "2026-03-30 11:58:00", "t3"),
        ("tf1", "2026-03-30 12:03:00", "t4"),
        ("france2", "2026-03-30 12:02:00", "t5"),
    ])

    result = utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                           country="france", channel_titles=FAKE_CHANNEL_TITLES)
    assert result.shape[0] == 3
    assert set(result["plaintext"]) == {"core", "t2", "t3"}


def test_extract_adjacent_chunks_day_flags_thematic_true_and_neighbors_false():
    """Verify that thematic_chunk is True for chunks coming from the keywords table
    and False for their neighbors fetched in S3."""
    core = _core("2026-03-30 12:00:00")
    s3 = _s3([
        ("tf1", "2026-03-30 12:00:00", "core"),
        ("tf1", "2026-03-30 12:01:00", "voisin"),
    ])

    result = utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                           country="france", channel_titles=FAKE_CHANNEL_TITLES)
    by_text = result.set_index("plaintext")
    assert by_text.loc["core", "thematic_chunk"]
    assert not by_text.loc["voisin", "thematic_chunk"]


def test_extract_adjacent_chunks_day_output_columns_id_format_and_title():
    """Verify all output columns (neither more nor less, regardless of order since insertion
    is done BY NAME), the this_id format (channel + Paris time), and channel title from
    the channel_titles dictionary passed as argument."""
    core = _core("2026-03-30 14:00:00")
    s3 = _s3([("tf1", "2026-03-30 14:00:00", "core")])

    result = utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                           country="france", channel_titles=FAKE_CHANNEL_TITLES)
    assert set(result.columns) == {
        "this_id", "channel_name", "channel_title", "start",
        "thematic_chunk",
        "source_keywords_id", "keywords", "themes", "plaintext",
    }
    assert result.loc[0, "this_id"] == "tf1_20260330140000"
    assert result.loc[0, "channel_title"] == "TF1"


def test_extract_adjacent_chunks_day_raises_if_core_missing_from_s3():
    """Verify that the function raises a ValueError if a thematic chunk (here 14h)
    does not exist on the S3 side, instead of ingesting an incomplete day."""
    core = _core("2026-03-30 12:00:00", "2026-03-30 14:00:00")
    s3 = _s3([("tf1", "2026-03-30 12:00:00", "core")])

    with pytest.raises(ValueError):
        utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                      country="france", channel_titles=FAKE_CHANNEL_TITLES)

def test_extract_adjacent_chunks_day_raises_if_core_and_s3_plaintext_differ():
    """Verify that the function raises a ValueError when, for the same channel and start time,
    the thematic chunk plaintext differs from the S3 chunk (likely timezone shift issue)."""
    core = _core("2026-03-30 12:00:00", plaintexts=["texte du core"])
    s3 = _s3([
        ("tf1", "2026-03-30 12:00:00", "texte différent côté S3"),
        ("tf1", "2026-03-30 12:01:00", "voisin"),
    ])

    with pytest.raises(ValueError):
        utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                      country="france", channel_titles=FAKE_CHANNEL_TITLES)

def test_extract_adjacent_chunks_day_raises_if_core_channel_absent_from_s3():
    """Verify that the function raises a ValueError when the thematic chunk channel
    does not exist at all on S3 side."""
    core = _core("2026-03-30 12:00:00", channel="france2")
    s3 = _s3([("tf1", "2026-03-30 12:00:00", "core")])

    with pytest.raises(ValueError):
        utils.extract_adjacent_chunks_day(core, s3, gap=GAP,
                                      country="france", channel_titles=FAKE_CHANNEL_TITLES)

# ---------- run: simulated environment ----------

def _table(start=date(2026, 7, 1), end=date(2026, 7, 4), kw=("incendie",)):
    """Name of the destination table computed by a new run with the default test config."""
    return utils.make_destination_table_name(start, end, list(kw))


@pytest.fixture
def env(monkeypatch):
    """Isolates run() from Postgres and S3. Destination is an in-memory DuckDB database,
    and source reads, S3 reads, and extraction are replaced with controllable functions via
    the returned object (failing days, keywords, logged calls).
    run() reads module globals set by load_env(): env.run(**overrides) sets them then calls run().
    run() calls the other functions through the utils module, so they are patched on utils."""
    _con = duckdb.connect()
    _con.execute(f"ATTACH ':memory:' AS {SOURCE_ALIAS}")
    _con.execute(f"CREATE SCHEMA {SOURCE_ALIAS}.public")
    real_configure_dest = utils.configure_barometre_destination
    state = SimpleNamespace(
        con=_con, real_configure_dest=real_configure_dest,
        read_source_days=[], read_source_keywords=[], read_s3_days=[], configure_dest_calls=0,
        failing_s3_days=set(), bad_insert_days=set(), keywords_value="incendie",
    )

    def fake_read_source(con, kw, start, end, country, source_table, postgres_db):
        """Returns a single thematic chunk at 12h for the requested day."""
        state.read_source_days.append(start)
        state.read_source_keywords.append(list(kw))

        return pd.DataFrame({
            "id": [f"id_{start}"], "channel_name": ["tf1"],
            "start": [pd.Timestamp(start) + pd.Timedelta(hours=12)],
            "plaintext": ["core"], "keywords": ["incendie"], "themes": ["climat_consequences"],
        })

    def fake_read_s3(con, day, channels, country, s3_bucket_name):
        """Simulates S3 read, failing for days in failing_s3_days."""
        state.read_s3_days.append(day)
        if day in state.failing_s3_days:
            raise FileNotFoundError(f"No parquet found on S3 for {day}")
        return pd.DataFrame()

    def fake_extract(core_day, s3_day_df, gap, country, channel_titles):
        """Returns thematic chunk and neighbor at +1 min. For bad_insert_days,
        neighbor has NULL plaintext, violating NOT NULL constraint upon insertion."""
        start = core_day["start"].iloc[0]
        neighbor = start + pd.Timedelta(minutes=1)
        bad = start.date() in state.bad_insert_days
        return pd.DataFrame({
            "this_id": [f"tf1_{start:%Y%m%d%H%M%S}", f"tf1_{neighbor:%Y%m%d%H%M%S}"],
            "channel_name": ["tf1", "tf1"],
            "channel_title": ["TF1", "TF1"],
            "start": [start, neighbor],
            "thematic_chunk": [True, False],
            "source_keywords_id": [core_day["id"].iloc[0], None],
            "keywords": [state.keywords_value, None],
            "themes": ["climat_consequences", None],
            "plaintext": ["core", None if bad else "voisin"],
        })

    def counting_configure_dest(con, dry_run, source_table, destination_table, postgres_db):
        """Counts calls then delegates to the real function."""
        state.configure_dest_calls += 1
        real_configure_dest(con, dry_run, source_table, destination_table, postgres_db)

    def run(**overrides):
        """Sets the module globals as load_env() would, then calls run()."""
        for k, v in make_cfg(**overrides).items():
            monkeypatch.setattr(tk, k, v, raising=False)
        tk.run()

    state.run = run

    monkeypatch.setattr(tk.duckdb, "connect", lambda *a, **k: _con)
    monkeypatch.setattr(utils, "configure_barometre_source", lambda *a, **k: None)
    monkeypatch.setattr(utils, "_configure_s3", lambda *a, **k: None)
    monkeypatch.setattr(utils, "get_keywords_in_ome", lambda kws, lang: ["incendie"])
    monkeypatch.setattr(utils, "read_barometre_keywords", fake_read_source)
    monkeypatch.setattr(utils, "read_day_from_s3", fake_read_s3)
    monkeypatch.setattr(utils, "extract_adjacent_chunks_day", fake_extract)
    monkeypatch.setattr(utils, "configure_barometre_destination", counting_configure_dest)
    yield state
    _con.close()


def _seed(env, table, this_id="old_1", start="2026-07-02 12:00:00", keywords="ancien"):
    """Creates destination table and registry (real functions), registers the run,
    and inserts an existing row into the table."""
    env.real_configure_dest(env.con, False, SOURCE_TABLE, table, SOURCE_ALIAS)
    utils.configure_barometre_registry(env.con, False, SOURCE_ALIAS, tk.REGISTRY_TABLE)
    utils.register_run(env.con, False, SOURCE_ALIAS, table, date(2026, 7, 1), date(2026, 7, 4),
                       ["incendie"], tk.REGISTRY_TABLE)
    env.con.execute(
        f"INSERT INTO {SOURCE_ALIAS}.{table} (this_id, channel_name, channel_title, start, "
        f"thematic_chunk, keywords, themes, plaintext) "
        f"VALUES (?, 'tf1', 'TF1', ?, TRUE, ?, 'x', 'texte')",
        [this_id, start, keywords],
    )


def _ids(env, table):
    """Returns the sorted list of this_id present in the destination table."""
    rows = env.con.execute(f"SELECT this_id FROM {SOURCE_ALIAS}.{table} ORDER BY this_id").fetchall()
    return [r[0] for r in rows]


# ---------- run: stop, atomicity, resume ----------

def test_run_stops_at_first_failing_day(env):
    """Verify that run stops at first failing day: day 1 is in table,
    day 3 is never read, and a RuntimeError is raised."""
    env.failing_s3_days = {date(2026, 7, 2)}

    with pytest.raises(RuntimeError):
        env.run(START_DATE=date(2026, 7, 1), END_DATE=date(2026, 7, 4))

    assert env.read_s3_days == [date(2026, 7, 1), date(2026, 7, 2)]
    ids = _ids(env, _table())
    assert ids and all(i.startswith("tf1_20260701") for i in ids)


def test_run_leaves_no_partial_day_when_insert_fails(env):
    """Verify that a failure during insertion leaves no rows for that day
    (even valid ones), previous day remains in table, and next day is not processed."""
    env.bad_insert_days = {date(2026, 7, 2)}

    with pytest.raises(RuntimeError):
        env.run(START_DATE=date(2026, 7, 1), END_DATE=date(2026, 7, 4))

    ids = _ids(env, _table())
    assert ids and all(i.startswith("tf1_20260701") for i in ids)
    assert date(2026, 7, 3) not in env.read_s3_days


def test_run_twice_does_not_duplicate_or_overwrite_rows(env):
    """Verify idempotency: replaying the same range creates no duplicates and, with
    ON CONFLICT DO NOTHING, does not modify existing rows."""
    kwargs = dict(START_DATE=date(2026, 7, 1), END_DATE=date(2026, 7, 3))
    table = _table(end=date(2026, 7, 3))
    query = f"SELECT this_id, keywords FROM {SOURCE_ALIAS}.{table} ORDER BY this_id"

    env.run(**kwargs)
    first = env.con.execute(query).fetchall()
    env.keywords_value = "canadair"
    env.run(**kwargs)
    second = env.con.execute(query).fetchall()

    assert len(first) == 4  # 2 days x (thematic + neighbor)
    assert second == first
    
    
def _thematic_tables(env):
    """Names of all thematic_* tables in the attached database (excluding the registry)."""
    rows = env.con.execute(
        "SELECT table_name FROM duckdb_tables() "
        f"WHERE database_name = '{SOURCE_ALIAS}' AND table_name LIKE 'thematic\\_%' ESCAPE '\\' "
        "AND table_name <> 'thematic_tables_registry'"
    ).fetchall()
    return sorted(r[0] for r in rows)


def test_run_resume_restarts_from_last_ingested_day(env):
    """Verify that without START_DATE, a table already in the registry is resumed from the day
    of its last chunk (retreated, harmless thanks to ON CONFLICT) up to END_DATE excluded,
    and that earlier days are not read again."""
    table = _table()
    _seed(env, table, start="2026-07-02 12:00:00")

    env.run(START_DATE=None, DESTINATION_TABLE=table, END_DATE=date(2026, 7, 4))

    assert env.read_source_days == [date(2026, 7, 2), date(2026, 7, 3)]


def test_run_resume_does_not_create_a_new_table(env):
    """Verify that resuming writes into the existing table: no other thematic table is created
    and the registry still holds a single entry."""
    table = _table()
    _seed(env, table, start="2026-07-02 12:00:00")

    env.run(START_DATE=None, DESTINATION_TABLE=table, END_DATE=date(2026, 7, 4))

    assert _thematic_tables(env) == [table]
    n_registered = env.con.execute(
        f"SELECT count(*) FROM {SOURCE_ALIAS}.{tk.REGISTRY_TABLE}").fetchone()[0]
    assert n_registered == 1
    assert "old_1" in _ids(env, table)
    assert len(_ids(env, table)) > 1   # new chunks were added to the same table


def test_run_resume_uses_keywords_from_registry_and_ignores_env_keywords(env):
    """Verify that a resumed run searches the keywords stored in the registry,
    whatever KEYWORDS contains."""
    table = _table()
    _seed(env, table, start="2026-07-02 12:00:00")

    env.run(START_DATE=None, DESTINATION_TABLE=table, KEYWORDS=["autre_mot"],
            END_DATE=date(2026, 7, 4))

    assert env.read_source_keywords
    assert all(kw == ["incendie"] for kw in env.read_source_keywords)


# ---------- destructive safeguards ----------

@pytest.mark.parametrize("destination",
                         ["keywords", "public.keywords", "PUBLIC.KEYWORDS", "autre_schema.keywords"])
def test_configure_barometre_destination_refuses_source_table_before_any_query(destination):
    """Verify that safeguard rejects destination with source table name,
    with or without schema, before any query (thus before CREATE)."""
    con = MagicMock()

    with pytest.raises(ValueError):
        utils.configure_barometre_destination(
            con, dry_run=False,
            source_table=SOURCE_TABLE, destination_table=destination,
            postgres_db=SOURCE_ALIAS,
        )

    con.execute.assert_not_called()


def test_run_dry_run_writes_nothing_and_counts_chunks(env, caplog):
    """Verify that in DRY_RUN mode nothing is written (existing table untouched)
    and the number of chunks that would be ingested is logged."""
    table = _table(end=date(2026, 7, 3))
    _seed(env, table)
    caplog.set_level(logging.INFO)

    env.run(START_DATE=date(2026, 7, 1), END_DATE=date(2026, 7, 3), DRY_RUN=True)

    assert _ids(env, table) == ["old_1"]
    assert "4 chunks would have been ingested" in caplog.text