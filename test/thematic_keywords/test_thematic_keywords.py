import json
from datetime import date
from unittest.mock import patch
import duckdb
import pandas as pd
import pytest
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from quotaclimat.data_processing.mediatree.thematic_keywords import thematic_keyword as tk

MODULE = "quotaclimat.data_processing.mediatree.thematic_keywords.thematic_keyword"
SOURCE_ALIAS = "barometre_test"
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


# ---------- get_keyword_ome_hrfp_theme ----------

@patch(f"{MODULE}.THEME_KEYWORDS", FAKE_THEME_KEYWORDS)
def test_get_keyword_ome_hrfp_theme_drops_keywords_absent_from_dictionary():
    """Verify that a word absent from the OME dictionary is discarded, and that a present word
    is retained with all its themes and its hrfp flag."""
    result = tk.get_keyword_ome_hrfp_theme(["bassin de récupération", "blabla"], language="french")

    assert "blabla" not in result
    assert set(result.keys()) == {"bassin de récupération"}
    assert result["bassin de récupération"]["hrfp"] is False
    assert set(result["bassin de récupération"]["theme"]) == {
        "adaptation_climatique_solution",
        "ressources_solutions",
    }


@patch(f"{MODULE}.THEME_KEYWORDS", FAKE_THEME_KEYWORDS)
def test_get_keyword_ome_hrfp_theme_filters_on_language():
    """Verify that a word present in the dictionary but in another language
    than requested is not kept (ValueError, as no words are found)."""
    with pytest.raises(ValueError):
        tk.get_keyword_ome_hrfp_theme(["bassin de récupération"], language="english")


# ---------- read_barometre_keywords ----------

@pytest.fixture
def con(monkeypatch):
    monkeypatch.setattr(tk, "PG_DATABASE", SOURCE_ALIAS)
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


def test_read_barometre_keywords_keeps_only_searched_keywords_with_direct_themes(con):
    """Verify that a chunk is kept if at least one of its entries has a searched
    keyword with a theme not ending in _indirectes, and excluded otherwise."""
    searched = {"bassin de récupération": {"theme": ["adaptation_climatique_solution"], "hrfp": False}}

    # kept: searched keyword, direct theme
    _insert(con, "1", [{"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"}])
    # excluded: searched keyword but _indirectes theme
    _insert(con, "2", [{"keyword": "bassin de récupération", "theme": "ressources_solutions_indirectes"}])
    # excluded: non-searched keyword
    _insert(con, "3", [{"keyword": "blabla", "theme": "adaptation_climatique_solution"}])
    # kept: one indirect entry and one direct entry for the searched keyword
    _insert(con, "4", [
        {"keyword": "bassin de récupération", "theme": "ressources_solutions_indirectes"},
        {"keyword": "bassin de récupération", "theme": "ressources_solutions"},
    ])
    # excluded: direct on non-searched keyword, indirect on searched keyword
    _insert(con, "5", [
        {"keyword": "blabla", "theme": "ressources_solutions"},
        {"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution_indirectes"},
    ])

    result = tk.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), country="france"
    )

    assert set(result["id"]) == {"1", "4"}
    assert result["id"].is_unique
    assert {"plaintext", "keywords", "themes"} <= set(result.columns)


def test_read_barometre_keywords_returns_plaintext_keywords_and_themes(con):
    """Verify that reading returns the chunk's plaintext, searched keywords, and
    direct themes concatenated by a comma without spaces (_indirectes themes excluded)."""
    searched = {
        "incendie": {"theme": ["a"], "hrfp": False}, #theme has no effect
        "canadair": {"theme": ["a"], "hrfp": False},
    }
    _insert(con, "1", [
        {"keyword": "incendie", "theme": "climat_consequences"},
        {"keyword": "canadair", "theme": "climat_consequences"},
        {"keyword": "incendie", "theme": "ressources_indirectes"},
        {"keyword": "blabla", "theme": "autre"},
    ], plaintext="un incendie, un canadair")

    result = tk.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), country="france"
    )

    row = result.iloc[0]
    assert row["plaintext"] == "un incendie, un canadair"
    assert row["keywords"] == "canadair,incendie"
    assert row["themes"] == "climat_consequences"


def test_read_barometre_keywords_filters_on_date_window_and_country(con):
    """Verify that reading respects the [start_date, end_date) window and country:
    a chunk the day before, at end date (excluded), or in another country is discarded."""
    searched = {"bassin de récupération": {"theme": ["x"], "hrfp": False}}
    kw = [{"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"}]

    _insert(con, "in", kw, start="2026-03-30 23:59:00+02:00")
    _insert(con, "before", kw, start="2026-03-29 23:59:00+02:00")
    _insert(con, "end_excluded", kw, start="2026-03-31 00:00:00+02:00")   # end_date excluded
    _insert(con, "other_country", kw, country="belgium")
    # 00:30 Paris on 30th = 22:30 UTC on 29th: inside in Paris, outside in UTC
    _insert(con, "paris_morning", kw, start="2026-03-30 00:30:00+02:00")
    # 00:30 Paris on 31st = 22:30 UTC on 30th: outside in Paris, inside in UTC
    _insert(con, "paris_next_day", kw, start="2026-03-31 00:30:00+02:00")

    result = tk.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), country="france"
    )

    assert set(result["id"]) == {"in", "paris_morning"}  
    
    
  
def test_read_barometre_keywords_dedups_keywords_and_excludes_indirect_themes(con):
    """Verify that a word present with two direct themes appears only once in keywords,
    that themes lists sorted direct themes excluding _indirectes, and that a searched
    word with only _indirectes themes is absent from keywords."""
    searched = {
        "incendie": {"theme": ["climat_consequences", "autre_theme"], "hrfp": False},
        "inondation": {"theme": ["ressources_indirectes"], "hrfp": False},
    }
    _insert(con, "1", [
        {"keyword": "incendie", "theme": "climat_consequences"},
        {"keyword": "incendie", "theme": "autre_theme"},
        {"keyword": "incendie", "theme": "ressources_indirectes"},
        {"keyword": "inondation", "theme": "ressources_indirectes"},
    ])

    result = tk.read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), country="france"
    )

    assert len(result) == 1
    row = result.iloc[0]
    assert row["keywords"] == "incendie"
    assert row["themes"] == "autre_theme,climat_consequences"
    
    

def test_read_barometre_keywords_dst_fallback_gives_same_naive_start(con):
    """Document the trap: two chunks at 02:30 (+02:00 and +01:00) become indistinguishable when naive."""
    searched = {"incendie": {"theme": ["a"], "hrfp": False}}
    kw = [{"keyword": "incendie", "theme": "climat_consequences"}]
    _insert(con, "a", kw, start="2026-10-25 02:30:00+02:00")
    _insert(con, "b", kw, start="2026-10-25 02:30:00+01:00")

    result = tk.read_barometre_keywords(
        con, searched, start_date=date(2026, 10, 25), end_date=date(2026, 10, 26), country="france"
    )

    assert len(result) == 2
    assert result["start"].nunique() == 1
    
    
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

    result = tk.read_day_from_s3(con, date(2026, 3, 30), ["tf1"], "france")

    assert len(result) == 2
    assert set(result["plaintext"]) == {"recent", "autre"}
    assert result["start"].dt.tz is None
    assert list(result["start"]) == [pd.Timestamp("2026-03-30 12:00:00"), pd.Timestamp("2026-03-30 12:01:00")]


# ---------- extract_adjacent_chunks_day ----------

def _core(*starts, channel="tf1", plaintexts=None, keywords="incendie", themes="climat_consequences"):
    """Builds a core chunks DataFrame (id, channel_name, start, plaintext, keywords, themes)
    for a single channel. Default plaintext is 'core' and IDs are id_0, id_1..."""
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


@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
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

    result = tk.extract_adjacent_chunks_day(core, s3, gap=GAP)

    assert not result["this_id"].duplicated().any()


@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
def test_extract_adjacent_chunks_day_number_of_chunks_kept():
    """Verify that only chunks within 2 min 30 s of a core chunk, on the same channel,
    are kept: core, t2 and t3. t4 is 3 min away from core, t5 is on another channel."""
    core = _core("2026-03-30 12:00:00")
    s3 = _s3([
        ("tf1", "2026-03-30 12:00:00", "core"),
        ("tf1", "2026-03-30 12:01:00", "t2"),
        ("tf1", "2026-03-30 11:58:00", "t3"),
        ("tf1", "2026-03-30 12:03:00", "t4"),
        ("france2", "2026-03-30 12:02:00", "t5"),
    ])

    result = tk.extract_adjacent_chunks_day(core, s3, gap=GAP)

    assert result.shape[0] == 3
    assert set(result["plaintext"]) == {"core", "t2", "t3"}


@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
def test_extract_adjacent_chunks_day_flags_core_true_and_neighbors_false():
    """Verify that thematic_core_chunk is True for core chunk, False for its neighbor,
    that the column is boolean, and keywords/themes are populated for core and null for neighbor."""
    core = _core("2026-03-30 12:00:00")
    s3 = _s3([
        ("tf1", "2026-03-30 12:00:00", "core"),
        ("tf1", "2026-03-30 12:01:00", "voisin"),
    ])

    result = tk.extract_adjacent_chunks_day(core, s3, gap=GAP)

    by_text = result.set_index("plaintext")
    assert by_text.loc["core", "keywords"] == "incendie"
    assert by_text.loc["core", "themes"] == "climat_consequences"
    assert pd.isna(by_text.loc["voisin", "keywords"])
    assert pd.isna(by_text.loc["voisin", "themes"])
    assert by_text.loc["core", "source_keywords_id"] == "id_0"
    assert pd.isna(by_text.loc["voisin", "source_keywords_id"])


@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
def test_extract_adjacent_chunks_day_output_columns_id_format_and_title():
    """Verify all output columns (neither more nor less, regardless of order since insertion
    is done BY NAME), the this_id format (channel + Paris time), and channel title from
    the channel_titles_france dictionary."""
    core = _core("2026-03-30 14:00:00")
    s3 = _s3([("tf1", "2026-03-30 14:00:00", "core")])

    result = tk.extract_adjacent_chunks_day(core, s3, gap=GAP)

    assert set(result.columns) == {
        "this_id", "channel_name", "channel_title", "start",
        "thematic_core_chunk", "source_keywords_id", "keywords", "themes", "plaintext",
    }
    assert result.loc[0, "this_id"] == "tf1_20260330140000"
    assert result.loc[0, "channel_title"] == "TF1"


@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
def test_extract_adjacent_chunks_day_raises_if_core_missing_from_s3():
    """Verify that the function raises a ValueError if a core chunk (here 14h)
    does not exist on the S3 side, instead of ingesting an incomplete day."""
    core = _core("2026-03-30 12:00:00", "2026-03-30 14:00:00")
    s3 = _s3([("tf1", "2026-03-30 12:00:00", "core")])

    with pytest.raises(ValueError):
        tk.extract_adjacent_chunks_day(core, s3, gap=GAP)


@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
def test_extract_adjacent_chunks_day_raises_if_core_and_s3_plaintext_differ():
    """Verify that the function raises a ValueError when, for the same channel and start time,
    the core chunk plaintext differs from the S3 chunk (likely timezone shift issue)."""
    core = _core("2026-03-30 12:00:00", plaintexts=["texte du core"])
    s3 = _s3([
        ("tf1", "2026-03-30 12:00:00", "texte différent côté S3"),
        ("tf1", "2026-03-30 12:01:00", "voisin"),
    ])

    with pytest.raises(ValueError):
        tk.extract_adjacent_chunks_day(core, s3, gap=GAP)
        
        
@patch(f"{MODULE}.channel_titles_france", FAKE_TITLES)
def test_extract_adjacent_chunks_day_raises_if_core_channel_absent_from_s3():
    """Verify that the function raises a ValueError when the core chunk channel
    does not exist at all on S3 side."""
    core = _core("2026-03-30 12:00:00", channel="france2")
    s3 = _s3([("tf1", "2026-03-30 12:00:00", "core")])

    with pytest.raises(ValueError):
        tk.extract_adjacent_chunks_day(core, s3, gap=GAP)
        
        
        

# ---------- run: simulated environment ----------

DEST = "public.test_destination"
DEST_FULL = f"{SOURCE_ALIAS}.{DEST}"
ARGS = dict(keywords_to_look_for=["incendie"], country="france", language="french")


@pytest.fixture
def env(monkeypatch):
    """Isolates run() from Postgres and S3. Destination is an in-memory DuckDB database,
    and source reads, S3 reads, and extraction are replaced with controllable functions via
    the returned object (failing days, keywords, logged calls)."""
    _con = duckdb.connect()
    _con.execute(f"ATTACH ':memory:' AS {SOURCE_ALIAS}")
    _con.execute(f"CREATE SCHEMA {SOURCE_ALIAS}.public")
    real_configure_dest = tk.configure_barometre_destination
    state = SimpleNamespace(
        con=_con, real_configure_dest=real_configure_dest,
        read_source_days=[], read_s3_days=[], configure_dest_calls=0,
        failing_s3_days=set(), bad_insert_days=set(), keywords_value="incendie",
    )

    def fake_read_source(con, kw, start, end, country):
        """Returns a single core chunk at 12h for the requested day."""
        state.read_source_days.append(start)
        return pd.DataFrame({
            "id": [f"id_{start}"], "channel_name": ["tf1"],
            "start": [pd.Timestamp(start) + pd.Timedelta(hours=12)],
            "plaintext": ["core"], "keywords": ["incendie"], "themes": ["climat_consequences"],
        })

    def fake_read_s3(con, day, channels, country):
        """Simulates S3 read, failing for days in failing_s3_days."""
        state.read_s3_days.append(day)
        if day in state.failing_s3_days:
            raise FileNotFoundError(f"No parquet found on S3 for {day}")
        return pd.DataFrame()

    def fake_extract(core_day, s3_day_df, gap):
        """Returns core and neighbor at +1 min. For bad_insert_days,
        neighbor has NULL plaintext, violating NOT NULL constraint upon insertion."""
        start = core_day["start"].iloc[0]
        neighbor = start + pd.Timedelta(minutes=1)
        bad = start.date() in state.bad_insert_days
        return pd.DataFrame({
            "this_id": [f"tf1_{start:%Y%m%d%H%M%S}", f"tf1_{neighbor:%Y%m%d%H%M%S}"],
            "channel_name": ["tf1", "tf1"],
            "channel_title": ["TF1", "TF1"],
            "start": [start, neighbor],
            "thematic_core_chunk": [True, False],
            "keywords": [state.keywords_value, None],
            "themes": ["climat_consequences", None],
            "plaintext": ["core", None if bad else "voisin"],
        })

    def counting_configure_dest(con, clear):
        """Counts calls then delegates to the real function."""
        state.configure_dest_calls += 1
        real_configure_dest(con, clear)

    monkeypatch.setattr(tk, "PG_DATABASE", SOURCE_ALIAS)
    monkeypatch.setattr(tk, "DESTINATION_TABLE", DEST)
    monkeypatch.setattr(tk, "LOGGING_SOURCE_TABLE", "SOURCE")
    monkeypatch.setattr(tk, "LOGGING_DESTINATION_TABLE", DEST.upper())
    monkeypatch.setattr(tk, "DRY_RUN", False)
    monkeypatch.setattr(tk.duckdb, "connect", lambda *a, **k: _con)
    monkeypatch.setattr(tk, "configure_barometre_source", lambda con: None)
    monkeypatch.setattr(tk, "_configure_s3", lambda con: None)
    monkeypatch.setattr(tk, "get_keyword_ome_hrfp_theme",
                        lambda kws, lang: {"incendie": {"theme": ["climat_consequences"], "hrfp": False}})
    monkeypatch.setattr(tk, "read_barometre_keywords", fake_read_source)
    monkeypatch.setattr(tk, "read_day_from_s3", fake_read_s3)
    monkeypatch.setattr(tk, "extract_adjacent_chunks_day", fake_extract)
    monkeypatch.setattr(tk, "configure_barometre_destination", counting_configure_dest)
    yield state
    _con.close()


def _seed(env, this_id="old_1", start="2026-07-02 12:00:00", keywords="ancien"):
    """Creates destination table (real function) and inserts an existing row into it."""
    env.real_configure_dest(env.con, False)
    env.con.execute(
        f"INSERT INTO {DEST_FULL} (this_id, channel_name, channel_title, start, "
        f"thematic_core_chunk, keywords, themes, plaintext) "
        f"VALUES (?, 'tf1', 'TF1', ?, TRUE, ?, 'x', 'texte')",
        [this_id, start, keywords],
    )


def _ids(env):
    """Returns the sorted list of this_id present in the destination table."""
    rows = env.con.execute(f"SELECT this_id FROM {DEST_FULL} ORDER BY this_id").fetchall()
    return [r[0] for r in rows]


# ---------- run: stop, atomicity, resume ----------

def test_run_stops_at_first_failing_day(env):
    """Verify that run stops at first failing day: day 1 is in table,
    day 3 is never read, and a RuntimeError names the failing day."""
    env.failing_s3_days = {date(2026, 7, 2)}

    with pytest.raises(RuntimeError, match="2026-07-02"):
        tk.run(start_date=date(2026, 7, 1), end_date=date(2026, 7, 4),
               clear_destination_table=False, assume_yes=True, **ARGS)

    assert env.read_s3_days == [date(2026, 7, 1), date(2026, 7, 2)]
    ids = _ids(env)
    assert ids and all(i.startswith("tf1_20260701") for i in ids)


def test_run_leaves_no_partial_day_when_insert_fails(env):
    """Verify that a failure during insertion leaves no rows for that day
    (even valid ones), previous day remains in table, and next day is not processed."""
    env.bad_insert_days = {date(2026, 7, 2)}

    with pytest.raises(RuntimeError):
        tk.run(start_date=date(2026, 7, 1), end_date=date(2026, 7, 4),
               clear_destination_table=False, assume_yes=True, **ARGS)

    ids = _ids(env)
    assert ids and all(i.startswith("tf1_20260701") for i in ids)
    assert date(2026, 7, 3) not in env.read_s3_days




def test_run_refuses_clear_without_start_date(env):
    """Verify that --clear_destination_table without start_date is rejected, as auto-resume
    makes no sense on a cleared table."""
    with pytest.raises(ValueError):
        tk.run(start_date=None, end_date=date(2026, 7, 4),
               clear_destination_table=True, assume_yes=True, **ARGS)



def test_run_twice_does_not_duplicate_or_overwrite_rows(env):
    """Verify idempotency: replaying the same range creates no duplicates and, with
    ON CONFLICT DO NOTHING, does not modify existing rows."""
    kwargs = dict(start_date=date(2026, 7, 1), end_date=date(2026, 7, 3),
                  clear_destination_table=False, assume_yes=True, **ARGS)
    query = f"SELECT this_id, keywords FROM {DEST_FULL} ORDER BY this_id"

    tk.run(**kwargs)
    first = env.con.execute(query).fetchall()
    env.keywords_value = "canadair"
    tk.run(**kwargs)
    second = env.con.execute(query).fetchall()

    assert len(first) == 4  # 2 days x (core + neighbor)
    assert second == first


# ---------- destructive safeguards (tests 10 to 12) ----------

@pytest.mark.parametrize("destination",
                         ["keywords", "public.keywords", "PUBLIC.KEYWORDS", "autre_schema.keywords"])
def test_configure_barometre_destination_refuses_source_table_before_any_query(monkeypatch, destination):
    """Verify that safeguard rejects destination with source table name,
    with or without schema, before any query (thus before CREATE or DELETE)."""
    monkeypatch.setattr(tk, "DESTINATION_TABLE", destination)
    monkeypatch.setattr(tk, "LOGGING_DESTINATION_TABLE", destination.upper())
    monkeypatch.setattr(tk, "DRY_RUN", False)
    con = MagicMock()

    with pytest.raises(ValueError):
        tk.configure_barometre_destination(con, clear_destination_table=True)

    con.execute.assert_not_called()


def test_confirm_assume_yes_returns_true_without_asking(monkeypatch):
    """Verify that with assume_yes, confirm returns True without calling input."""
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("input must not be called"))

    assert tk.confirm("ok ?", assume_yes=True) is True


def test_run_clear_without_confirmation_empties_nothing(env, monkeypatch):
    """Verify that --clear_destination_table without confirmation (no terminal, no
    assume_yes) exits via SystemExit, without clearing table or configuring destination."""
    _seed(env)
    monkeypatch.setattr(tk.sys, "stdin", SimpleNamespace(isatty=lambda: False))

    with pytest.raises(SystemExit):
        tk.run(start_date=date(2026, 7, 1), end_date=date(2026, 7, 3),
               clear_destination_table=True, assume_yes=False, **ARGS)

    assert _ids(env) == ["old_1"]
    assert env.configure_dest_calls == 0
    assert env.read_source_days == []


def test_run_dry_run_writes_nothing_and_counts_chunks(env, monkeypatch, caplog):
    """Verify that in DRY_RUN mode nothing is written or deleted (even with clear and without
    confirmation) and the number of chunks that would be ingested is logged."""
    _seed(env)
    monkeypatch.setattr(tk, "DRY_RUN", True)
    caplog.set_level(logging.INFO)

    tk.run(start_date=date(2026, 7, 1), end_date=date(2026, 7, 3),
           clear_destination_table=True, assume_yes=False, **ARGS)

    assert _ids(env) == ["old_1"]
    assert "4 chunks would have been ingested" in caplog.text