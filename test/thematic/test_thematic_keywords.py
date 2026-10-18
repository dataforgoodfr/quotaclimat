import json
import logging
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest
import yaml

import quotaclimat.data_processing.thematic.thematic_keywords as tk
import quotaclimat.data_processing.thematic.thematic_keywords_utils as kw_utils
import quotaclimat.data_processing.thematic.thematic_utils as utils


DB = "barometre"
SCHEMA = "public"
PARIS = "Europe/Paris"
T0 = pd.Timestamp("2025-03-10 10:00", tz=PARIS)
MIN = timedelta(minutes=1)
PARIS_DAY = f"CAST(start AT TIME ZONE '{PARIS}' AS DATE)"
D1, D2, D3, D4, D5 = (date(2025, 3, d) for d in (10, 11, 12, 13, 14))
TITLES = {"france": {"tf1": "TF1 (test)", "m6": "M6 (test)"}}

def _wall(ts):
    """Paris wall-clock time as text, for SQL inserts."""
    return ts.tz_convert(PARIS).strftime("%Y-%m-%d %H:%M:%S")

# ───────────────────────── helpers & fixtures ─────────────────────────
@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone='Europe/Paris'")
    c.execute(f"ATTACH ':memory:' AS {DB}")
    c.execute(f"CREATE SCHEMA {DB}.{SCHEMA}")
    c.execute(f"""
        CREATE TABLE {DB}.{SCHEMA}.keywords (
            id VARCHAR, channel_name VARCHAR, start TIMESTAMPTZ, plaintext VARCHAR,
            keywords_with_timestamp VARCHAR, country VARCHAR
        )
    """)
    yield c
    c.close()

def _add_kw(con, id_, channel, start, plaintext, pairs, country="france"):
    """pairs = [(keyword, theme), ...]"""
    con.execute(
        f"INSERT INTO {DB}.{SCHEMA}.keywords VALUES (?, ?, CAST(? AS TIMESTAMP) AT TIME ZONE 'Europe/Paris', ?, ?, ?)",
        [id_, channel, start, plaintext,
         json.dumps([{"keyword": k, "theme": t} for k, t in pairs]), country],
    )

def _read(con, keywords):
    return kw_utils.read_barometre_keywords(
        con, keywords, D1, D2, "france", f"{SCHEMA}.keywords", DB
    )

def _kw_df(rows):
    """rows = [(id, channel, start, plaintext)]"""
    return pd.DataFrame([
        {"id": i, "channel_name": c, "start": s, "plaintext": p,
         "keywords": "inondation", "themes": "changement_climatique_constat"}
        for i, c, s, p in rows
    ])

def _s3_df(rows):
    """rows = [(channel, start, plaintext)]"""
    return pd.DataFrame([{"channel_name": c, "start": s, "plaintext": p} for c, s, p in rows])

def _extract(kw, s3, titles=TITLES):
    return kw_utils.extract_adjacent_chunks_day(kw, s3, tk.GAP, "france", titles)

class _FakeRel:
    def __init__(self, rows=None, df=None):
        self._rows, self._df = rows, df

    def fetchall(self):
        return self._rows

    def df(self):
        return self._df

class _FakeS3Con:
    """Stands for the duckdb connection: answers the glob query then the read_parquet query."""
    def __init__(self, df, channels=("tf1",)):
        self.df_to_return = df
        self.files = [f"s3://bucket/year=2025/month=3/day=10/channel={ch}/x.parquet" for ch in channels]

    def sql(self, query):
        if "glob(" in query:
            return _FakeRel(rows=[(f,) for f in self.files])
        return _FakeRel(df=self.df_to_return.copy())

class RunEnv:
    """Everything run() needs, with no Postgres and no S3."""
    destination_table = "inondations_thematic_kw_fr"

    def __init__(self, con, monkeypatch, tmp_path):
        self.con, self.mp = con, monkeypatch
        self.s3_rows, self.read_days, self.failing_days, self._n = [], [], set(), 0
        self.table = f"{DB}.{SCHEMA}.{self.destination_table}"

        yml = tmp_path / "thematic.yml"
        yml.write_text(yaml.safe_dump({"inondations":{"french": {"audiovisuel": ["inondation", "crue"]}}}))
        monkeypatch.setattr(tk, "THEMATIC_YML", yml)
        monkeypatch.setattr(tk.duckdb, "connect", lambda *a, **k: con)
        monkeypatch.setattr(tk.utils, "configure_barometre_source", lambda *a, **k: None)
        monkeypatch.setattr(tk.kw_utils, "_configure_s3", lambda *a, **k: None)
        monkeypatch.setattr(tk.kw_utils, "read_day_from_s3", self._fake_read_day_from_s3)
        self.configure(start=D1, end=D2)

    def configure(self, start=None, end=D2, dry_run=False, source_table=f"{SCHEMA}.keywords",
                  thematic="inondations"):
        values = dict(
            START_DATE=start, END_DATE=end, THEMATIC=thematic, COUNTRY="france", LANGUAGE="french",
            DRY_RUN=dry_run, SOURCE_TABLE=source_table, POSTGRES_USER="u", POSTGRES_PASSWORD="p",
            POSTGRES_HOST="h", POSTGRES_PORT="5432", POSTGRES_DB=DB, S3_ENDPOINT_URL="http://s3",
            S3_REGION="r", S3_ACCESS_KEY="a", S3_SECRET_KEY="s", S3_BUCKET_NAME="bucket",
        )
        for k, v in values.items():
            self.mp.setattr(tk, k, v, raising=False)

    def add_s3(self, start, plaintext, channel="tf1"):
        self.s3_rows.append({"channel_name": channel, "start": start, "plaintext": plaintext})

    def add_day(self, day, channel="tf1"):
        """One thematic chunk at 10:00 + one neighbour at 10:02 → 2 rows ingested for that day."""
        self._n += 1
        start = pd.Timestamp(day.year, day.month, day.day, 10, tz=PARIS)

        _add_kw(self.con, f"kw_{self._n}", channel, _wall(start), f"core {self._n}",
                [("inondation", "changement_climatique_constat")])
        self.add_s3(start, f"core {self._n}", channel)
        self.add_s3(start + 2 * MIN, f"neighbour {self._n}", channel)

    def _fake_read_day_from_s3(self, _con, day, channels, country, bucket):
        self.read_days.append(day)
        if day in self.failing_days:
            raise FileNotFoundError(f"No parquet found on S3 for {day}")
        rows = [r for r in self.s3_rows
                if r["start"].date() == day and r["channel_name"] in channels]
        return pd.DataFrame(rows, columns=["channel_name", "start", "plaintext"])

    def create_destination(self):
        kw_utils.configure_barometre_thematic_keywords(
            self.con, False, self.destination_table, DB, SCHEMA)
    

    def count(self):
        return self.con.execute(f"SELECT count(*) FROM {self.table}").fetchone()[0]

    def days_in_table(self):
        return [r[0] for r in self.con.execute(
            f"SELECT DISTINCT {PARIS_DAY} FROM {self.table} ORDER BY 1").fetchall()]

    def tables(self):
        return {r[0] for r in self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?", [DB, SCHEMA]).fetchall()}


@pytest.fixture
def env(con, monkeypatch, tmp_path):
    return RunEnv(con, monkeypatch, tmp_path)


# ───────────────────────── read_barometre_keywords ─────────────────────────
def test_read_barometre_keywords_dedups_keywords_and_themes(con):
    """Verify that a word present with several themes appears only once in keywords,
    that themes lists the distinct themes sorted (direct and _indirectes), and that a searched
    word with only _indirectes themes still appears in keywords."""
    _add_kw(con, "a", "tf1", "2025-03-10 10:00:00", "pa", [
        ("inondation", "changement_climatique_constat_indirectes"),
        ("inondation", "changement_climatique_constat"),
        ("inondation", "changement_climatique_constat"),
    ])
    _add_kw(con, "b", "tf1", "2025-03-10 11:00:00", "pb",
            [("crue", "adaptation_climatique_solutions_indirectes")])
    _add_kw(con, "c", "tf1", "2025-03-10 12:00:00", "pc", [
        ("inondation", "changement_climatique_constat"),
        ("crue", "adaptation_climatique_solutions_indirectes"),
    ])

    out = _read(con, ["inondation", "crue"]).set_index("id")

    assert out.loc["a", "keywords"] == "inondation"
    assert out.loc["a", "themes"] == "changement_climatique_constat,changement_climatique_constat_indirectes"
    assert out.loc["b", "keywords"] == "crue"
    assert out.loc["b", "themes"] == "adaptation_climatique_solutions_indirectes"
    assert out.loc["c", "keywords"] == "crue,inondation"
    assert out.loc["c", "themes"] == "adaptation_climatique_solutions_indirectes,changement_climatique_constat"


# ───────────────────────── read_day_from_s3 ─────────────────────────
def test_read_day_from_s3_keeps_last_row_for_duplicate_channel_and_start():
    """Verify that two S3 rows with the same (channel_name, start) are deduplicated
    by keeping the last one."""
    raw = _s3_df([
        ("tf1", T0, "first version"),
        ("tf1", T0, "last version"),
        ("tf1", T0 + 2 * MIN, "other chunk"),
    ])

    out = kw_utils.read_day_from_s3(_FakeS3Con(raw), D1, ["tf1"], "france", "bucket")

    assert len(out) == 2
    assert out.loc[out["start"] == T0, "plaintext"].tolist() == ["last version"]


# ───────────────────────── extract_adjacent_chunks_day ─────────────────────────
def test_extract_adjacent_chunks_day_no_duplicate_this_id():
    """Verify that there are no duplicates in the this_id column (primary key)."""
    kw = _kw_df([
        ("kw1", "tf1", T0, "core"),
        ("kw1bis", "tf1", T0, "core"),            # same (channel, start) twice in the source
        ("kw2", "tf1", T0 + 2 * MIN, "core 2"),   # overlapping neighbourhood
    ])
    s3 = _s3_df([
        ("tf1", T0 - 2 * MIN, "n1"), ("tf1", T0, "core"),
        ("tf1", T0 + 2 * MIN, "core 2"), ("tf1", T0 + 4 * MIN, "n2"),
    ])

    out = _extract(kw, s3)

    assert out["this_id"].is_unique
    assert len(out) == 4


def _scenario():
    kw = _kw_df([("kw1", "tf1", T0, "core")])
    s3 = _s3_df([
        ("tf1", T0, "core"),
        ("tf1", T0 + 2 * MIN, "t2"),
        ("tf1", T0 - 2 * MIN, "t3"),
        ("tf1", T0 + 3 * MIN, "t4"),   # too far
        ("m6", T0, "t5"),              # other channel
    ])
    return kw, s3


def test_extract_adjacent_chunks_day_number_of_chunks_kept():
    """Verify that only chunks within 2 min 30 s of a thematic chunk, on the same channel,
    are kept: core, t2 and t3. t4 is 3 min away from core, t5 is on another channel."""
    out = _extract(*_scenario())

    assert len(out) == 3
    assert set(out["plaintext"]) == {"core", "t2", "t3"}
    assert set(out["channel_name"]) == {"tf1"}


def test_extract_adjacent_chunks_day_flags_thematic_true_and_neighbors_false():
    """Verify that thematic_chunk is True for chunks coming from the keywords table
    and False for their neighbors fetched in S3."""
    out = _extract(*_scenario()).set_index("plaintext")

    assert bool(out.loc["core", "thematic_chunk"]) is True
    assert out.loc["core", "source_keywords_id"] == "kw1"
    for neighbour in ("t2", "t3"):
        assert bool(out.loc[neighbour, "thematic_chunk"]) is False
        assert pd.isna(out.loc[neighbour, "source_keywords_id"])
        assert pd.isna(out.loc[neighbour, "keywords"])
        assert pd.isna(out.loc[neighbour, "themes"])


def test_extract_adjacent_chunks_day_output_columns_id_format_and_title():
    """Verify all output columns (neither more nor less, regardless of order since insertion
    is done BY NAME), the this_id format (channel + Paris time), and channel title from
    the channel_titles dictionary passed as argument."""
    out = _extract(*_scenario())

    assert set(out.columns) == {
        "this_id", "channel_name", "channel_title", "start", "thematic_chunk",
        "source_keywords_id", "keywords", "themes", "plaintext",
    }
    assert len(out.columns) == 9
    assert set(out["this_id"]) == {
        "tf1_20250310100000_0100", "tf1_20250310100200_0100", "tf1_20250310095800_0100"
    }
    assert set(out["channel_title"]) == {"TF1 (test)"}


def test_extract_adjacent_chunks_day_raises_if_core_missing_from_s3():
    """Verify that the function raises a ValueError if a thematic chunk (here 14h)
    does not exist on the S3 side, instead of ingesting an incomplete day."""
    kw = _kw_df([
        ("kw1", "tf1", T0, "core 10h"),
        ("kw2", "tf1", T0 + timedelta(hours=4), "core 14h"),
    ])
    s3 = _s3_df([("tf1", T0, "core 10h"), ("tf1", T0 + 2 * MIN, "n")])

    with pytest.raises(ValueError):
        _extract(kw, s3)


def test_extract_adjacent_chunks_day_raises_if_core_and_s3_plaintext_differ():
    """Verify that the function raises a ValueError when, for the same channel and start time,
    the thematic chunk plaintext differs from the S3 chunk (likely timezone shift issue)."""
    kw = _kw_df([("kw1", "tf1", T0, "plaintext from source")])
    s3 = _s3_df([("tf1", T0, "plaintext from s3")])

    with pytest.raises(ValueError):
        _extract(kw, s3)


def test_extract_adjacent_chunks_day_raises_if_core_channel_absent_from_s3():
    """Verify that the function raises a ValueError when the thematic chunk channel
    does not exist at all on S3 side."""
    kw = _kw_df([("kw1", "tf1", T0, "core tf1"), ("kw2", "m6", T0, "core m6")])
    s3 = _s3_df([("tf1", T0, "core tf1")])

    with pytest.raises(ValueError):
        _extract(kw, s3)


# ───────────────────────── run() ─────────────────────────
def test_run_stops_at_first_failing_day(env):
    """Verify that run stops at first failing day: day 1 is in table,
    day 3 is never read, and a RuntimeError is raised."""
    for d in (D1, D2, D3):
        env.add_day(d)
    env.failing_days = {D2}
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        tk.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 2


def test_run_leaves_no_partial_day_when_insert_fails(env):
    """Verify that a failure during insertion leaves no rows for that day
    (even valid ones), previous day remains in table, and next day is not processed."""
    env.add_day(D1)
    env.add_day(D2)
    # NULL plaintext on an adjacent chunk of day 2 → NOT NULL violation at INSERT time,
    # while the thematic chunk of that day is valid
    env.add_s3(pd.Timestamp(2025, 3, 11, 9, 59, tz=PARIS), None)
    env.add_day(D3)
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        tk.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 2


def test_run_twice_does_not_duplicate_or_overwrite_rows(env):
    """Verify idempotency: replaying the same range creates no duplicates and, with
    ON CONFLICT DO NOTHING, does not modify existing rows."""
    for d in (D1, D2, D3):
        env.add_day(d)
    env.configure(start=D1, end=D4)
    tk.run()
    assert env.count() == 6

    env.con.execute(
        f"UPDATE {env.table} SET plaintext = 'SENTINEL' "
        f"WHERE thematic_chunk AND {PARIS_DAY} = DATE '{D3}'"
    )
    # replay through a resume (a new run with START_DATE would clear the table first)
    env.configure(start=None, end=D4)
    tk.run()

    assert env.count() == 6
    assert env.con.execute(f"SELECT count(DISTINCT this_id) FROM {env.table}").fetchone()[0] == 6
    assert env.con.execute(
        f"SELECT count(*) FROM {env.table} WHERE plaintext = 'SENTINEL'").fetchone()[0] == 1


def test_run_resume_restarts_from_last_ingested_day(env):
    """Verify that a resume (no START_DATE) restarts from the day of the last ingested chunk
    (not from the first day, not from the next one) and processes up to END_DATE."""
    for d in (D1, D2, D3, D4):
        env.add_day(d)
    env.configure(start=D1, end=D3)
    tk.run()
    assert env.days_in_table() == [D1, D2]

    env.read_days.clear()
    env.configure(start=None, end=D5)
    tk.run()

    assert env.read_days == [D2, D3, D4]
    assert env.days_in_table() == [D1, D2, D3, D4]
    assert env.count() == 8


def test_run_resume_does_not_create_a_new_table(env):
    """Verify that resuming writes into the existing table: no other thematic table is created
    and the destination table keeps being the only one next to the source table."""
    for d in (D1, D2, D3):
        env.add_day(d)
    env.configure(start=D1, end=D2)
    tk.run()
    tables_before = env.tables()
    assert tables_before == {"keywords", env.destination_table}

    env.configure(start=None, end=D4)
    tk.run()

    assert env.tables() == tables_before
    assert env.days_in_table() == [D1, D2, D3]


@pytest.mark.parametrize("name", ["factiva_articles", "inondations_kw_fr", "autre_table"])
def test_clear_refuses_non_thematic_table_and_keeps_rows(env, name):
    env.con.execute(f"CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.{name} (id INTEGER)")
    env.con.execute(f"INSERT INTO {DB}.{SCHEMA}.{name} VALUES (1), (2)")
    before = env.con.execute(f"SELECT count(*) FROM {DB}.{SCHEMA}.{name}").fetchone()[0]

    with pytest.raises(ValueError, match="Refusing to clear"):
        utils.clear_destination_table(env.con, False, name, DB, SCHEMA)

    after = env.con.execute(f"SELECT count(*) FROM {DB}.{SCHEMA}.{name}").fetchone()[0]
    assert after == before


def test_run_dry_run_writes_nothing_and_counts_chunks(env, caplog):
    """Verify that in DRY_RUN mode nothing is written (existing table untouched)
    and the number of chunks that would be ingested is logged."""
    env.add_day(D1)
    env.add_day(D2)
    env.create_destination()
    env.con.execute(
        f"INSERT INTO {env.table} (this_id, channel_name, channel_title, start, thematic_chunk, plaintext) "
        "VALUES ('sentinel', 'tf1', 'TF1', TIMESTAMPTZ '2025-01-01 10:00:00+01', true, 'old')"
    )
    env.configure(start=D1, end=D3, dry_run=True)

    with caplog.at_level(logging.INFO):
        tk.run()

    assert env.con.execute(f"SELECT this_id FROM {env.table}").fetchall() == [("sentinel",)]
    assert "DRY_RUN: 4 chunks would have been ingested" in caplog.text