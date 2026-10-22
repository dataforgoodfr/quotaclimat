import json
import logging
from datetime import date, timedelta

import duckdb
import pandas as pd
import pytest
import yaml

import quotaclimat.data_processing.thematic_extracts.thematic_keywords as tk
import quotaclimat.data_processing.thematic_extracts.thematic_keywords_utils as kw_utils
import quotaclimat.data_processing.thematic_extracts.thematic_utils as utils


DB = "barometre"
SCHEMA = "public"
DESTINATION_SCHEMA = tk.DESTINATION_SCHEMA
PARIS = "Europe/Paris"
T0 = pd.Timestamp("2025-03-10 10:00", tz=PARIS)
MIN = timedelta(minutes=1)
PARIS_DAY = f"CAST(start AT TIME ZONE '{PARIS}' AS DATE)"
D1, D2, D3, D4, D5 = (date(2025, 3, d) for d in (10, 11, 12, 13, 14))
TITLES = {"france": {"tf1": "TF1 (test)", "m6": "M6 (test)"}}
PERIOD = "p_test"

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
    c.execute(f"CREATE SCHEMA {DB}.{DESTINATION_SCHEMA}")
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
         "keywords_found": "inondation", "themes": "changement_climatique_constat"}
        for i, c, s, p in rows
    ])

def _s3_df(rows):
    """rows = [(channel, start, plaintext)]"""
    return pd.DataFrame([{"channel_name": c, "start": s, "plaintext": p} for c, s, p in rows])

def _extract(kw, s3, titles=TITLES):
    return kw_utils.extract_adjacent_chunks_day(kw, s3, tk.GAP, "france", titles)

class RunEnv:
    """Everything run() needs, with no Postgres and no S3."""

    def __init__(self, con, monkeypatch, tmp_path):
        self.con, self.mp = con, monkeypatch
        self.s3_rows, self.read_days, self.failing_days, self._n = [], [], set(), 0

        self.yml = tmp_path / "thematic.yml"
        self.keywords = {"keywords": {"french": {"audiovisuel": ["inondation", "crue"]}}}
        monkeypatch.setattr(tk, "THEMATIC_YML", self.yml)
        monkeypatch.setattr(tk.duckdb, "connect", lambda *a, **k: con)
        monkeypatch.setattr(tk.utils, "configure_barometre_source", lambda *a, **k: None)
        monkeypatch.setattr(tk.kw_utils, "_configure_s3", lambda *a, **k: None)
        monkeypatch.setattr(tk.kw_utils, "read_day_from_s3", self._fake_read_day_from_s3)
        self.configure(start=D1, end=D2)

    def configure(self, start=None, end=D2, dry_run=False, source_table=f"{SCHEMA}.keywords",
                  thematic="inondations", mode="new"):
        values = dict(
            THEMATIC=thematic, COUNTRY="france", LANGUAGE="french",
            DRY_RUN=dry_run, MODE=mode, SOURCE_TABLE=source_table, POSTGRES_USER="u", POSTGRES_PASSWORD="p",
            POSTGRES_HOST="h", POSTGRES_PORT="5432", POSTGRES_DB=DB, S3_ENDPOINT_URL="http://s3",
            S3_REGION="r", S3_ACCESS_KEY="a", S3_SECRET_KEY="s", S3_BUCKET_NAME="bucket",
        )
        for k, v in values.items():
            self.mp.setattr(tk, k, v, raising=False)
        # one period from start (included) to end (excluded)
        if start is not None:
            content = {"inondations": {**self.keywords,
                                       "periodes": {"france": [{"id": PERIOD, "start": start,
                                                                "end": end}]}}}
            self.yml.write_text(yaml.safe_dump(content))
        self.destination_table = utils.make_destination_table_name(
            thematic, tk.MEDIA_TYPE, tk.COUNTRIES["france"])
        self.table = f"{DB}.{DESTINATION_SCHEMA}.{self.destination_table}"

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
        kw_utils.create_thematic_keywords_table(
            self.con, False, self.destination_table, DB, DESTINATION_SCHEMA)

    def insert_sentinel(self, this_id="sentinel"):
        self.con.execute(
            f"INSERT INTO {self.table} (this_id, channel_name, channel_title, start, thematic_chunk, plaintext_normalized, country, period) "
            f"VALUES ('{this_id}', 'tf1', 'TF1', TIMESTAMPTZ '2025-01-01 10:00:00+01', true, 'old', 'france', '{PERIOD}')")
    

    def count(self):
        return self.con.execute(f"SELECT count(*) FROM {self.table}").fetchone()[0]

    def days_in_table(self):
        return [r[0] for r in self.con.execute(
            f"SELECT DISTINCT {PARIS_DAY} FROM {self.table} ORDER BY 1").fetchall()]

    def tables(self):
        return {r[0] for r in self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?", [DB, DESTINATION_SCHEMA]).fetchall()}


@pytest.fixture
def env(con, monkeypatch, tmp_path):
    return RunEnv(con, monkeypatch, tmp_path)


# ───────────────────────── read_barometre_keywords ─────────────────────────
def test_read_barometre_keywords_dedups_keywords_and_themes(con):
    """Verify that a word present with several themes appears only once in keywords_found,
    that themes lists the distinct themes sorted (direct and _indirectes), and that a searched
    word with only _indirectes themes still appears in keywords_found."""
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

    assert out.loc["a", "keywords_found"] == "inondation"
    assert out.loc["a", "themes"] == "changement_climatique_constat,changement_climatique_constat_indirectes"
    assert out.loc["b", "keywords_found"] == "crue"
    assert out.loc["b", "themes"] == "adaptation_climatique_solutions_indirectes"
    assert out.loc["c", "keywords_found"] == "crue,inondation"
    assert out.loc["c", "themes"] == "adaptation_climatique_solutions_indirectes,changement_climatique_constat"


# ───────────────────────── extract_adjacent_chunks_day ─────────────────────────


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
    are kept: core, t2 and t3. t4 is 3 min away from core, t5 is on another channel.
    thematic_chunk is True for the chunk from the keywords table, False for its S3 neighbours."""
    out = _extract(*_scenario())

    assert len(out) == 3
    assert set(out["plaintext_normalized"]) == {"core", "t2", "t3"}
    assert set(out["channel_name"]) == {"tf1"}

    out = out.set_index("plaintext_normalized")
    assert bool(out.loc["core", "thematic_chunk"]) is True
    assert out.loc["core", "source_keywords_id"] == "kw1"
    for neighbour in ("t2", "t3"):
        assert bool(out.loc[neighbour, "thematic_chunk"]) is False
        assert pd.isna(out.loc[neighbour, "source_keywords_id"])
        assert pd.isna(out.loc[neighbour, "keywords_found"])
        assert pd.isna(out.loc[neighbour, "themes"])


def test_extract_adjacent_chunks_day_output_columns_id_format_and_title():
    """Verify all output columns (neither more nor less, regardless of order since insertion
    is done BY NAME), the this_id format (channel + UTC time, 10:00 Paris = 09:00 UTC in March),
    that this_id does not depend on the input timezone, and channel title from
    the channel_titles dictionary passed as argument."""
    kw, s3 = _scenario()
    out = _extract(kw, s3)

    assert set(out.columns) == {
        "this_id", "channel_name", "channel_title", "start", "thematic_chunk",
        "source_keywords_id", "keywords_found", "themes", "plaintext_normalized",
    }
    assert len(out.columns) == 9
    assert set(out["this_id"]) == {
        "tf1_20250310090000", "tf1_20250310090200", "tf1_20250310085800"
    }
    assert set(out["channel_title"]) == {"TF1 (test)"}

    # same instants given in UTC: same ids
    to_utc = lambda df: df.assign(start=df["start"].dt.tz_convert("UTC"))
    out_utc = _extract(to_utc(kw), to_utc(s3))
    assert set(out_utc["this_id"]) == set(out["this_id"])


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


# ───────────────────────── run() ─────────────────────────
def test_run_ingests_every_period_into_one_table(env):
    """Verify that two non-contiguous periods go into the same table, each row tagged with
    its period id and the country, that days outside the periods (D2, D4) are not read,
    and that plaintext_normalized is filled with the normalized plaintext."""
    for d in (D1, D2, D3, D4):
        env.add_day(d)
    # one more neighbour on D1, at 09:58, with accents, apostrophe and uppercase
    env.add_s3(pd.Timestamp(D1.year, D1.month, D1.day, 9, 58, tz=PARIS), "L'Été")
    env.configure(start=D1, end=D2)
    env.yml.write_text(yaml.safe_dump({"inondations": {**env.keywords, "periodes": {"france": [
        {"id": "p1", "start": D1, "end": D2},
        {"id": "p2", "start": D3, "end": D4},
    ]}}}))

    tk.run()

    assert env.read_days == [D1, D3]
    assert env.tables() == {env.destination_table}
    assert env.con.execute(
        f"SELECT period, {PARIS_DAY}, country, count(*) FROM {env.table} GROUP BY ALL ORDER BY 2"
    ).fetchall() == [("p1", D1, "france", 3), ("p2", D3, "france", 2)]
    assert env.con.execute(
        f"SELECT count(*) FROM {env.table} WHERE plaintext_normalized = 'l ete'"
    ).fetchone()[0] == 1


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
    # NULL plaintext on an adjacent chunk of day 2 → NOT NULL violation (plaintext_normalized) at INSERT time,
    # while the thematic chunk of that day is valid
    env.add_s3(pd.Timestamp(2025, 3, 11, 9, 59, tz=PARIS), None)
    env.add_day(D3)
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        tk.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 2


def test_run_new_raises_if_table_exists(env):
    """Verify that MODE=new refuses an existing destination table: nothing is read
    and the existing rows are untouched."""
    env.add_day(D1)
    env.configure(start=D1, end=D2)
    env.create_destination()
    env.insert_sentinel()

    with pytest.raises(ValueError, match="already exists"):
        tk.run()

    assert env.read_days == []
    assert env.con.execute(f"SELECT this_id FROM {env.table}").fetchall() == [("sentinel",)]


def test_run_reset_clears_existing_destination(env):
    """Verify that MODE=reset empties an existing destination table before ingesting."""
    env.add_day(D1)
    env.configure(start=D1, end=D2, mode="reset")
    env.create_destination()
    env.insert_sentinel("old_1")
    env.insert_sentinel("old_2")

    tk.run()

    assert env.count() == 2
    assert env.con.execute(
        f"SELECT count(*) FROM {env.table} WHERE this_id LIKE 'old_%'").fetchone()[0] == 0


def test_run_resume_restarts_from_last_ingested_day(env):
    """Verify that, after a run stopped on a failing day, MODE=resume on the same range restarts
    from the day of the last ingested chunk (not from START_DATE, not from the next day)
    and processes up to END_DATE."""
    for d in (D1, D2, D3, D4):
        env.add_day(d)
    env.failing_days = {D3}
    env.configure(start=D1, end=D5)
    with pytest.raises(RuntimeError):
        tk.run()
    assert env.days_in_table() == [D1, D2]

    env.failing_days.clear()
    env.read_days.clear()
    env.configure(start=D1, end=D5, mode="resume")
    tk.run()

    assert env.read_days == [D2, D3, D4]
    assert env.days_in_table() == [D1, D2, D3, D4]
    assert env.count() == 8


@pytest.mark.parametrize("name", ["factiva_articles",
                                  "inondations_keywords_fr_20250301_20250601"])  # previous format
def test_clear_refuses_non_thematic_table_and_keeps_rows(env, name):
    """Verify that clear_destination_table refuses a table whose name does not match the
    make_destination_table_name format and leaves its rows untouched."""
    env.con.execute(f"CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.{name} (id INTEGER)")
    env.con.execute(f"INSERT INTO {DB}.{SCHEMA}.{name} VALUES (1), (2)")
    before = env.con.execute(f"SELECT count(*) FROM {DB}.{SCHEMA}.{name}").fetchone()[0]

    with pytest.raises(ValueError, match="Refusing to clear"):
        utils.clear_destination_table(env.con, False, name, DB, SCHEMA)

    after = env.con.execute(f"SELECT count(*) FROM {DB}.{SCHEMA}.{name}").fetchone()[0]
    assert after == before


def test_run_dry_run_writes_nothing_and_counts_chunks(env, caplog):
    """Verify that in DRY_RUN mode nothing is written, even with MODE=reset (existing table
    neither cleared nor filled), and the number of chunks that would be ingested is logged."""
    env.add_day(D1)
    env.add_day(D2)
    env.configure(start=D1, end=D3, dry_run=True, mode="reset")
    env.create_destination()
    env.con.execute(
        f"INSERT INTO {env.table} (this_id, channel_name, channel_title, start, thematic_chunk, plaintext_normalized, country, period) "
        f"VALUES ('sentinel', 'tf1', 'TF1', TIMESTAMPTZ '2025-01-01 10:00:00+01', true, 'old', 'france', '{PERIOD}')"
    )

    with caplog.at_level(logging.INFO):
        tk.run()

    assert env.con.execute(f"SELECT this_id FROM {env.table}").fetchall() == [("sentinel",)]
    assert "DRY_RUN: 4 chunks would have been ingested" in caplog.text