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
TITLES = {"tf1": "TF1 (test)", "m6": "M6 (test)"}
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
            keywords_with_timestamp VARCHAR, country VARCHAR, channel_title VARCHAR
        )
    """)
    yield c
    c.close()

def _add_kw(con, id_, channel, start, plaintext, pairs, country="france"):
    """pairs = [(keyword, theme), ...]"""
    con.execute(
        f"INSERT INTO {DB}.{SCHEMA}.keywords VALUES (?, ?, CAST(? AS TIMESTAMP) AT TIME ZONE 'Europe/Paris', ?, ?, ?, ?)",
        [id_, channel, start, plaintext,
         json.dumps([{"keyword": k, "theme": t} for k, t in pairs]), country, TITLES.get(channel)],
    )

def _read(con, keywords):
    return kw_utils.read_barometre_keywords(
        con, keywords, D1, D2, "france", None, f"{SCHEMA}.keywords", DB
    )

def _kw_df(rows):
    """rows = [(id, channel, start, plaintext)]"""
    return pd.DataFrame([
        {"id": i, "channel_name": c, "channel_title": TITLES.get(c), "start": s, "plaintext": p,
         "keywords_found": "inondation", "themes": "changement_climatique_constat"}
        for i, c, s, p in rows
    ])

def _s3_df(rows):
    """rows = [(channel, start, plaintext)]"""
    return pd.DataFrame([{"channel_name": c, "start": s, "plaintext": p} for c, s, p in rows])

def _extract(kw, s3):
    out, _, _ = kw_utils.add_adjacent_plaintexts_day(kw, s3, tk.GAP)
    return out

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
        monkeypatch.setattr(tk, "USERS_TO_GRANT", [])  # in-memory DuckDB: no Postgres to grant on
        monkeypatch.setattr(tk.kw_utils, "_configure_s3", lambda *a, **k: None)
        monkeypatch.setattr(tk.kw_utils, "read_day_from_s3", self._fake_read_day_from_s3)
        self.configure(start=D1, end=D2)

    def configure(self, start=None, end=D2, dry_run=False, source_table=f"{SCHEMA}.keywords",
                  thematic="inondations", mode="new"):
        values = dict(
            THEMATIC=thematic, COUNTRY="france", COUNTRY_CONF=utils.COUNTRIES["france"],
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
            thematic, tk.MEDIA_TYPE, utils.COUNTRIES["france"]["country_short"])
        self.table = f"{DB}.{DESTINATION_SCHEMA}.{self.destination_table}"

    def add_s3(self, start, plaintext, channel="tf1"):
        self.s3_rows.append({"channel_name": channel, "start": start, "plaintext": plaintext})

    def add_day(self, day, channel="tf1"):
        """One thematic chunk at 10:00 + one S3 neighbour at 10:02 → 1 row ingested for that day,
        with the neighbour plaintext in plaintext_normalized_next."""
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

    def insert_sentinel(self, id_="sentinel"):
        self.con.execute(
            f"INSERT INTO {self.table} (id, channel_name, channel_title, start, plaintext_normalized, country, period) "
            f"VALUES ('{id_}', 'tf1', 'TF1', TIMESTAMPTZ '2025-01-01 10:00:00+01', 'old', 'france', '{PERIOD}')")


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


# ───────────────────────── add_adjacent_plaintexts_day ─────────────────────────


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


def test_add_adjacent_plaintexts_day_one_row_per_thematic_chunk_with_neighbours():
    """Verify that only the thematic chunk is kept as a row (S3 chunks are not rows any more),
    with the S3 chunk 2 min before (t3) as prev and 2 min after (t2) as next.
    t4 is 3 min away from core, t5 is on another channel: neither is used."""
    out = _extract(*_scenario())

    assert len(out) == 1
    row = out.iloc[0]
    assert row["id"] == "kw1"
    assert row["plaintext_normalized"] == "core"
    assert row["plaintext_normalized_prev"] == "t3"
    assert row["plaintext_normalized_next"] == "t2"


def test_add_adjacent_plaintexts_day_output_columns_and_timezone():
    """Verify all output columns (neither more nor less, regardless of order since insertion
    is done BY NAME), that id and channel title come from the keywords table, and that
    the same instants given in UTC give the same result."""
    kw, s3 = _scenario()
    out = _extract(kw, s3)

    assert set(out.columns) == {
        "id", "channel_name", "channel_title", "start", "keywords_found", "themes",
        "plaintext_normalized", "plaintext_normalized_prev", "plaintext_normalized_next",
    }
    assert len(out.columns) == 9
    assert list(out["channel_title"]) == ["TF1 (test)"]

    def to_utc(df):
        return df.assign(start=df["start"].dt.tz_convert("UTC"))
    pd.testing.assert_frame_equal(_extract(to_utc(kw), to_utc(s3)), out)


def test_add_adjacent_plaintexts_day_neighbour_choice():
    """Verify how the neighbours are chosen:
    - two S3 chunks on the same side within gap: the closest is kept (09:59, not 09:57:30)
    - a neighbour exactly at gap (2 min 30 s) is kept
    - a neighbour that is itself a thematic chunk is filled, with its S3 plaintext
    - no S3 chunk within gap on a side: None
    - plaintexts are normalized (accents, apostrophes, uppercase)."""
    kw = _kw_df([
        ("a", "tf1", T0, "core a"),
        ("b", "tf1", T0 + 2 * MIN, "core b"),
    ])
    s3 = _s3_df([
        ("tf1", T0 - timedelta(minutes=2, seconds=30), "far before a"),
        ("tf1", T0 - MIN, "close before a"),
        ("tf1", T0, "core a"),
        ("tf1", T0 + 2 * MIN, "core b"),
        ("tf1", T0 + timedelta(minutes=4, seconds=30), "L'Été after b"),
    ])
    out = _extract(kw, s3).set_index("id")

    assert out.loc["a", "plaintext_normalized_prev"] == "close before a"
    assert out.loc["a", "plaintext_normalized_next"] == "core b"
    assert out.loc["b", "plaintext_normalized_prev"] == "core a"
    assert out.loc["b", "plaintext_normalized_next"] == "l ete after b"

    out = _extract(_kw_df([("c", "tf1", T0, "core c")]), _s3_df([("tf1", T0, "core c")]))
    assert out.iloc[0]["plaintext_normalized_prev"] is None
    assert out.iloc[0]["plaintext_normalized_next"] is None


def test_add_adjacent_plaintexts_day_counts_core_missing_from_s3(caplog):
    """Verify that a thematic chunk (here 14h) that does not exist on the S3 side (old days: keywords
    filled before S3) is counted per channel in a warning and kept with the source table plaintext,
    without raising. Its neighbours are filled if S3 has them (here none)."""
    kw = _kw_df([
        ("kw1", "tf1", T0, "core 10h"),
        ("kw2", "tf1", T0 + timedelta(hours=4), "core 14h"),
        ("kw3", "m6", T0, "core m6"),
    ])
    s3 = _s3_df([("tf1", T0, "core 10h"), ("tf1", T0 + 2 * MIN, "n"), ("m6", T0, "core m6")])

    with caplog.at_level(logging.WARNING):
        out, n_mismatch, n_missing_s3 = kw_utils.add_adjacent_plaintexts_day(kw, s3, tk.GAP)

    assert n_missing_s3 == {"tf1": 1}
    assert n_mismatch == {}
    assert "1 thematic chunks missing from S3" in caplog.text
    out = out.set_index("id")
    assert set(out.index) == {"kw1", "kw2", "kw3"}
    assert out.loc["kw2", "plaintext_normalized"] == "core 14h"
    assert out.loc["kw2", "plaintext_normalized_prev"] is None
    assert out.loc["kw2", "plaintext_normalized_next"] is None
    assert out.loc["kw1", "plaintext_normalized_next"] == "n"


def test_add_adjacent_plaintexts_day_warns_if_core_and_s3_plaintext_differ(caplog):
    """Verify that when, for the same channel and start time, the thematic chunk plaintext differs
    from the S3 chunk (two Mediatree versions of the same chunk), the mismatch is counted per channel
    in a warning and the source table plaintext is kept, without raising."""
    kw = _kw_df([("kw1", "tf1", T0, "plaintext from source")])
    s3 = _s3_df([("tf1", T0, "plaintext from s3")])

    with caplog.at_level(logging.WARNING):
        out, n_mismatch, _ = kw_utils.add_adjacent_plaintexts_day(kw, s3, tk.GAP)

    assert "1 plaintext mismatch" in caplog.text
    assert n_mismatch == {"tf1": 1}
    assert list(out["plaintext_normalized"]) == ["plaintext from source"]


def test_add_adjacent_plaintexts_day_raises_if_nothing_found_in_s3():
    """Verify that the day fails when no thematic chunk and no neighbour is found in S3
    (missing channel or timezone shift)."""
    with pytest.raises(ValueError, match="nor any neighbour"):
        _extract(_kw_df([("kw1", "tf1", T0, "core")]), _s3_df([("m6", T0, "other channel")]))


# ───────────────────────── run() ─────────────────────────
def test_run_ingests_every_period_into_one_table(env):
    """Verify that two non-contiguous periods go into the same table, each row tagged with
    its period id and the country, that days outside the periods (D2, D4) are not read,
    that only thematic chunks are rows and that the neighbour plaintexts are ingested normalized."""
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
    ).fetchall() == [("p1", D1, "france", 1), ("p2", D3, "france", 1)]
    assert env.con.execute(
        f"SELECT id, plaintext_normalized, plaintext_normalized_prev, plaintext_normalized_next "
        f"FROM {env.table} ORDER BY start"
    ).fetchall() == [("kw_1", "core 1", "l ete", "neighbour 1"),
                     ("kw_3", "core 3", None, "neighbour 3")]


def test_run_counts_core_missing_from_s3_in_check(env, caplog):
    """Verify that a thematic chunk missing from S3 does not stop the run: it is ingested,
    the per channel count is logged by the check, and the check report has it in n_missing_s3.
    Also verify the report counts of thematic chunks without previous / next neighbour."""
    env.add_day(D1)
    _add_kw(env.con, "kw_missing", "tf1", "2025-03-10 14:00:00", "core missing",
            [("inondation", "changement_climatique_constat")])
    env.add_day(D1, channel="m6")
    env.configure(start=D1, end=D2)

    with caplog.at_level(logging.INFO):
        tk.run()

    assert env.count() == 3
    assert "1 thematic chunks of public.keywords missing from S3 during this run" in caplog.text
    assert "[CHECK] inondations_keywords_fr: 3 thematic chunks on 2 channels" in caplog.text
    report = kw_utils.check_keywords(env.con, DB, env.destination_table, DESTINATION_SCHEMA,
                                     False, f"{SCHEMA}.keywords", {"m6": 2}, {"tf1": 1})
    report = report.set_index("channel_name")
    assert report["n_missing_s3"].to_dict() == {"tf1": 1, "m6": 0}
    assert report["n_plaintext_mismatch"].to_dict() == {"tf1": 0, "m6": 2}
    # tf1: 10:00 has only a next neighbour, 14:00 has none; m6: 10:00 has only a next neighbour
    assert report["missing_prev"].to_dict() == {"tf1": 2, "m6": 1}
    assert report["missing_next"].to_dict() == {"tf1": 1, "m6": 0}
    assert report["pct_missing_adj"].to_dict() == {"tf1": 75.0, "m6": 50.0}


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
    assert env.count() == 1


def test_run_leaves_no_partial_day_when_insert_fails(env):
    """Verify that a failure during insertion leaves no rows for that day
    (even valid ones), previous day remains in table, and next day is not processed."""
    env.add_day(D1)
    env.add_day(D2)
    # second thematic chunk of day 2 with a NULL plaintext → NOT NULL violation (plaintext_normalized)
    # at INSERT time, while the first thematic chunk of that day is valid
    _add_kw(env.con, "kw_null", "tf1", "2025-03-11 11:00:00", None,
            [("inondation", "changement_climatique_constat")])
    env.add_day(D3)
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        tk.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 1


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
    assert env.con.execute(f"SELECT id FROM {env.table}").fetchall() == [("sentinel",)]


def test_run_reset_clears_existing_destination(env):
    """Verify that MODE=reset empties an existing destination table before ingesting."""
    env.add_day(D1)
    env.configure(start=D1, end=D2, mode="reset")
    env.create_destination()
    env.insert_sentinel("old_1")
    env.insert_sentinel("old_2")

    tk.run()

    assert env.count() == 1
    assert env.con.execute(
        f"SELECT count(*) FROM {env.table} WHERE id LIKE 'old_%'").fetchone()[0] == 0


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
    assert env.count() == 4


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
    env.insert_sentinel()

    with caplog.at_level(logging.INFO):
        tk.run()

    assert env.con.execute(f"SELECT id FROM {env.table}").fetchall() == [("sentinel",)]
    assert "DRY_RUN: 2 chunks would have been ingested" in caplog.text