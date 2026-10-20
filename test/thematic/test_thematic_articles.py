import logging
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest
import yaml

import quotaclimat.data_processing.thematic.thematic_articles as ta
import quotaclimat.data_processing.thematic.thematic_articles_utils as art_utils
import quotaclimat.data_processing.thematic.thematic_utils as utils

DB = "barometre"
SCHEMA = "public"
D1, D2, D3, D4, D5 = (date(2025, 3, d) for d in (10, 11, 12, 13, 14))
SOURCE = f"{SCHEMA}.factiva_articles"
OUT_COLS = ["an", "source_code", "source_name", "publication_datetime",
            "title", "snippet", "body", "word_count"]
PARIS_DAY = "CAST(publication_datetime AT TIME ZONE 'Europe/Paris' AS DATE)"


# ───────────────────────── helpers & fixtures ─────────────────────────
@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone='UTC'") ## script does not depend on session TZ
    c.execute(f"ATTACH ':memory:' AS {DB}")
    c.execute(f"CREATE SCHEMA {DB}.{SCHEMA}")
    c.execute(f"""
        CREATE TABLE {DB}.{SCHEMA}.factiva_articles (
            an VARCHAR, source_code VARCHAR, source_name VARCHAR,
            publication_datetime TIMESTAMPTZ, title VARCHAR, snippet VARCHAR,
            body VARCHAR, word_count INTEGER, duplicate_status VARCHAR
        )
    """)
    yield c
    c.close()


def _add_article(con, an, ts, title="t", snippet="", body="", word_count=150,
                 dup="UNIQUE", code="LEFIG", name="Le Figaro"):
    """ts = heure murale de Paris, sans fuseau (ex. '2025-03-10 23:30:00')."""
    con.execute(
        f"INSERT INTO {DB}.{SCHEMA}.factiva_articles "
        "VALUES (?, ?, ?, CAST(? AS TIMESTAMP) AT TIME ZONE 'Europe/Paris', ?, ?, ?, ?, ?)",
        [an, code, name, ts, title, snippet, body, word_count, dup],
    )


def _read(con, keywords, start=D1, end=D2):
    return art_utils.read_barometre_factiva_articles(con, keywords, start, end, SOURCE, DB)


def _ans(df):
    return set(df["an"])


class RunEnv:
    """Everything run() needs, with no Postgres: reading of factiva is faked."""

    def __init__(self, con, monkeypatch, tmp_path):
        self.con, self.mp = con, monkeypatch
        self.articles, self.read_days, self.read_kw = [], [], []
        self.failing_days, self._n = set(), 0

        yml = tmp_path / "thematic.yml"
        yml.write_text(yaml.safe_dump({"inondations":{ "french": {"presse": ["inondation", "crue"],
                                                       "audiovisuel": ["autre"]}}}))
        monkeypatch.setattr(ta, "THEMATIC_YML", yml)
        monkeypatch.setattr(ta.duckdb, "connect", lambda *a, **k: con)
        monkeypatch.setattr(ta.utils, "configure_barometre_source", lambda *a, **k: None)
        monkeypatch.setattr(ta.art_utils, "read_barometre_factiva_articles", self._fake_read)
        self.configure(start=D1, end=D2)

    def configure(self, start=None, end=D2, dry_run=False, source_table=SOURCE,
                  thematic="inondations", language="french", mode="new"):
        values = dict(
            START_DATE=start, END_DATE=end, THEMATIC=thematic, COUNTRY="france", LANGUAGE=language,
            DRY_RUN=dry_run, MODE=mode,
            SOURCE_TABLE=source_table, POSTGRES_USER="u", POSTGRES_PASSWORD="p",
            POSTGRES_HOST="h", POSTGRES_PORT="5432", POSTGRES_DB=DB,
        )
        for k, v in values.items():
            self.mp.setattr(ta, k, v, raising=False)
        # the table name depends on the dates: same computation as run()
        if start is not None:
            self.destination_table = utils.make_destination_table_name(
                thematic, ta.MEDIA_TYPE, ta.COUNTRIES["france"], start, end)
            self.table = f"{DB}.{SCHEMA}.{self.destination_table}"

    def add_article(self, day, an=None, **overrides):
        self._n += 1
        row = dict(an=an or f"an_{self._n}", source_code="LEFIG", source_name="Le Figaro",
                   publication_datetime=pd.Timestamp(f"{day} 10:00", tz="Europe/Paris"),
                   title=f"title {self._n}", snippet="s",
                   body="b", word_count=150)
        row.update(overrides)
        self.articles.append(row)

    def add_day(self, day):
        """Two articles for that day."""
        self.add_article(day)
        self.add_article(day)

    def _fake_read(self, _con, kw_list, start, end, source_table, postgres_db):
        self.read_days.append(start)
        self.read_kw.append(kw_list)
        if start in self.failing_days:
            raise RuntimeError(f"read failed for {start}")
        rows = [r for r in self.articles if start <= r["publication_datetime"].date() < end]
        return pd.DataFrame(rows, columns=OUT_COLS)

    def create_destination(self):
        art_utils.create_thematic_articles_table(
            self.con, False, self.destination_table, DB, SCHEMA)

    def count(self):
        return self.con.execute(f"SELECT count(*) FROM {self.table}").fetchone()[0]
    
    def insert_sentinel(self, an="sentinel", day=date(2025, 1, 1)):
        self.con.execute(
            f"INSERT INTO {self.table} (an, source_code, source_name, publication_datetime, title) "
            f"VALUES ('{an}', 'SENT', 'Sentinel', TIMESTAMPTZ '{day} 10:00:00+01', 'old')")

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


# ───────────────────────── get_keywords ─────────────────────────
def _yml(tmp_path, content):
    p = tmp_path / "t.yml"
    p.write_text(yaml.safe_dump(content))
    return p


def test_get_keywords_returns_list_of_requested_media_type(tmp_path):
    """Verify that only the keywords of the requested media_type are returned."""
    p = _yml(tmp_path, {"inondations": {"french": {"presse": ["crue"], "audiovisuel": ["inondation"]}}})
    assert utils.get_keywords(p, "inondations", "presse", "french") == ["crue"]


def test_get_keywords_raises_keyerror_on_unknown_thematic_or_media_type(tmp_path):
    """Verify that an unknown thematic or media_type raises a KeyError (and not an
    AttributeError while building the error message)."""
    p = _yml(tmp_path, {"inondations": {"french":{"presse": ["crue"]}}})
    with pytest.raises(KeyError):
        utils.get_keywords(p, "inconnue", "presse", "french")
    with pytest.raises(KeyError):
        utils.get_keywords(p, "inondations", "audiovisuel", "french",)


# ───────────────────────── read_barometre_factiva_articles ─────────────────────────
def test_read_raises_on_empty_keywords(con):
    """Verify that an empty keyword list raises instead of matching everything."""
    with pytest.raises(ValueError):
        _read(con, [])


def test_read_output_columns_and_paris_date(con):
    """Verify the output columns (those of the destination table, without created_at)
    and that publication_datetime is a timestamptz, the instant is not shifted (23:30 Paris stays 23:30+01)."""
    _add_article(con, "a1", "2025-03-10 23:30:00", body="une inondation")

    out = _read(con, ["inondation"])

    assert set(out.columns) == set(OUT_COLS)
    assert out.iloc[0]["publication_datetime"] == pd.Timestamp("2025-03-10 23:30:00+01:00")


def test_read_matches_in_title_snippet_or_body(con):
    """Verify that a keyword is searched in title, snippet and body."""
    _add_article(con, "t", "2025-03-10 10:00:00", title="inondation")
    _add_article(con, "s", "2025-03-10 10:00:00", snippet="inondation")
    _add_article(con, "b", "2025-03-10 10:00:00", body="inondation")
    _add_article(con, "none", "2025-03-10 10:00:00", body="rien")

    assert _ans(_read(con, ["inondation"])) == {"t", "s", "b"}


def test_read_normalizes_accents_case_and_apostrophes(con):
    """Verify that accents, case and both apostrophes (' and ’) are normalized on the
    text side and the keyword side, so that all spellings match each other."""
    _add_article(con, "acc", "2025-03-10 10:00:00", snippet="Un simple départ de feu.")
    _add_article(con, "up", "2025-03-10 10:00:00", body="INCENDIE géant")
    _add_article(con, "ap1", "2025-03-10 10:00:00", body="le bombardier d'eau")
    _add_article(con, "ap2", "2025-03-10 10:00:00", body="le bombardier d’eau")

    out = _read(con, ["départ de feu", "incendie", "bombardier d'eau"])
    assert _ans(out) == {"acc", "up", "ap1", "ap2"}

    out = _read(con, ["bombardier deau"])
    assert _ans(out) == {"ap1", "ap2"}


def test_read_matches_whole_expressions_only(con):
    """Verify word boundaries: 'feu' must not match 'feutre', 'incendie' not 'incendiaire'."""
    _add_article(con, "x1", "2025-03-10 10:00:00", body="un feutre rouge")
    _add_article(con, "x2", "2025-03-10 10:00:00", body="un acte incendiaire")
    _add_article(con, "ok", "2025-03-10 10:00:00", body="un feu de camp")

    assert _ans(_read(con, ["feu", "incendie"])) == {"ok"}


def test_read_escapes_regex_special_characters(con):
    """Verify that keywords are literal: 'a.b' matches 'a.b' but not 'axb'."""
    _add_article(con, "lit", "2025-03-10 10:00:00", body="voir a.b ici")
    _add_article(con, "wild", "2025-03-10 10:00:00", body="voir axb ici")

    assert _ans(_read(con, ["a.b"])) == {"lit"}


def test_read_date_window_is_paris_midnight_start_inclusive_end_exclusive(con):
    """Verify that the window is [start 00:00, end 00:00) in Paris time."""
    _add_article(con, "before", "2025-03-09 23:59:59", body="inondation")
    _add_article(con, "first", "2025-03-10 00:00:00", body="inondation")
    _add_article(con, "last", "2025-03-10 23:59:59", body="inondation")
    _add_article(con, "after", "2025-03-11 00:00:00", body="inondation")

    assert _ans(_read(con, ["inondation"])) == {"first", "last"}


# ───────────────────────── destination helpers ─────────────────────────
def test_destination_table_exists_and_clear(env):
    """Verify destination_table_exists before/after creation, that clear_destination_table
    in dry run deletes nothing, and that a real clear empties the table but keeps it."""
    assert not utils.destination_table_exists(
        env.con, env.destination_table, DB, SCHEMA)

    env.create_destination()
    env.insert_sentinel("a")
    env.insert_sentinel("b")
    assert utils.destination_table_exists(env.con, env.destination_table, DB, SCHEMA)

    utils.clear_destination_table(env.con, True, env.destination_table, DB, SCHEMA)
    assert env.count() == 2

    utils.clear_destination_table(env.con, False, env.destination_table, DB, SCHEMA)
    assert env.count() == 0
    assert utils.destination_table_exists(env.con, env.destination_table, DB, SCHEMA)


def test_get_max_start_date_from_thematic_articles(env):
    """Verify that the max date is None on an empty table and the latest day otherwise."""
    env.create_destination()
    assert art_utils.get_max_start_date_from_thematic_articles(
        env.con, DB, env.destination_table) is None

    env.insert_sentinel("a", D1)
    env.insert_sentinel("b", D3)
    assert art_utils.get_max_start_date_from_thematic_articles(
        env.con, DB, env.destination_table) == D3


# ───────────────────────── check_articles ─────────────────────────
def test_check_articles_counts_articles_per_media(env):
    """Verify the number of articles per media (source_code, source_name), most covered first,
    on a table filled by a run."""
    env.add_day(D1)
    env.add_article(D1, source_code="LEMON", source_name="Le Monde")
    env.configure(start=D1, end=D2)
    ta.run()

    report = art_utils.check_articles(env.con, DB, env.destination_table, SCHEMA)

    assert report.to_dict("records") == [
        {"source_code": "LEFIG", "source_name": "Le Figaro", "n_articles": 2},
        {"source_code": "LEMON", "source_name": "Le Monde", "n_articles": 1},
    ]


# ───────────────────────── run() ─────────────────────────
def test_run_new_run_creates_table_and_uses_presse_keywords(env):
    """Verify that a new run creates the destination table, ingests the day, and reads
    with the keywords of the 'presse' category only."""
    env.add_day(D1)
    env.configure(start=D1, end=D2)

    ta.run()

    assert env.destination_table in env.tables()
    assert env.count() == 2
    assert env.read_kw == [["inondation", "crue"]]


def test_run_new_raises_if_table_exists(env):
    """Verify that MODE=new refuses an existing destination table: nothing is read
    and the existing rows are untouched."""
    env.add_day(D1)
    env.configure(start=D1, end=D2)
    env.create_destination()
    env.insert_sentinel()

    with pytest.raises(ValueError, match="already exists"):
        ta.run()

    assert env.read_days == []
    assert env.con.execute(f"SELECT an FROM {env.table}").fetchall() == [("sentinel",)]


def test_run_reset_clears_existing_destination(env):
    """Verify that MODE=reset empties an existing destination table before ingesting."""
    env.add_day(D1)
    env.configure(start=D1, end=D2, mode="reset")
    env.create_destination()
    env.insert_sentinel("old_1")
    env.insert_sentinel("old_2")

    ta.run()

    assert env.count() == 2
    assert env.con.execute(
        f"SELECT count(*) FROM {env.table} WHERE an LIKE 'old_%'").fetchone()[0] == 0


def test_run_stops_at_first_failing_day(env):
    """Verify that run stops at the first failing day: day 1 is in table,
    day 3 is never read, and a RuntimeError is raised."""
    for d in (D1, D2, D3):
        env.add_day(d)
    env.failing_days = {D2}
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        ta.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 2


def test_run_stops_on_duplicated_an_and_ingests_nothing_for_that_day(env):
    """Verify that duplicated 'an' in the articles read stops the run before any insertion
    for that day, while the previous day stays in the table."""
    env.add_day(D1)
    env.add_article(D2, an="same")
    env.add_article(D2, an="same")
    env.add_day(D3)
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        ta.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 2


def test_run_leaves_no_partial_day_when_insert_fails(env):
    """Verify that a failure during insertion leaves no rows for that day (even valid
    ones), the previous day remains, and the next day is not processed."""
    env.add_day(D1)
    env.add_article(D2)
    # not an integer: fails at INSERT time, while the other article of that day is valid
    env.add_article(D2, word_count="not a number")
    env.add_day(D3)
    env.configure(start=D1, end=D4)

    with pytest.raises(RuntimeError):
        ta.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1]
    assert env.count() == 2


def test_run_twice_does_not_duplicate_or_overwrite_rows(env):
    """Verify idempotency: replaying the last day through a resume creates no duplicates
    and, with ON CONFLICT DO NOTHING, does not modify existing rows."""
    for d in (D1, D2, D3):
        env.add_day(d)
    env.configure(start=D1, end=D4)
    ta.run()
    assert env.count() == 6

    env.con.execute(
        f"UPDATE {env.table} SET title = 'SENTINEL' "
        f"WHERE {PARIS_DAY} = DATE '{D3}' AND an = "
        f"(SELECT min(an) FROM {env.table} WHERE {PARIS_DAY} = DATE '{D3}')"
    )
    # replay through a resume on the same range (MODE=new would refuse the existing table,
    # MODE=reset would clear it first)
    env.configure(start=D1, end=D4, mode="resume")
    ta.run()

    assert env.count() == 6
    assert env.con.execute(f"SELECT count(DISTINCT an) FROM {env.table}").fetchone()[0] == 6
    assert env.con.execute(
        f"SELECT count(*) FROM {env.table} WHERE title = 'SENTINEL'").fetchone()[0] == 1


def test_run_resume_restarts_from_last_ingested_day(env):
    """Verify that, after a run stopped on a failing day, MODE=resume on the same range restarts
    from the day of the last ingested article (not from START_DATE, not from the next day)
    and processes up to END_DATE."""
    for d in (D1, D2, D3, D4):
        env.add_day(d)
    env.failing_days = {D3}
    env.configure(start=D1, end=D5)
    with pytest.raises(RuntimeError):
        ta.run()
    assert env.days_in_table() == [D1, D2]

    env.failing_days.clear()
    env.read_days.clear()
    env.configure(start=D1, end=D5, mode="resume")
    ta.run()

    assert env.read_days == [D2, D3, D4]
    assert env.days_in_table() == [D1, D2, D3, D4]
    assert env.count() == 8


def test_run_resume_does_not_create_a_new_table(env):
    """Verify that resuming writes into the existing table: no other table is created."""
    for d in (D1, D2, D3):
        env.add_day(d)
    env.failing_days = {D2}
    env.configure(start=D1, end=D4)
    with pytest.raises(RuntimeError):
        ta.run()
    tables_before = env.tables()
    assert tables_before == {"factiva_articles", env.destination_table}

    env.failing_days.clear()
    env.configure(start=D1, end=D4, mode="resume")
    ta.run()

    assert env.tables() == tables_before
    assert env.days_in_table() == [D1, D2, D3]


def test_run_resume_raises_if_table_missing(env):
    """Verify that MODE=resume without destination table raises instead of creating one
    (most likely a typo in START_DATE / END_DATE): nothing is read, nothing is created."""
    env.add_day(D1)
    env.configure(start=D1, end=D3, mode="resume")

    with pytest.raises(ValueError, match="nothing to resume"):
        ta.run()

    assert env.read_days == []
    assert env.destination_table not in env.tables()


def test_run_resume_on_empty_table_starts_from_start_date(env):
    """Verify that MODE=resume on an existing but empty destination table processes
    the whole range from START_DATE."""
    env.add_day(D1)
    env.add_day(D2)
    env.configure(start=D1, end=D3, mode="resume")
    env.create_destination()

    ta.run()

    assert env.read_days == [D1, D2]
    assert env.days_in_table() == [D1, D2]

def test_run_returns_without_reading_when_start_not_before_end(env):
    """Verify that run returns quietly when there is nothing to process."""
    env.add_day(D1)
    env.configure(start=D2, end=D2)

    ta.run()

    assert env.read_days == []


@pytest.mark.parametrize("name", ["autre_table", "inondations_articles", "thematic_articles"])
def test_clear_refuses_non_thematic_table_and_keeps_rows(env, name):
    """Verify that clear_destination_table refuses a table whose name does not match the
    make_destination_table_name format and leaves its rows untouched."""
    env.con.execute(f"CREATE TABLE {DB}.{SCHEMA}.{name} (id INTEGER)")
    env.con.execute(f"INSERT INTO {DB}.{SCHEMA}.{name} VALUES (1), (2)")

    with pytest.raises(ValueError, match="Refusing to clear"):
        utils.clear_destination_table(env.con, False, name, DB, SCHEMA)

    n = env.con.execute(f"SELECT count(*) FROM {DB}.{SCHEMA}.{name}").fetchone()[0]
    assert n == 2


def test_run_dry_run_writes_nothing_and_counts_articles(env, caplog):
    """Verify that in DRY_RUN mode nothing is written, even with MODE=reset (existing table
    neither cleared nor filled), and the number of articles that would be ingested is logged."""
    env.add_day(D1)
    env.add_day(D2)
    env.configure(start=D1, end=D3, dry_run=True, mode="reset")
    env.create_destination()
    env.insert_sentinel()

    with caplog.at_level(logging.INFO):
        ta.run()

    assert env.con.execute(f"SELECT an FROM {env.table}").fetchall() == [("sentinel",)]
    assert "DRY_RUN: 4 articles would have been ingested" in caplog.text