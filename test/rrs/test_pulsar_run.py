"""Orchestration test for the Pulsar pipeline (run.run), all I/O mocked."""

import pytest

# The pulsar pipeline imports psycopg (DB) and playwright (download), which live in
# optional poetry groups the default CI test jobs don't install. Skip this module at
# collection time when they're absent so it doesn't break the shared `pytest test/`.
pytest.importorskip("psycopg")
pytest.importorskip("playwright")

import rrs.pulsar.run as run_mod  # noqa: E402
from rrs.pulsar.download_themes import DownloadResult  # noqa: E402
from rrs.pulsar.parse_themes import Theme, ThemesExport, Topic  # noqa: E402
from rrs.pulsar.settings import PulsarSettings  # noqa: E402


class _FakeConn:
    def commit(self):
        pass


class _FakeConnCtx:
    def __enter__(self):
        return _FakeConn()

    def __exit__(self, *a):
        return False


class _FakePostsClient:
    def __init__(self, *a, **k):
        pass

    def fetch_theme_posts(self, search_id, topics, date_from, date_to, limit=30):
        return [{"pulsarId": f"p-{topics[0]}"}]  # one post per theme


def test_run_stores_themes_and_fetches_posts(monkeypatch, tmp_path):
    settings = PulsarSettings(
        email="e", password="p", search_id="201830", folder_id="529",
        themes_xlsx=str(tmp_path / "t.xlsx"),
    )
    monkeypatch.setattr(PulsarSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(
        run_mod, "download_themes_xlsx",
        lambda **kw: DownloadResult(tmp_path / "t.xlsx", "Bearer XYZ", "team-1"),
    )
    export = ThemesExport(
        search_name="S", date_from=None, date_to=None,
        themes=[
            Theme(title="T1", topics=[Topic("nucléaire", 10)]),
            Theme(title="T2", topics=[]),  # no topics → no post fetch
        ],
    )
    monkeypatch.setattr(run_mod, "parse_themes_xlsx", lambda path: export)
    monkeypatch.setattr(run_mod, "PulsarPostsClient", _FakePostsClient)

    captured = {"searches": 0, "themes": [], "posts": 0}
    monkeypatch.setattr(run_mod.store, "connect", lambda: _FakeConnCtx())
    monkeypatch.setattr(
        run_mod.store, "upsert_search",
        lambda *a, **k: captured.__setitem__("searches", captured["searches"] + 1),
    )
    monkeypatch.setattr(
        run_mod.store, "insert_theme",
        lambda conn, sid, theme, df, dt: (captured["themes"].append(theme.title), f"id-{theme.title}")[1],
    )
    monkeypatch.setattr(
        run_mod.store, "upsert_posts",
        lambda conn, sid, tid, posts: (captured.__setitem__("posts", captured["posts"] + len(posts)), len(posts))[1],
    )

    run_mod.run()

    assert captured["searches"] == 1
    assert captured["themes"] == ["T1", "T2"]
    assert captured["posts"] == 1  # only T1 had topics → one post fetched & stored
