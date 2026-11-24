"""advertising.ad.media_export: the exportation (s02) reads in the database which ads are left to export, and
records the export of each ad (exported or found in the bucket)."""

import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad, Ad_Occurrence, AdvertisingBase
from quotaclimat.data_ingestion.advertising.s02_exportation import run as export

T0 = datetime(2026, 9, 1, 20, 0, 0)


@pytest.fixture
def session():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _ad(session, ad_id, media_export=None, fragment_type="AD", duration_sec=10.0):
    session.add(
        Ad(
            id=ad_id,
            first_detection_date=T0,
            duration_sec=duration_sec,
            chunks=[],
            fragment_type=fragment_type,
            media_export=media_export,
        )
    )
    session.add(
        Ad_Occurrence(
            id=f"{ad_id}-1", occurrence_date=T0, channel_name="tf1", ad_id=ad_id
        )
    )
    session.flush()


def test_only_the_ads_left_to_export_are_taken(session):
    enough = int(export._min_mp4_bytes(10.0)) + 1
    _ad(session, "unknown")
    _ad(session, "exported", export.media_export_info(enough, 0.1))
    _ad(session, "too_small", export.media_export_info(1000, 0.1))
    _ad(session, "size_unknown", export.media_export_info(None, 0.1))
    session.commit()

    ids = {ad.id for ad, _ in session.execute(export._base_ads_query(None, None))}
    assert ids == {"unknown", "too_small"}
    assert export.count_ads_since(session, None, None) == 2


def test_media_export_none_is_sql_null(session):
    _ad(session, "a", export.media_export_info(1, 0.1))
    session.commit()
    ad = session.get(Ad, "a")
    ad.media_export = None
    session.commit()
    ids = {ad.id for ad, _ in session.execute(export._base_ads_query(None, None))}
    assert ids == {"a"}


def test_save_exports_writes_the_ads_and_resets_no_data(session):
    _ad(session, "a")
    _ad(session, "b", fragment_type="no_data")
    session.commit()
    info = export.media_export_info(5_000_000, 0.1, method="mediatree_cut")

    export._save_exports(session, {"a": info, "b": info}, ["b"])

    session.expire_all()
    assert session.get(Ad, "a").media_export == info
    assert session.get(Ad, "b").fragment_type == "advertising"
    assert export.count_ads_since(session, None, None) == 0


class FakeBucket:
    def __init__(self, files: dict, folders=()):
        self.files = files  # {ad_id: (size, LastModified)}
        self.folders = set(folders)
        self.requests = 0

    async def _info(self, key):
        self.requests += 1
        ad_id = key.split("/")[-2]
        if ad_id not in self.files:
            raise FileNotFoundError(key)
        size, modified = self.files[ad_id]
        return {"size": size, "LastModified": modified}

    async def _exists(self, path):
        return path.split("/")[-1] in self.folders


def _check(ad, fs):
    return asyncio.run(export._check_in_bucket(ad, fs))


def test_check_in_bucket():
    old = datetime(2026, 1, 5, tzinfo=timezone.utc)  # margin 1 s then
    recent = datetime(2026, 11, 1, tzinfo=timezone.utc)  # margin 0.1 s
    fs = FakeBucket(
        {
            "old": (5_000_000, old),
            "recent": (5_000_000, recent),
            "small": (1000, recent),
        },
        folders={"mp3_only"},
    )

    def ad(ad_id, media_export=None):
        return SimpleNamespace(id=ad_id, duration_sec=10.0, media_export=media_export)

    needs, found = _check(ad("old"), fs)
    assert not needs
    assert found == {
        "exported_at": old.isoformat(),
        "mp4_bytes": 5_000_000,
        "margin_sec": 1.0,
    }
    assert _check(ad("recent"), fs)[1]["margin_sec"] == 0.1
    assert _check(ad("small"), fs) == (True, None)
    assert _check(ad("missing"), fs) == (True, None)
    # a folder without raw.mp4: not exported again, as before
    assert _check(ad("mp3_only"), fs) == (False, None)

    # known as exported with a too small mp4: exported again without a request
    requests = fs.requests
    assert _check(ad("x", {"mp4_bytes": 10}), fs) == (True, None)
    assert fs.requests == requests


class _Progress:
    def __init__(self):
        self.counts = {}

    def count(self, key):
        self.counts[key] = self.counts.get(key, 0) + 1

    def update(self, _):
        pass


def test_process_group_records_cut_and_found_ads(monkeypatch):
    found = export.media_export_info(5_000_000, 0.1)

    async def _check_in_bucket(ad, fs):
        return (False, found) if ad.id == "cached" else (True, None)

    async def _download(*args, **kwargs):
        return {}

    async def _export_ad(ad, *args, media_sizes=None):
        media_sizes["mp4"] = 42
        return None

    monkeypatch.setattr(export, "_check_in_bucket", _check_in_bucket)
    monkeypatch.setattr(export, "download_media_parts", _download)
    monkeypatch.setattr(export, "_export_ad", _export_ad)
    monkeypatch.setattr(export, "cleanup_day_media_parts", lambda _: None)

    def ad(ad_id):
        return (
            Ad(id=ad_id, duration_sec=5.0, fragment_type="AD"),
            Ad_Occurrence(id=f"{ad_id}-1", occurrence_date=T0, channel_name="tf1"),
        )

    exported, progress = {}, _Progress()
    asyncio.run(
        export._process_group(
            "tf1",
            date(2026, 9, 1),
            [ad("cached"), ad("cut")],
            None,
            asyncio.Semaphore(2),
            progress,
            [],
            [],
            [],
            None,
            exported,
        )
    )

    assert progress.counts == {"cached": 1, "uploaded": 1}
    assert exported["cached"] == found
    assert exported["cut"]["method"] == "mediatree_cut"
    assert exported["cut"]["mp4_bytes"] == 42


def test_margin_sec_at():
    assert export.margin_sec_at(datetime(2026, 1, 1)) == 1.0
    assert export.margin_sec_at(datetime(2026, 5, 1, tzinfo=timezone.utc)) == 0.1
    assert export.margin_sec_at(datetime(2024, 1, 1)) is None
    assert export.margin_sec_at(datetime(2026, 4, 27) + timedelta(hours=1)) == 0.1
