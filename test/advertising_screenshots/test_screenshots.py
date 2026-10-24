"""Screenshots of the ads and occurrences (s06_screenshots): frame positions, ffmpeg mosaic, and
selection / saving on a PostgreSQL database, with the S3 buckets mocked."""

import asyncio
import shutil
import subprocess
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import (
    Ad,
    Ad_Occurrence,
    Ad_Screenshot,
    AdvertisingBase,
)
from quotaclimat.data_ingestion.advertising.s06_screenshots import run as screenshots
from quotaclimat.data_ingestion.advertising.s06_screenshots.frames import (
    build_mosaic,
    extract_frame,
    frame_offsets,
    frame_positions_in_parts,
)

T0 = datetime(2026, 4, 13, 20, 0, 0)
RUN_MODULE = "quotaclimat.data_ingestion.advertising.s06_screenshots.run"

needs_ffmpeg = pytest.mark.skipif(
    shutil.which("ffmpeg") is None, reason="ffmpeg is not installed"
)


def _image_size(path: str) -> tuple[int, int]:
    out = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0",
            path,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    width, height = out.split(",")
    return int(width), int(height)


def _test_video(path: str, duration_sec: int = 10) -> str:
    subprocess.run(
        [
            "ffmpeg",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=640x360:rate=25",
            "-t",
            str(duration_sec),
            path,
        ],
        check=True,
    )
    return path


def test_frame_offsets_same_positions_for_ad_and_occurrences():
    assert frame_offsets(12, nb_frames=6) == [1.0, 3.0, 5.0, 7.0, 9.0, 11.0]
    assert frame_offsets(3, nb_frames=2) == [0.75, 2.25]


def test_frame_positions_cross_2_minutes_archives():
    start = datetime(2026, 4, 13, 20, 1, 55)
    positions = frame_positions_in_parts(start, [1.0, 7.5])
    assert positions == [
        (datetime(2026, 4, 13, 20, 0), 116.0),
        (datetime(2026, 4, 13, 20, 2), 2.5),
    ]


@needs_ffmpeg
def test_extract_frame_and_mosaic(tmp_path):
    video = _test_video(str(tmp_path / "video.mp4"))

    async def _run():
        frames = []
        for i, offset in enumerate([1, 3, 5, 30]):
            path = str(tmp_path / f"frame_{i}.jpg")
            frames.append(path if await extract_frame(video, offset, path) else None)
        mosaic = str(tmp_path / "mosaic.jpg")
        await build_mosaic(frames, str(tmp_path / "tiles"), mosaic, columns=3)
        return frames, mosaic

    frames, mosaic = asyncio.run(_run())
    # 30s is past the end of the 10s video: no frame, a black tile in the mosaic
    assert frames[3] is None
    assert all(f is not None for f in frames[:3])
    # 3 columns x 2 rows of 320x180 frames, 4px padding and margins
    assert _image_size(mosaic) == (3 * 320 + 2 * 4 + 2 * 4, 2 * 180 + 4 + 2 * 4)


@pytest.fixture
def session():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _add_ad(session, ad_id, nb_occurrences, status="subcat_done", channel="tf1"):
    session.add(
        Ad(
            id=ad_id,
            first_detection_date=T0,
            duration_sec=12.0,
            chunks=[],
            fragment_type="AD",
            prediction_status=status,
        )
    )
    session.flush()
    for i in range(nb_occurrences):
        session.add(
            Ad_Occurrence(
                id=f"{ad_id}-occ{i}",
                occurrence_date=T0 + timedelta(minutes=10 * i),
                channel_name=channel,
                ad_id=ad_id,
            )
        )
    session.commit()


def test_queries_sample_occurrences_and_skip_done(session):
    _add_ad(session, "ad-a", 5)
    _add_ad(session, "ad-b", 2)
    _add_ad(session, "ad-unclassified", 2, status=None)

    filters = screenshots.Filters(max_occurrences_per_ad=3)
    ads = session.execute(screenshots.ads_query(filters)).all()
    assert sorted(a.id for a in ads) == ["ad-a", "ad-b"]

    occurrences = session.execute(screenshots.occurrences_query(filters)).all()
    by_ad = {}
    for o in occurrences:
        by_ad.setdefault(o.ad_id, []).append(o.id)
    assert {ad: len(ids) for ad, ids in by_ad.items()} == {"ad-a": 3, "ad-b": 2}

    screenshots.save_results(
        session,
        [
            screenshots.ScreenshotResult(
                target_type="occurrence",
                target_id=by_ad["ad-a"][0],
                ad_id="ad-a",
                status="done",
                source="mediatree_archive",
                image_url="https://example/img.jpg",
                frame_offsets_sec=[1.0, 3.0],
                nb_frames=2,
            ),
            screenshots.ScreenshotResult(
                target_type="occurrence",
                target_id=by_ad["ad-a"][1],
                ad_id="ad-a",
                status="missing_source",
                source="mediatree_archive",
                details={"missing_parts": ["2026-04-13T20:00:00+00:00"]},
            ),
        ],
    )
    # the sample stays the same: only the third sampled occurrence of ad-a is left
    remaining = session.execute(screenshots.occurrences_query(filters)).all()
    assert sorted(o.id for o in remaining if o.ad_id == "ad-a") == [by_ad["ad-a"][2]]

    retried = session.execute(
        screenshots.occurrences_query(
            screenshots.Filters(max_occurrences_per_ad=3, retry_missing=True)
        )
    ).all()
    assert sorted(o.id for o in retried if o.ad_id == "ad-a") == sorted(
        by_ad["ad-a"][1:]
    )

    every = screenshots.Filters(max_occurrences_per_ad=0, prediction_statuses=None)
    assert len(session.execute(screenshots.occurrences_query(every)).all()) == 7


@needs_ffmpeg
def test_occurrence_group_marks_missing_archives(session, tmp_path, monkeypatch):
    _add_ad(session, "ad-a", 2)
    occurrences = session.execute(
        screenshots.occurrences_query(screenshots.Filters())
    ).all()
    # occurrences at 20:00 and 20:10 UTC: only the archives of 20:00 are in the bucket
    available = {T0.replace(tzinfo=ZoneInfo("UTC"))}
    video = _test_video(str(tmp_path / "part.mp4"), duration_sec=20)
    monkeypatch.setattr(screenshots, "LOCAL_CACHE_DIR", str(tmp_path))

    async def _download(fs, channel, part_starts, local_dir, **kwargs):
        return {p: {"mp4": video} for p in part_starts}

    with (
        patch(
            f"{RUN_MODULE}.list_available_part_starts",
            AsyncMock(return_value=available),
        ),
        patch(f"{RUN_MODULE}.download_media_parts", side_effect=_download),
        patch(f"{RUN_MODULE}._upload_mosaic", AsyncMock(return_value="https://img")),
    ):
        results = asyncio.run(
            screenshots.screenshot_occurrence_group(
                None, "tf1", occurrences, asyncio.Semaphore(2)
            )
        )

    by_id = {r.target_id: r for r in results}
    assert by_id["ad-a-occ0"].status == "done"
    assert by_id["ad-a-occ0"].nb_frames == 6
    assert by_id["ad-a-occ0"].image_url == "https://img"
    assert by_id["ad-a-occ1"].status == "missing_source"
    assert by_id["ad-a-occ1"].details == {
        "missing_parts": ["2026-04-13T20:10:00+00:00"]
    }

    screenshots.save_results(session, results)
    saved = session.scalars(select(Ad_Screenshot)).all()
    assert {s.target_id: s.status for s in saved} == {
        "ad-a-occ0": "done",
        "ad-a-occ1": "missing_source",
    }


@needs_ffmpeg
def test_screenshot_ad_from_exported_video(tmp_path, monkeypatch):
    video = _test_video(str(tmp_path / "raw.mp4"), duration_sec=13)
    monkeypatch.setattr(screenshots, "LOCAL_CACHE_DIR", str(tmp_path))

    async def _get_file(s3_key, local_path):
        if "missing" in s3_key:
            raise FileNotFoundError(s3_key)
        shutil.copy(video, local_path)

    fs = SimpleNamespace(_get_file=_get_file)
    with patch(f"{RUN_MODULE}._upload_mosaic", AsyncMock(return_value="https://img")):
        done = asyncio.run(screenshots.screenshot_ad(fs, "ad-a", 12.0))
        missing = asyncio.run(screenshots.screenshot_ad(fs, "missing-ad", 12.0))

    assert done.status == "done"
    assert done.nb_frames == 6
    assert done.frame_offsets_sec == [1.0, 3.0, 5.0, 7.0, 9.0, 11.0]
    assert missing.status == "missing_source"
    assert missing.image_url is None
