"""Screenshots of the ads and occurrences (s05_enrichment/e02_screenshots): frame positions, ffmpeg mosaic, and
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
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots import (
    run as screenshots,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.frames import (
    build_mosaic,
    extract_frame,
    frame_offsets,
    frame_positions_in_parts,
)

T0 = datetime(2026, 4, 13, 20, 0, 0)
RUN_MODULE = "quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.run"

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


def test_queries_take_every_occurrence_and_skip_done(session):
    _add_ad(session, "ad-a", 5)
    _add_ad(session, "ad-b", 2)
    _add_ad(session, "ad-unclassified", 2, status=None)

    filters = screenshots.Filters()
    ads = session.execute(screenshots.ads_query(filters)).all()
    assert sorted(a.id for a in ads) == ["ad-a", "ad-b"]

    occurrences = session.execute(screenshots.occurrences_query(filters)).all()
    assert len(occurrences) == 7

    screenshots.save_results(
        session,
        [
            screenshots.ScreenshotResult(
                target_type="occurrence",
                target_id="ad-a-occ0",
                ad_id="ad-a",
                status="done",
                source="mediatree_archive",
                image_url="https://example/img.jpg",
                frame_offsets_sec=[1.0, 3.0],
                nb_frames=2,
            ),
            screenshots.ScreenshotResult(
                target_type="occurrence",
                target_id="ad-a-occ1",
                ad_id="ad-a",
                status="missing_source",
                source="mediatree_archive",
                details={"missing_parts": ["2026-04-13T20:00:00+00:00"]},
            ),
        ],
    )
    remaining = session.execute(screenshots.occurrences_query(filters)).all()
    assert sorted(o.id for o in remaining if o.ad_id == "ad-a") == [
        "ad-a-occ2",
        "ad-a-occ3",
        "ad-a-occ4",
    ]

    retried = session.execute(
        screenshots.occurrences_query(screenshots.Filters(retry_missing=True))
    ).all()
    assert "ad-a-occ1" in {o.id for o in retried}

    every = screenshots.Filters(prediction_statuses=None)
    assert len(session.execute(screenshots.occurrences_query(every)).all()) == 7


def test_occurrences_streamed_by_channel_day(session):
    _add_ad(session, "ad-a", 2, channel="tf1")
    _add_ad(session, "ad-b", 2, channel="france2")
    session.add(
        Ad_Occurrence(
            id="ad-a-next-day",
            occurrence_date=T0 + timedelta(days=1),
            channel_name="tf1",
            ad_id="ad-a",
        )
    )
    session.commit()

    groups = list(
        screenshots.iter_occurrences_by_channel_day(
            session, screenshots.occurrences_query(screenshots.Filters()), page_size=2
        )
    )
    assert [(key, [o.id for o in group]) for key, group in groups] == [
        (("france2", T0.date()), ["ad-b-occ0", "ad-b-occ1"]),
        (("tf1", T0.date()), ["ad-a-occ0", "ad-a-occ1"]),
        (("tf1", T0.date() + timedelta(days=1)), ["ad-a-next-day"]),
    ]


def test_ads_and_occurrence_batches_most_broadcast_first(session):
    _add_ad(session, "ad-a", 2)
    _add_ad(session, "ad-b", 5)
    _add_ad(session, "ad-c", 0)
    _add_ad(session, "ad-unclassified", 9, status=None)

    filters = screenshots.Filters()
    assert [a.id for a in session.execute(screenshots.ads_query(filters))] == [
        "ad-b",
        "ad-a",
        "ad-c",
    ]
    assert session.execute(
        screenshots.ranked_ad_ids_query(filters)
    ).scalars().all() == [
        "ad-b",
        "ad-a",
    ]

    # from T0 + 25 min: occurrences 3 and 4 of ad-b only (every 10 minutes from T0)
    recent = screenshots.Filters(
        occurrences_from=(T0 + timedelta(minutes=25)).replace(tzinfo=ZoneInfo("UTC"))
    )
    assert [a.id for a in session.execute(screenshots.ads_query(recent))] == ["ad-b"]
    assert sorted(
        o.id for o in session.execute(screenshots.occurrences_query(recent))
    ) == ["ad-b-occ3", "ad-b-occ4"]


def test_run_occurrences_by_batches_of_ads(session, monkeypatch):
    _add_ad(session, "ad-a", 2)
    _add_ad(session, "ad-b", 3, channel="m6")
    _add_ad(session, "ad-c", 4)
    monkeypatch.setattr(screenshots, "ADS_PER_OCCURRENCE_BATCH", 1)

    processed = []

    async def _group(fs, channel, occurrences, semaphore):
        processed.append([o.id for o in occurrences])
        return [
            screenshots.ScreenshotResult(
                target_type="occurrence",
                target_id=o.id,
                ad_id=o.ad_id,
                status="done",
                source="mediatree_archive",
            )
            for o in occurrences
        ]

    with patch(f"{RUN_MODULE}.screenshot_occurrence_group", side_effect=_group):
        asyncio.run(
            screenshots.run_occurrences(
                session, session, None, screenshots.Filters(limit=8)
            )
        )

    # one batch per ad, the most broadcast first; the limit stops in the last batch
    assert processed == [
        ["ad-c-occ0", "ad-c-occ1", "ad-c-occ2", "ad-c-occ3"],
        ["ad-b-occ0", "ad-b-occ1", "ad-b-occ2"],
        ["ad-a-occ0"],
    ]
    assert len(session.scalars(select(Ad_Screenshot)).all()) == 8


def test_export_margin_from_video_duration_or_detection_date():
    # 1s margins before 2026-04-27, 0.1s after
    assert screenshots.export_margin_sec(12.0, datetime(2026, 1, 1), 14.02) == 1.0
    assert screenshots.export_margin_sec(12.0, datetime(2026, 1, 1), 12.21) == 0.1
    assert screenshots.export_margin_sec(12.0, datetime(2026, 1, 1), None) == 1.0
    assert screenshots.export_margin_sec(12.0, datetime(2026, 6, 1), None) == 0.1


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
    # 12s ad exported with the 1s margins
    video = _test_video(str(tmp_path / "raw.mp4"), duration_sec=14)
    monkeypatch.setattr(screenshots, "LOCAL_CACHE_DIR", str(tmp_path))

    async def _get_file(s3_key, local_path):
        if "missing" in s3_key:
            raise FileNotFoundError(s3_key)
        shutil.copy(video, local_path)

    fs = SimpleNamespace(_get_file=_get_file)
    with patch(f"{RUN_MODULE}._upload_mosaic", AsyncMock(return_value="https://img")):
        done = asyncio.run(screenshots.screenshot_ad(fs, "ad-a", 12.0, T0))
        missing = asyncio.run(screenshots.screenshot_ad(fs, "missing-ad", 12.0, T0))

    assert done.status == "done"
    assert done.nb_frames == 6
    assert done.frame_offsets_sec == [1.0, 3.0, 5.0, 7.0, 9.0, 11.0]
    assert done.details == {"margin_sec": 1.0}
    assert missing.status == "missing_source"
    assert missing.image_url is None
