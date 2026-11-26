import asyncio
import os
import shutil
import subprocess
from datetime import datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Merge_Occurrence,
    Ad_Occurrence,
    AdvertisingBase,
)
from quotaclimat.data_ingestion.advertising.s02_exportation import merged_ads
from quotaclimat.data_ingestion.advertising.s02_exportation import run as export
from quotaclimat.data_ingestion.advertising.s02_exportation.merged_ads import (
    MergePlan,
    RebuildError,
    Segment,
    plan_merged_ad,
    render_merged_ad,
    render_steps,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.frames import (
    video_duration,
)

T0 = datetime(2026, 9, 1, 20, 0, 0)
# after 2026-04-27: the export margin of the source videos is 0.1 s
DETECTION = datetime(2026, 6, 1)
MARGIN = 0.1


@pytest.fixture
def session():
    engine = connect_to_db(use_custom_json_serializer=True)
    AdvertisingBase.metadata.drop_all(engine)
    AdvertisingBase.metadata.create_all(engine, checkfirst=True)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _ad(session, ad_id, duration_sec, deleted=False):
    session.add(
        Ad(
            id=ad_id,
            first_detection_date=DETECTION,
            duration_sec=duration_sec,
            chunks=[],
            fragment_type="AD",
            deleted_at=T0 if deleted else None,
        )
    )
    session.flush()


def _occurrence(session, occ_id, ad_id, date, deleted=False):
    session.add(
        Ad_Occurrence(
            id=occ_id,
            occurrence_date=date,
            channel_name="tf1",
            ad_id=ad_id,
            deleted_at=T0 if deleted else None,
        )
    )
    session.flush()


def _merge(session, merged_id, source_ids, duration_sec, status="active"):
    _ad(session, merged_id, duration_sec)
    session.add(
        Ad_Merge(
            merged_ad_id=merged_id,
            source_ad_ids=source_ids,
            method="manual",
            status=status,
        )
    )
    session.flush()


def _journal(session, merged_occ_id, merged_id, sources):
    """sources: [(source occurrence id, source ad id, role)]"""
    for position, (occ_id, ad_id, role) in enumerate(sources):
        session.add(
            Ad_Merge_Occurrence(
                merged_occurrence_id=merged_occ_id,
                source_occurrence_id=occ_id,
                merged_ad_id=merged_id,
                source_ad_id=ad_id,
                role=role,
                position=position,
            )
        )
    session.flush()


def test_render_steps_fills_gaps_and_cuts_overlaps():
    segments = [
        Segment("a", 0.0, 5.0, "piece"),
        Segment("b", 6.0, 4.0, "piece"),  # 1 s gap before it
        Segment(
            "c", 9.5, 3.0, "piece"
        ),  # overlaps b by 0.5 s, cut at the end of the ad
    ]
    steps = render_steps(segments, 11.0)
    assert [
        (
            s.segment.source_ad_id if s.segment else None,
            s.duration_sec,
            s.start_in_source_sec,
        )
        for s in steps
    ] == [
        ("a", 5.0, 0.0),
        (None, 1.0, 0.0),
        ("b", 4.0, 0.0),
        ("c", 1.0, 0.5),
    ]


def test_render_steps_pads_the_end():
    steps = render_steps([Segment("a", 0.0, 5.0, "piece")], 6.0)
    assert [(s.segment is None, s.duration_sec) for s in steps] == [
        (False, 5.0),
        (True, 1.0),
    ]


def test_plan_from_journal_of_most_recent_occurrence(session):
    _ad(session, "a", 5.0)
    _ad(session, "b", 4.0)
    _ad(session, "short", 1.0)
    _merge(session, "m", ["a", "b"], 10.0)
    # old broadcast: pieces contiguous
    _occurrence(session, "a-1", "a", T0, deleted=True)
    _occurrence(session, "b-1", "b", T0 + timedelta(seconds=5), deleted=True)
    _occurrence(session, "m-1", "m", T0)
    _journal(session, "m-1", "m", [("a-1", "a", "piece"), ("b-1", "b", "piece")])
    # most recent broadcast: a short segment absorbed between the pieces
    t1 = T0 + timedelta(days=1)
    _occurrence(session, "a-2", "a", t1, deleted=True)
    _occurrence(session, "s-2", "short", t1 + timedelta(seconds=5), deleted=True)
    _occurrence(session, "b-2", "b", t1 + timedelta(seconds=6), deleted=True)
    _occurrence(session, "m-2", "m", t1)
    _journal(
        session,
        "m-2",
        "m",
        [("a-2", "a", "piece"), ("s-2", "short", "absorbed"), ("b-2", "b", "piece")],
    )

    plan = plan_merged_ad(session, "m")
    assert plan.layout == "journal"
    assert plan.occurrence_id == "m-2"
    assert plan.duration_sec == 10.0
    assert [(s.source_ad_id, s.offset_sec, s.role) for s in plan.segments] == [
        ("a", 0.0, "piece"),
        ("short", 5.0, "absorbed"),
        ("b", 6.0, "piece"),
    ]


def test_plan_end_to_end_without_journal_and_recursive(session):
    _ad(session, "a", 5.0, deleted=True)
    _ad(session, "b", 4.0)
    _ad(session, "c", 3.0)
    _merge(session, "ab", ["a", "b"], 9.0)
    _merge(session, "abc", ["ab", "c"], 12.0)
    _occurrence(session, "abc-1", "abc", T0)

    plan = plan_merged_ad(session, "abc")
    assert plan.layout == "end_to_end"
    assert [(s.source_ad_id, s.offset_sec) for s in plan.segments] == [
        ("ab", 0.0),
        ("c", 9.0),
    ]
    sub_plan = plan.segments[0].sub_plan
    assert sub_plan is not None and sub_plan.ad_id == "ab"
    assert [(s.source_ad_id, s.offset_sec) for s in sub_plan.segments] == [
        ("a", 0.0),
        ("b", 5.0),
    ]
    assert plan.segments[1].sub_plan is None


def test_plan_loop_is_refused(session):
    _merge(session, "x", ["y", "c"], 5.0)
    _merge(session, "y", ["x", "c"], 5.0)
    _ad(session, "c", 2.0)
    plan = plan_merged_ad(session, "x")
    # y's own plan would go back to x: y gets no plan instead of looping
    assert plan.segments[0].sub_plan is not None
    assert plan.segments[0].sub_plan.segments[0].sub_plan is None


def test_plan_refuses_a_regular_ad(session):
    _ad(session, "a", 5.0)
    with pytest.raises(RebuildError):
        plan_merged_ad(session, "a")


def test_merged_ads_flag_splits_the_ads(session):
    _ad(session, "regular", 5.0)
    _ad(session, "a", 5.0)
    _ad(session, "b", 4.0)
    _merge(session, "active", ["a", "b"], 9.0)
    _merge(session, "reverted", ["b", "a"], 9.0, status="reverted")
    for ad_id in ("regular", "active", "reverted"):
        _occurrence(session, f"{ad_id}-1", ad_id, T0)
    session.commit()

    def _ids(merged_ads):
        return {
            ad.id
            for ad, _ in session.execute(
                export._base_ads_query(None, None, merged_ads)
            ).all()
        }

    assert _ids(False) == {"regular"}
    assert _ids(True) == {"active"}
    assert export.count_ads_since(session, None, None, True) == 1


# --- rendering, with ffmpeg ---------------------------------------------------------------------

needs_ffmpeg = pytest.mark.skipif(
    shutil.which("ffmpeg") is None, reason="ffmpeg is not installed"
)


def _make_video(path, duration_sec, color):
    subprocess.run(
        [
            "ffmpeg", "-y", "-v", "error",
            "-f", "lavfi", "-i", f"color=c={color}:s=320x240:r=25:d={duration_sec}",
            "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate=48000:duration={duration_sec}",
            "-c:v", "libx264", "-c:a", "aac", "-shortest", path,
        ],
        check=True,
    )  # fmt: skip


class FakeBucket:
    """The advertising bucket: ads/<ad_id>/raw.mp4 read from a local folder."""

    def __init__(self, folder):
        self.folder = folder
        self.downloads = []

    async def _get_file(self, key, local_path):
        ad_id = key.split("/")[-2]
        source = os.path.join(self.folder, f"{ad_id}.mp4")
        if not os.path.exists(source):
            raise FileNotFoundError(key)
        self.downloads.append(ad_id)
        shutil.copy(source, local_path)


@needs_ffmpeg
def test_render_merged_ad_recursively(tmp_path):
    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir()
    # source videos as the exportation saves them: the ad with 0.1 s before and after
    _make_video(str(bucket_dir / "a.mp4"), 2.0 + 2 * MARGIN, "red")
    _make_video(str(bucket_dir / "b.mp4"), 1.5 + 2 * MARGIN, "green")
    _make_video(str(bucket_dir / "c.mp4"), 1.0 + 2 * MARGIN, "blue")
    bucket = FakeBucket(str(bucket_dir))

    # "ab" has no video: rebuilt from a and b; "absorbed" has none either: black
    ab = MergePlan(
        "ab",
        3.5,
        "end_to_end",
        segments=[
            Segment("a", 0.0, 2.0, "piece", DETECTION),
            Segment("b", 2.0, 1.5, "piece", DETECTION),
        ],
    )
    plan = MergePlan(
        "abc",
        5.0,
        "journal",
        "abc-1",
        segments=[
            Segment("ab", 0.0, 3.5, "piece", DETECTION, sub_plan=ab),
            Segment("absorbed", 3.5, 0.5, "absorbed", DETECTION),
            Segment("c", 4.0, 1.0, "piece", DETECTION),
        ],
    )
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    output = str(tmp_path / "abc.mp4")
    rebuilt = asyncio.run(render_merged_ad(bucket, plan, output, str(work_dir), MARGIN))
    assert rebuilt == ["ab"]
    assert sorted(bucket.downloads) == ["a", "b", "c"]
    duration = asyncio.run(video_duration(output))
    assert duration == pytest.approx(5.0 + 2 * MARGIN, abs=0.1)

    mp3 = str(tmp_path / "abc.mp3")
    asyncio.run(merged_ads.mp4_to_mp3(output, mp3))
    assert os.path.getsize(mp3) > 0


@needs_ffmpeg
def test_render_merged_ad_fails_without_a_piece(tmp_path):
    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir()
    _make_video(str(bucket_dir / "a.mp4"), 2.0 + 2 * MARGIN, "red")
    plan = MergePlan(
        "ab",
        3.5,
        "end_to_end",
        segments=[
            Segment("a", 0.0, 2.0, "piece", DETECTION),
            Segment("b", 2.0, 1.5, "piece", DETECTION),
        ],
    )
    with pytest.raises(RebuildError):
        asyncio.run(
            render_merged_ad(
                FakeBucket(str(bucket_dir)),
                plan,
                str(tmp_path / "ab.mp4"),
                str(tmp_path),
                MARGIN,
            )
        )


class _Progress:
    def __init__(self):
        self.counts = {}

    def count(self, key):
        self.counts[key] = self.counts.get(key, 0) + 1

    def update(self, _):
        pass


def _run_group(monkeypatch, plan_merged, exported=None):
    async def _needs_export(ad, fs):
        return True, None

    async def _no_archives(*args, **kwargs):
        raise RuntimeError("archives gone")

    rebuilt = []

    async def _rebuild(ad, plan, local_dir, fs, media_sizes=None):
        rebuilt.append(ad.id)
        if media_sizes is not None:
            media_sizes["mp4"] = 123_456
        return None

    monkeypatch.setattr(export, "_check_in_bucket", _needs_export)
    monkeypatch.setattr(export, "download_media_parts", _no_archives)
    monkeypatch.setattr(export, "_rebuild_merged_ad", _rebuild)
    ad = Ad(id="m", duration_sec=5.0, fragment_type="no_data")
    occurrence = Ad_Occurrence(id="m-1", occurrence_date=T0, channel_name="tf1")
    progress, missing, reset = _Progress(), [], []
    asyncio.run(
        export._process_group(
            "tf1",
            T0.date(),
            [(ad, occurrence)],
            None,
            asyncio.Semaphore(2),
            progress,
            missing,
            reset,
            [],
            plan_merged,
            exported,
        )
    )
    return progress.counts, missing, reset, rebuilt


def test_process_group_rebuilds_merged_ads_without_archives(monkeypatch):
    counts, missing, reset, rebuilt = _run_group(monkeypatch, lambda ad_id: None)
    assert rebuilt == ["m"]
    assert counts == {"rebuilt": 1}
    assert missing == []
    assert reset == ["m"]


def test_process_group_without_flag_keeps_the_error(monkeypatch):
    counts, missing, reset, rebuilt = _run_group(monkeypatch, None)
    assert rebuilt == []
    assert counts == {"error": 1}
    assert missing == ["m"]


def test_process_group_records_the_rebuilt_export(monkeypatch):
    exported = {}
    _run_group(monkeypatch, lambda ad_id: None, exported)
    assert exported["m"]["method"] == "merged_rebuild"
    assert exported["m"]["mp4_bytes"] == 123_456
    assert exported["m"]["margin_sec"] == export.MARGIN_ON_MEDIA_EXPORT.total_seconds()


def test_process_group_without_flag_records_nothing(monkeypatch):
    exported = {}
    _run_group(monkeypatch, None, exported)
    assert exported == {}


def test_stuck_process_is_killed():
    # a process that never ends stands for a stuck ffmpeg
    async def _run():
        process = await asyncio.create_subprocess_exec(
            "sleep",
            "60",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await merged_ads._communicate(process, 0.5, "sleep")
        finally:
            assert process.returncode is not None  # killed, not left running

    with pytest.raises(RuntimeError, match="no answer after"):
        asyncio.run(_run())


def test_exit_reason_names_the_signal():
    assert merged_ads._exit_reason(1) == "exit code 1"
    assert "out of memory" in merged_ads._exit_reason(-9)
    assert merged_ads._exit_reason(-15) == "killed by signal 15"


def test_process_group_cancels_a_stuck_rebuild(monkeypatch):
    async def _needs_export(ad, fs):
        return True, None

    async def _no_archives(*args, **kwargs):
        raise RuntimeError("archives gone")

    async def _stuck_rebuild(ad, plan, local_dir, fs, media_sizes=None):
        await asyncio.sleep(60)

    monkeypatch.setattr(export, "_check_in_bucket", _needs_export)
    monkeypatch.setattr(export, "download_media_parts", _no_archives)
    monkeypatch.setattr(export, "_rebuild_merged_ad", _stuck_rebuild)
    monkeypatch.setattr(export, "REBUILD_TIMEOUT_SEC", 0.2)
    ad = Ad(id="m", duration_sec=5.0, fragment_type="advertising")
    occurrence = Ad_Occurrence(id="m-1", occurrence_date=T0, channel_name="tf1")
    progress, missing, exported = _Progress(), [], {}
    asyncio.run(
        export._process_group(
            "tf1",
            T0.date(),
            [(ad, occurrence)],
            None,
            asyncio.Semaphore(2),
            progress,
            missing,
            [],
            [],
            lambda ad_id: None,
            exported,
            None,
            asyncio.Semaphore(1),
        )
    )
    assert progress.counts == {"error": 1}
    assert missing == ["m"]
    assert exported == {}
