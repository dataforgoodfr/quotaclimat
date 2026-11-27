"""Video of a merged ad (advertising.ad_merge, see s04_consolidation) rebuilt from the videos of its
source ads, when the mediatree archives of its occurrences are no longer in the bucket.

A merged ad is a new ad: nothing is in the advertising bucket for it until the exportation cuts its
video from the mediatree archives of one of its occurrences. When these archives are gone, its video
is rebuilt from the videos of the ads it was merged from (ads/<source_ad_id>/raw.mp4), placed where
they were broadcast in one occurrence of the merged ad (advertising.ad_merge_occurrence journal):

- the pieces and absorbed segments of that occurrence give the offset of each source ad in the
  merged ad; time covered by no source ad (gaps between pieces, an absorbed segment without video)
  is black and silent;
- without journal rows (deleted by a reprocessing of the detection), the pieces are put end to end;
- a source ad without video that is itself a merged ad is rebuilt the same way, recursively;
- a piece without video that cannot be rebuilt makes the whole rebuild fail: no video is better than
  a video missing a piece.

The planning (database) and the rendering (ffmpeg) are separate: plan_merged_ad reads the database
synchronously, render_merged_ad only downloads and runs ffmpeg.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field

import s3fs
from sqlalchemy import select
from sqlalchemy.orm import Session

from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Merge_Occurrence,
    Ad_Occurrence,
)
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.frames import (
    video_duration,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.run import (
    export_margin_sec,
)
from quotaclimat.data_ingestion.advertising.tools.ad_merge import (
    ROLE_ABSORBED,
    ROLE_PIECE,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    VIDEO_ENCODER_ARGS,
)

logger = logging.getLogger(__name__)

# A merge of merges of merges... deeper than this is a loop in the journal, not a real merge.
MAX_MERGE_DEPTH = 10

# Format of the rebuilt video when the sources can't tell it (no readable video stream).
DEFAULT_WIDTH, DEFAULT_HEIGHT, DEFAULT_FPS = 1280, 720, "25"

# An ffmpeg/ffprobe of a rebuild handles a few seconds of video: one still running after this time is
# stuck (seen when the job ran out of memory: the run froze for good). It is killed and the rebuild
# fails, so the run goes on with the next ads.
FFMPEG_TIMEOUT_SEC = int(os.environ.get("REBUILD_FFMPEG_TIMEOUT_SEC") or 300)


async def _communicate(process, timeout_sec: float, what: str) -> tuple[bytes, bytes]:
    """process.communicate() with a timeout. The process is killed on timeout, and when the calling
    task is cancelled (timeout of the whole rebuild in run.py), so that no ffmpeg is left running."""
    try:
        return await asyncio.wait_for(process.communicate(), timeout_sec)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError(f"{what}: no answer after {timeout_sec:.0f}s, killed")
    except asyncio.CancelledError:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise


def _exit_reason(returncode: int) -> str:
    """Exit code of ffmpeg, or the signal that killed it: SIGKILL with no error message is most often
    the kernel freeing memory (the disk of a Scaleway job counts in its memory)."""
    if returncode < 0:
        reason = f"killed by signal {-returncode}"
        if -returncode == 9:
            reason += " (SIGKILL, likely out of memory)"
        return reason
    return f"exit code {returncode}"


AUDIO_SAMPLE_RATE = 48000


class RebuildError(RuntimeError):
    """The video of a merged ad can't be rebuilt from its sources."""


@dataclass
class Segment:
    """A source ad placed in a merged ad: from offset_sec, for duration_sec."""

    source_ad_id: str
    offset_sec: float
    duration_sec: float
    role: str
    first_detection_date: object = None
    # the plan of the source ad when it is itself a merged ad (used when it has no video)
    sub_plan: "MergePlan | None" = None


@dataclass
class MergePlan:
    ad_id: str
    duration_sec: float
    # "journal": offsets of one broadcast (ad_merge_occurrence), "end_to_end": pieces one after the other
    layout: str
    occurrence_id: str | None = None
    segments: list[Segment] = field(default_factory=list)


@dataclass
class Step:
    """One step of the rendered video: a part of a source video, or black and silence (segment None)."""

    duration_sec: float
    segment: Segment | None = None
    start_in_source_sec: float = 0.0


def _layout_from_journal(session: Session, merged_ad_id: str):
    """(occurrence id, [(source ad id, offset, role)]) of the most recent occurrence of the merged ad
    that still has its journal rows, None when there is none."""
    merged_occurrence = session.execute(
        select(Ad_Occurrence.id, Ad_Occurrence.occurrence_date)
        .join(
            Ad_Merge_Occurrence,
            Ad_Merge_Occurrence.merged_occurrence_id == Ad_Occurrence.id,
        )
        .where(Ad_Occurrence.ad_id == merged_ad_id)
        .where(Ad_Merge_Occurrence.reverted_at.is_(None))
        .order_by(Ad_Occurrence.occurrence_date.desc(), Ad_Occurrence.id)
        .limit(1)
    ).first()
    if merged_occurrence is None:
        return None
    rows = session.execute(
        select(
            Ad_Merge_Occurrence.source_ad_id,
            Ad_Merge_Occurrence.role,
            Ad_Occurrence.occurrence_date,
        )
        .join(
            Ad_Occurrence,
            Ad_Occurrence.id == Ad_Merge_Occurrence.source_occurrence_id,
        )
        .where(Ad_Merge_Occurrence.merged_occurrence_id == merged_occurrence.id)
        .where(Ad_Merge_Occurrence.reverted_at.is_(None))
        .order_by(Ad_Merge_Occurrence.position)
    ).all()
    placed = [
        (
            row.source_ad_id,
            (row.occurrence_date - merged_occurrence.occurrence_date).total_seconds(),
            row.role,
        )
        for row in rows
        if row.source_ad_id is not None
    ]
    if not any(role == ROLE_PIECE for _, _, role in placed):
        return None
    return merged_occurrence.id, placed


def plan_merged_ad(
    session: Session, merged_ad_id: str, _depth: int = 0, _seen: frozenset = frozenset()
) -> MergePlan:
    """Where each source ad goes in the video of the merged ad, recursively for the source ads that
    are merged ads themselves. Raises RebuildError when the ad is not a merged ad, or on a loop.
    """
    if merged_ad_id in _seen or _depth > MAX_MERGE_DEPTH:
        raise RebuildError(f"Loop in the merges of {merged_ad_id}")
    merge = session.get(Ad_Merge, merged_ad_id)
    merged_ad = session.get(Ad, merged_ad_id)
    if merge is None or merged_ad is None:
        raise RebuildError(f"{merged_ad_id} is not a merged ad")

    journal = _layout_from_journal(session, merged_ad_id)
    if journal is not None:
        occurrence_id, placed = journal
        plan = MergePlan(merged_ad_id, merged_ad.duration_sec, "journal", occurrence_id)
    else:
        placed, offset = [], 0.0
        durations = dict(
            session.execute(
                select(Ad.id, Ad.duration_sec).where(Ad.id.in_(merge.source_ad_ids))
            ).all()
        )
        for source_ad_id in merge.source_ad_ids:
            placed.append((source_ad_id, offset, ROLE_PIECE))
            offset += durations.get(source_ad_id) or 0.0
        plan = MergePlan(merged_ad_id, merged_ad.duration_sec, "end_to_end")

    source_ads = {
        ad.id: ad
        for ad in session.scalars(
            select(Ad).where(Ad.id.in_({ad_id for ad_id, _, _ in placed}))
        ).all()
    }
    merged_sources = set(
        session.scalars(
            select(Ad_Merge.merged_ad_id).where(
                Ad_Merge.merged_ad_id.in_(list(source_ads))
            )
        ).all()
    )
    for source_ad_id, offset, role in placed:
        source_ad = source_ads.get(source_ad_id)
        if source_ad is None:
            if role == ROLE_PIECE:
                raise RebuildError(f"Unknown source ad {source_ad_id}")
            continue
        sub_plan = None
        if source_ad_id in merged_sources:
            try:
                sub_plan = plan_merged_ad(
                    session, source_ad_id, _depth + 1, _seen | {merged_ad_id}
                )
            except RebuildError as e:
                logger.warning(f"No plan for the source ad {source_ad_id}: {e}")
        plan.segments.append(
            Segment(
                source_ad_id,
                offset,
                source_ad.duration_sec,
                role,
                source_ad.first_detection_date,
                sub_plan,
            )
        )
    return plan


# Steps shorter than this are left out. Their duration would be written 0.000 in the filter, which
# ffmpeg reads as "no limit" for `color` and `atrim`: an endless black and silent step that froze the
# run and then filled the memory of the job. They come from float sums (0.96 + 3.5199999809 is short of
# 4.48 by 2e-8 s, a "gap" at the end).
MIN_STEP_SEC = 0.001


def render_steps(segments: list[Segment], total_sec: float) -> list[Step]:
    """The steps of the rendered video, total_sec long: the segments in time order, black where none
    covers the time. Where two segments overlap, the later one starts where the earlier one ends.
    A step shorter than MIN_STEP_SEC is left out.
    """
    steps, cursor = [], 0.0

    def add(step: Step) -> None:
        if step.duration_sec >= MIN_STEP_SEC:
            steps.append(step)

    for segment in sorted(segments, key=lambda s: s.offset_sec):
        start = max(segment.offset_sec, cursor)
        end = min(segment.offset_sec + segment.duration_sec, total_sec)
        if start >= total_sec:
            break
        if end <= start:
            continue
        if start > cursor:
            add(Step(start - cursor))
        add(Step(end - start, segment, start - segment.offset_sec))
        cursor = end
    if cursor < total_sec:
        add(Step(total_sec - cursor))
    return steps


async def _probe(video_path: str) -> dict:
    process = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type,width,height,r_frame_rate",
        "-of",
        "json",
        video_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await _communicate(process, FFMPEG_TIMEOUT_SEC, f"ffprobe {video_path}")
    if process.returncode != 0:
        return {"streams": []}
    return json.loads(stdout or b"{}")


async def _run_ffmpeg(args: list[str], output_path: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-v",
        "error",
        *args,
        output_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await _communicate(
        process, FFMPEG_TIMEOUT_SEC, f"ffmpeg writing {output_path}"
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed to write {output_path} ({_exit_reason(process.returncode)}): "
            f"{stderr.decode(errors='replace')}"
        )


@dataclass
class _SourceVideo:
    path: str
    margin_sec: float
    has_audio: bool
    width: int | None
    height: int | None
    fps: str | None


async def _source_video(
    fs: s3fs.S3FileSystem,
    segment: Segment,
    work_dir: str,
    margin_sec: float,
    rebuilt: list[str],
) -> _SourceVideo | None:
    """The video of a source ad: its raw.mp4 in the bucket, or rebuilt from its own sources when it is
    a merged ad. None when there is neither."""
    # one file per source ad: a source ad found twice (in a merge of merges) is downloaded once
    path = os.path.join(work_dir, f"source_{segment.source_ad_id}.mp4")
    if not os.path.exists(path):
        try:
            await fs._get_file(ad_media_s3_key(segment.source_ad_id, "mp4"), path)
        except FileNotFoundError:
            if segment.sub_plan is None:
                return None
            try:
                await render_merged_ad(
                    fs, segment.sub_plan, path, work_dir, margin_sec, rebuilt
                )
            except RebuildError as e:
                logger.warning(
                    f"Can't rebuild the source ad {segment.source_ad_id}: {e}"
                )
                return None
            rebuilt.append(segment.source_ad_id)
    # raw.mp4 starts the export margin before the ad (changed over time, see ad_bucket.py); a rebuilt
    # video has margin_sec, which the duration gives back the same way
    source_margin = export_margin_sec(
        segment.duration_sec, segment.first_detection_date, await video_duration(path)
    )
    streams = (await _probe(path)).get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), {})
    return _SourceVideo(
        path,
        source_margin,
        any(s.get("codec_type") == "audio" for s in streams),
        video.get("width"),
        video.get("height"),
        video.get("r_frame_rate"),
    )


async def render_merged_ad(
    fs: s3fs.S3FileSystem,
    plan: MergePlan,
    output_path: str,
    work_dir: str,
    margin_sec: float,
    rebuilt: list[str] | None = None,
) -> list[str]:
    """Write the mp4 of the merged ad to output_path, with margin_sec of black before and after (like
    the export margin of a video cut from mediatree). Returns the ids of the source ads that had no
    video and were rebuilt from their own sources. Raises RebuildError when a piece has no video.
    """
    rebuilt = [] if rebuilt is None else rebuilt
    videos: dict[str, _SourceVideo] = {}
    missing_pieces, missing_absorbed = [], []
    for segment in plan.segments:
        if segment.source_ad_id in videos:
            continue
        video = await _source_video(fs, segment, work_dir, margin_sec, rebuilt)
        if video is not None:
            videos[segment.source_ad_id] = video
        elif segment.role == ROLE_ABSORBED:
            missing_absorbed.append(segment.source_ad_id)
        else:
            missing_pieces.append(segment.source_ad_id)
    if missing_pieces:
        raise RebuildError(
            f"No video for the pieces {missing_pieces} of the merged ad {plan.ad_id}"
        )
    if missing_absorbed:
        logger.info(
            f"Merged ad {plan.ad_id}: black instead of the absorbed segments {missing_absorbed}"
        )

    segments = [s for s in plan.segments if s.source_ad_id in videos]
    steps = render_steps(segments, plan.duration_sec)
    if margin_sec > 0:
        steps = [Step(margin_sec), *steps, Step(margin_sec)]

    reference = next((v for v in videos.values() if v.width and v.height), None)
    width = reference.width if reference else DEFAULT_WIDTH
    height = reference.height if reference else DEFAULT_HEIGHT
    fps = (reference.fps if reference else None) or DEFAULT_FPS
    # even dimensions for libx264
    width, height = width - width % 2, height - height % 2

    inputs = list(videos)  # source ad id -> input index
    args: list[str] = []
    for ad_id in inputs:
        args += ["-i", videos[ad_id].path]
    video_format = (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={fps},format=yuv420p"
    )
    audio_format = f"aresample={AUDIO_SAMPLE_RATE},aformat=channel_layouts=stereo"
    filters, labels = [], []
    for k, step in enumerate(steps):
        d = f"{step.duration_sec:.3f}"
        silence = (
            f"anullsrc=channel_layout=stereo:sample_rate={AUDIO_SAMPLE_RATE},"
            f"atrim=duration={d}[a{k}]"
        )
        if step.segment is None:
            filters.append(
                f"color=c=black:s={width}x{height}:r={fps}:d={d},"
                f"setsar=1,format=yuv420p[v{k}]"
            )
            filters.append(silence)
        else:
            i = inputs.index(step.segment.source_ad_id)
            video = videos[step.segment.source_ad_id]
            start = f"{video.margin_sec + step.start_in_source_sec:.3f}"
            filters.append(
                f"[{i}:v]trim=start={start}:duration={d},setpts=PTS-STARTPTS,"
                f"{video_format}[v{k}]"
            )
            if video.has_audio:
                filters.append(
                    f"[{i}:a]atrim=start={start}:duration={d},asetpts=PTS-STARTPTS,"
                    f"{audio_format}[a{k}]"
                )
            else:
                filters.append(silence)
        labels.append(f"[v{k}][a{k}]")
    filters.append(f"{''.join(labels)}concat=n={len(steps)}:v=1:a=1[v][a]")
    args += [
        "-filter_complex",
        ";".join(filters),
        "-map",
        "[v]",
        "-map",
        "[a]",
        *VIDEO_ENCODER_ARGS,
        "-c:a",
        "aac",
    ]
    await _run_ffmpeg(args, output_path)
    return rebuilt


async def mp4_to_mp3(mp4_path: str, mp3_path: str) -> None:
    await _run_ffmpeg(["-i", mp4_path, "-vn", "-c:a", "libmp3lame"], mp3_path)
