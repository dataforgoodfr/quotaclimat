import logging
import os
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
import s3fs

from postgres.schemas.advertising.models import AdvertisingBase
from postgres.schemas.models import (
    connect_to_db,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e02_split_in_chunks import (
    ChunkCreatorJob,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import (
    chunk_creator,
    processor,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    _download_part,
    _floor_to_2_minutes,
    _get_s3_archive_key_for_part,
    get_s3_filesystem,
)
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@patch(
    "quotaclimat.data_ingestion.advertising.s01_detection.processor.download_all_audio_parts",
)
async def test_extract_fragments_run_successfully(mocked_download_all_audio_parts):
    # This should be put in pytest configuration
    conn = connect_to_db()
    AdvertisingBase.metadata.drop_all(conn)
    AdvertisingBase.metadata.create_all(conn, checkfirst=True)

    channel = "test-channel"

    segments = [
        Segment(
            start_date=datetime(2025, 5, 5, 12, 00, tzinfo=ZoneInfo("Europe/Paris")),
            end_date=datetime(2025, 5, 5, 12, 1, tzinfo=ZoneInfo("Europe/Paris")),
            channel=channel,
        ),
        Segment(
            start_date=datetime(2025, 5, 5, 12, 3, tzinfo=ZoneInfo("Europe/Paris")),
            end_date=datetime(2025, 5, 5, 12, 4, tzinfo=ZoneInfo("Europe/Paris")),
            channel=channel,
        ),
    ]

    mocked_download_all_audio_parts.return_value = [
        ChunkCreatorJob(
            segment=segments[0],
            audio_file_path="test/advertising_detection/assets/tf1_1.mp3",
        ),
        ChunkCreatorJob(
            segment=segments[1],
            audio_file_path="test/advertising_detection/assets/tf1_2.mp3",
        ),
    ]

    fragments = await processor(
        channel=channel,
        start_date=datetime(2025, 5, 5, 12, 00, tzinfo=ZoneInfo("Europe/Paris")),
        end_date=datetime(2025, 5, 5, 12, 4, tzinfo=ZoneInfo("Europe/Paris")),
        operation_name="test_extract_fragments_run_successfully",
        report_folder=None,
    )

    maybe_ads = [f for f in fragments if f.classification == "new_ad"]
    assert len(maybe_ads) == 2
    assert maybe_ads[0].group_id == maybe_ads[1].group_id

    # It was 20, it may depends on the splitting algo, it needs to be checked again
    AD_DURATION = 11
    assert maybe_ads[0].end_sec - maybe_ads[0].start_sec >= AD_DURATION
    assert maybe_ads[0].end_sec - maybe_ads[0].start_sec <= AD_DURATION + 1
    assert maybe_ads[1].end_sec - maybe_ads[1].start_sec >= AD_DURATION
    assert maybe_ads[1].end_sec - maybe_ads[1].start_sec <= AD_DURATION + 1

    start_date_1 = datetime.fromtimestamp(maybe_ads[0].start_sec).astimezone(
        ZoneInfo("Europe/Paris")
    )
    start_date_2 = datetime.fromtimestamp(maybe_ads[1].start_sec).astimezone(
        ZoneInfo("Europe/Paris")
    )
    assert start_date_1 >= segments[0].start_date
    assert start_date_1 <= segments[0].end_date
    assert start_date_2 >= segments[1].start_date
    assert start_date_2 <= segments[1].end_date


test_cases = [
    ("tf1", 1787809775, 1787597261.6, 14, "astronautes"),
    ("tf1", 1788012613, 1787655192.5, 14, "promo koh lanta"),
    ("tf1", 1787334759.7, 1787204792.6, 16.2, "mat mut"),
]

# Splitting is not fully deterministic across different occurrences of the same
# ad (encoding noise, silence depth, ...), so chunk durations are only expected
# to match up to this margin.
_CHUNK_DURATION_MARGIN_SEC = 0.3

_MEDIATREE_TEST_CACHE_DIR = "./.cache/mediatree"


async def _download_job_for_start(channel: str, start_epoch: float) -> ChunkCreatorJob:
    """Download the 2-minutes mediatree audio part covering `start_epoch` (epoch
    seconds) and build the ChunkCreatorJob to run the splitting on it, the same way
    notebooks/e02_test_splitting.ipynb does it.
    """
    start_date = datetime.fromtimestamp(start_epoch, tz=ZoneInfo("UTC"))
    rounded_start_date = _floor_to_2_minutes(start_date)
    rounded_end_date = rounded_start_date + timedelta(minutes=2)

    os.makedirs(_MEDIATREE_TEST_CACHE_DIR, exist_ok=True)
    # s3fs caches filesystem instances by config and reuses them across calls; since
    # each parametrized test gets its own event loop (function-scoped asyncio loop),
    # a reused instance ends up bound to an already-closed loop from a previous test.
    s3fs.S3FileSystem.clear_instance_cache()
    fs = get_s3_filesystem()
    s3_key = _get_s3_archive_key_for_part(channel, rounded_start_date, rounded_end_date)
    audio_file_path = await _download_part(fs, s3_key, _MEDIATREE_TEST_CACHE_DIR)

    segment = Segment(
        start_date=rounded_start_date, end_date=rounded_end_date, channel=channel
    )
    return ChunkCreatorJob(segment=segment, audio_file_path=audio_file_path)


async def _split_window(
    channel: str, start_epoch: float, duration: float
) -> list[float]:
    """Download and split the 2-minutes audio part covering `start_epoch`, then
    return the durations of the chunks overlapping [start_epoch, start_epoch +
    duration), clipped to that window.

    Chunk boundaries drift by a few hundredths of a second between two airings of
    the same ad (the given timestamps are approximate, not exact chunk
    boundaries), so a chunk straddling either edge of the window is kept but only
    the portion inside the window is counted, instead of requiring its start (or
    end) to fall exactly within the window.
    """
    job = await _download_job_for_start(channel, start_epoch)
    chunks = chunk_creator.run(job)

    window_end = start_epoch + duration
    return [
        min(c.end_sec, window_end) - max(c.start_sec, start_epoch)
        for c in chunks
        if c.start_sec < window_end and c.end_sec > start_epoch
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "channel,start_a,start_b,duration,name",
    test_cases,
    ids=[case[4] for case in test_cases],
)
async def test_splitting_is_reproducible_across_occurrences(
    channel, start_a, start_b, duration, name
):
    """Two occurrences of the same ad should be split into the same number of
    chunks, with matching durations in the same order, regardless of when they
    aired.
    """
    durations_a = await _split_window(channel, start_a, duration)
    durations_b = await _split_window(channel, start_b, duration)

    assert len(durations_a) > 0, f"[{name}] no chunk found in window A"
    assert len(durations_a) == len(durations_b), (
        f"[{name}] got {len(durations_a)} vs {len(durations_b)}: {durations_a} vs {durations_b}"
    )

    for i, (duration_a, duration_b) in enumerate(zip(durations_a, durations_b)):
        assert abs(duration_a - duration_b) <= _CHUNK_DURATION_MARGIN_SEC, (
            f"[{name}] chunk {i} duration differs by more than "
            f"{_CHUNK_DURATION_MARGIN_SEC}s: {duration_a:.2f}s vs {duration_b:.2f}s"
        )
