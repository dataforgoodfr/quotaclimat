from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from quotaclimat.data_ingestion.advertising.s01_detection import \
    e00_download_audio
from quotaclimat.data_ingestion.advertising.s01_detection.e00_download_audio import (
    WeekAudio, delete_audio, download_jobs_audio)
from quotaclimat.data_ingestion.advertising.s01_detection.e02_split_in_chunks import \
    ChunkCreatorJob
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

START = datetime(2025, 10, 13, 11, 0, tzinfo=timezone.utc)


def job(i, tmp_path, contiguous_next=True):
    start = START + timedelta(minutes=2 * i)
    return ChunkCreatorJob(
        segment=Segment(start, start + timedelta(minutes=2), channel="tf1"),
        audio_file_path=str(tmp_path / f"{i}.mp3"),
        has_previous_segment=i > 0,
        next_audio_file_path=str(tmp_path / f"{i + 1}.mp3")
        if contiguous_next
        else None,
    )


@pytest.mark.asyncio
async def test_failed_downloads_drop_the_job_or_its_next_file(tmp_path, monkeypatch):
    monkeypatch.setattr(e00_download_audio, "AUDIO_DIR", str(tmp_path))
    jobs = [job(0, tmp_path), job(1, tmp_path), job(2, tmp_path)]
    week_audio = WeekAudio(
        jobs=jobs, s3_keys={str(tmp_path / f"{i}.mp3"): f"key{i}" for i in range(4)}
    )
    asked = []

    async def fake_download(fs, s3_keys, dest_dir, **kwargs):
        asked.extend(s3_keys)
        # key1 and key3 fail
        paths = []
        for key in s3_keys:
            if key in ("key0", "key2"):
                path = tmp_path / f"{key[-1]}.mp3"
                path.write_bytes(b"mp3")
                paths.append(str(path))
        return paths

    with (
        patch.object(e00_download_audio, "download_audio_archives", fake_download),
        patch.object(e00_download_audio, "get_s3_filesystem"),
    ):
        ready, downloaded = await download_jobs_audio(week_audio, jobs)

    # The next file of the last job is downloaded with the batch
    assert sorted(asked) == ["key0", "key1", "key2", "key3"]
    # Job 1 has no audio; jobs 0 and 2 run without their (missing) next file
    assert [j.audio_file_path for j in ready] == [
        jobs[0].audio_file_path,
        jobs[2].audio_file_path,
    ]
    assert all(j.next_audio_file_path is None for j in ready)

    delete_audio(downloaded)
    assert list(tmp_path.iterdir()) == []
