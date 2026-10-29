"""Video of a new ad: cut from the 2-minutes archives of the mediatree bucket around one of its occurrences,
saved like the exportation does (ads/<ad_id>/raw.mp3 and raw.mp4 in the advertising bucket, see
s02_exportation), so that the classification can process the ad.

The occurrence is the most recent one whose archives are all in the bucket (listed first, without
downloading anything): the exportation, which takes the most recent occurrence of an ad, could fall on an
occurrence without archives.
"""

import asyncio
import logging
import os
from collections.abc import Sequence
from types import SimpleNamespace

from postgres.schemas.advertising.models import Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s02_exportation.run import (
    _ad_export_window,
    _export_ad,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    cleanup_day_media_parts,
    download_media_parts,
    get_s3_filesystem,
    list_available_part_starts,
    required_part_starts,
)

LOCAL_CACHE_DIR = "./.cache/manual_corrections"


async def _has_archives(fs, occurrence: Ad_Occurrence, window, available: dict) -> bool:
    for start in required_part_starts([window]):
        key = (occurrence.channel_name, start.date())
        if key not in available:
            available[key] = await list_available_part_starts(fs, *key)
        if start not in available[key]:
            return False
    return True


async def _find_and_export(
    ad_id: str,
    occurrences: Sequence[Ad_Occurrence],
    duration_sec: float,
    dry_run: bool,
) -> Ad_Occurrence | None:
    fs = get_s3_filesystem()
    ad = SimpleNamespace(id=ad_id, duration_sec=duration_sec)
    available: dict = {}
    for occurrence in sorted(
        occurrences, key=lambda o: o.occurrence_date, reverse=True
    ):
        window = _ad_export_window(ad, occurrence)
        if not await _has_archives(fs, occurrence, window, available):
            continue
        if dry_run:
            return occurrence
        local_dir = os.path.join(LOCAL_CACHE_DIR, ad_id)
        try:
            parts = await download_media_parts(
                fs,
                occurrence.channel_name,
                required_part_starts([window]),
                local_dir,
                disable_progress=True,
            )
            await _export_ad(ad, occurrence, *window, parts, local_dir, fs)
            return occurrence
        except Exception:
            logging.exception(
                "Export of the ad %s from the occurrence %s failed, next occurrence",
                ad_id,
                occurrence.id,
            )
        finally:
            cleanup_day_media_parts(local_dir)
    return None


def export_new_ad_video(
    ad_id: str,
    occurrences: Sequence[Ad_Occurrence],
    duration_sec: float,
    dry_run: bool,
) -> Ad_Occurrence | None:
    """Save the video of the ad from one of its occurrences. Returns this occurrence, None when no occurrence
    has its archives in the bucket. In dry run, only looks for the occurrence."""
    return asyncio.run(_find_and_export(ad_id, occurrences, duration_sec, dry_run))
