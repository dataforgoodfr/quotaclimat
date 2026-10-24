# Stage 6: screenshots (visual checks)

Screenshots of the ads and of a sample of their occurrences, to check by eye in Metabase that:

- the metadata of an ad (brand, sector, category) match what is shown;
- every occurrence of an ad is the same ad (detection, merges of `s04_consolidation`).

For each ad and each sampled occurrence, one mosaic jpg of `SCREENSHOT_NB_FRAMES` frames (6 by default,
3 columns), taken in the middle of equal slices of the ad duration. The positions depend only on the
duration of the ad: tile i of an occurrence's mosaic compares with tile i of its ad's mosaic. A frame
that could not be read (past the end of the video) is a black tile, so tiles stay in place.

```bash
START_DATE=2026-04-01 END_DATE=2026-04-08 poetry run python -m quotaclimat.data_ingestion.advertising.s06_screenshots.run
```

## Sources

- **Ads**: the video saved by the exportation (`s02_exportation`), `ads/<ad_id>/raw.mp4` in the advertising
  bucket. It starts `MARGIN_ON_MEDIA_EXPORT` before the occurrence it was cut from, taken into account.
  No video: `missing_source`.
- **Occurrences**: the 2-minutes archives of the mediatree bucket (`output/<channel>/<yyyy>/<mm>/<dd>/`),
  the ones the exportation cuts the ads from. The day folders are listed first: an occurrence with a
  frame in an archive missing from the bucket is marked `missing_source` (with the missing archives in
  `details`), without downloading anything. The archives of the other occurrences of a channel and day
  are downloaded once (mp4 only) and deleted after the group. The frames are taken at the nominal times
  of the archives (start of the archive in its name): a shift of the archive (overlap, see
  `tools/mediatree/overlay_correction.py`) shifts the frames by as much.

## Storage

Images in the advertising bucket, `screenshots/ads/<ad_id>.jpg` and
`screenshots/occurrences/<occurrence_id>.jpg`, uploaded with the ACL `SCREENSHOT_ACL` (`public-read` by
default) so that Metabase shows them from their URL (column setting "Display as: Image"). Ids are 32
characters hashes: the URLs can't be guessed, but anyone holding one sees the image.

One row per ad or occurrence in `advertising.ad_screenshot`:

| Column | Content |
|---|---|
| `target_type`, `target_id` | `ad` and `advertising.ad.id`, or `occurrence` and `advertising.ad_occurrence.id` |
| `ad_id` | the ad (of the occurrence) |
| `status` | `done`, `missing_source` (no video to take the frames from) or `error` |
| `source` | `ad_export` or `mediatree_archive` |
| `image_url` | public URL of the mosaic (when `done`) |
| `frame_offsets_sec`, `nb_frames` | offsets of the frames from the start of the ad, number of frames found |
| `details` | missing archives, error message |

A new run skips the `done` and `missing_source` rows (`RETRY_MISSING=true` to retry the latter) and
retries the errors. The rows of soft deleted occurrences stay.

## Selection

- Ads: by `first_detection_date` (like the exportation), occurrences: by `occurrence_date`.
- Only the classified ads by default (same `prediction_status` as `analytics.publicites`).
- `MAX_OCCURRENCES_PER_AD` occurrences per ad (3 by default, 0 for all), drawn by the md5 of their id:
  random but always the same, so a new run completes the same sample.

## Environment variables

| Variable | Default | Role |
|---|---|---|
| `START_DATE`, `END_DATE` | none | window, Paris time (`2026-04-01`) |
| `CHANNEL` | all | one channel |
| `AD_IDS` | all | comma separated ad ids, for a spot check |
| `TARGETS` | `ads,occurrences` | what to screenshot |
| `PREDICTION_STATUSES` | `classified` | `classified`, `all` or a comma separated list |
| `MAX_OCCURRENCES_PER_AD` | `3` | sample size per ad, `0` for every occurrence |
| `LIMIT` | none | max number of ads, and of occurrences |
| `RETRY_MISSING` | `false` | also retry the `missing_source` rows |
| `SCREENSHOT_ACL` | `public-read` | ACL of the uploaded images (`private` to keep them private) |
| `SCREENSHOT_NB_FRAMES`, `SCREENSHOT_MOSAIC_COLUMNS` | `6`, `3` | mosaic layout |
| `ADVERTISING_BUCKET_NAME`, `ADVERTISING_BUCKET_REGION`, `MEDIATREE_BUCKET_NAME`, `BUCKET`, `BUCKET_SECRET` | | buckets and credentials, as for the exportation |

Needs `ffmpeg` (image `quotaclimat_with_media_tools`).
