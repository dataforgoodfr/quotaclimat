# Screenshots (visual checks)

Scaleway job (`entrypoints/advertising_screenshots.sh`, image `quotaclimat_with_media_tools` for ffmpeg).
Screenshots of every ad and every occurrence, to check by eye in Metabase that:

- the metadata of an ad (brand, sector, category) match what is shown;
- every occurrence of an ad is the same ad (detection, merges of `s04_consolidation`).

For each ad and each occurrence, one mosaic jpg of `SCREENSHOT_NB_FRAMES` frames (6 by default,
3 columns), taken in the middle of equal slices of the ad duration. The positions depend only on the
duration of the ad: tile i of an occurrence's mosaic compares with tile i of its ad's mosaic. A frame
that could not be read (past the end of the video) is a black tile, so tiles stay in place.

```bash
START_DATE=2026-04-01 END_DATE=2026-04-08 poetry run python -m quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.run
```

## Sources

- **Ads**: the video saved by the exportation (`s02_exportation`), `ads/<ad_id>/raw.mp4` in the advertising
  bucket. It starts a margin before the ad, which changed over time (`MARGIN_ON_MEDIA_EXPORT_HISTORY` in
  `s02_exportation/ad_bucket.py`, shared with `recompute_saved_chunks.py`: 1s, then 0.1s from
  2026-04-27). The margin is the one of the history closest to (video duration - ad duration) / 2, so that
  an ad exported after its detection (merged ad, mp4 exported again) gets the margin of its export; the
  detection date gives it when ffprobe can't read the duration. It is kept in `details.margin_sec`.
  No video: `missing_source`.
- **Occurrences**: the 2-minutes archives of the mediatree bucket (`output/<channel>/<yyyy>/<mm>/<dd>/`),
  the ones the exportation cuts the ads from. The occurrences are streamed from Postgres one channel and
  UTC day at a time. The day folders are listed first: an occurrence with a frame in an archive missing
  from the bucket is marked `missing_source` (with the missing archives in `details`), without
  downloading anything; these are left for the Mediatree link. The archives of the other occurrences of
  the group are downloaded once (mp4 only) and deleted after the group. The frames are taken at the
  nominal times of the archives (start of the archive in its name): a shift of the archive (overlap, see
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
| `details` | margin of the ad video, missing archives, error message |

Readable by Metabase (`metabase-analytics-<env>`) and `rrs-read-<env>` thanks to the default privileges of the
database admin, set by `terraform_data.readonly_default_privileges` (`infrastructure/live/barometre/template/grants.tf`):
the readonly privilege of Scaleway does not cover the tables created by the migrations.

A separate table rather than columns of `ad` / `ad_occurrence`: the detection deletes and recreates the
occurrences of a window it reprocesses (same ids when the ad and its start don't change), which would
lose the columns, and the job only writes its own table, without updating the tables the detection and
the classification write.

A new run skips the `done` and `missing_source` rows (`RETRY_MISSING=true` to retry the latter) and
retries the errors. The rows of soft deleted occurrences stay.

## Selection

- Ads: by `first_detection_date` (like the exportation), occurrences: by `occurrence_date`.
  `OCCURRENCES_FROM` keeps only the occurrences broadcast from a date (e.g. `2025-09-29`, start of the ad
  analysis), and only the ads broadcast since then, whatever their detection date.
- Only the classified ads and their occurrences by default (same `prediction_status` as
  `analytics.publicites`): the others have no metadata to check.
- Every occurrence that is not soft deleted.

## Order

The most broadcast ads first (number of occurrences, from `OCCURRENCES_FROM` and on `CHANNEL` when set), so
that their checks are complete first. The occurrences are processed by batches of `ADS_PER_OCCURRENCE_BATCH`
ads (20 by default): within a batch, one channel and UTC day at a time, so that the occurrences sharing a
mediatree archive download it once. A bigger batch downloads fewer archives twice, a smaller one completes
the first ads sooner.

## Environment variables

| Variable | Default | Role |
|---|---|---|
| `START_DATE`, `END_DATE` | none | window, Paris time (`2026-04-01`) |
| `CHANNEL` | all | one channel |
| `AD_IDS` | all | comma separated ad ids, for a spot check |
| `TARGETS` | `ads,occurrences` | what to screenshot |
| `PREDICTION_STATUSES` | `classified` | `classified`, `all` or a comma separated list |
| `LIMIT` | none | max number of ads, and of occurrences |
| `OCCURRENCES_FROM` | none | only the occurrences broadcast from this date, Paris time (`2025-09-29`) |
| `ADS_PER_OCCURRENCE_BATCH` | `20` | ads per batch of occurrences, see Order |
| `RETRY_MISSING` | `false` | also retry the `missing_source` rows |
| `SCREENSHOT_ACL` | `public-read` | ACL of the uploaded images (`private` to keep them private) |
| `SCREENSHOT_NB_FRAMES`, `SCREENSHOT_MOSAIC_COLUMNS` | `6`, `3` | mosaic layout |
| `ADVERTISING_BUCKET_NAME`, `ADVERTISING_BUCKET_REGION`, `MEDIATREE_BUCKET_NAME`, `BUCKET`, `BUCKET_SECRET` | | buckets and credentials, as for the exportation |
