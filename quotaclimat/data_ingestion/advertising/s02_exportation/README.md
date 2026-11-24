# s02_exportation

Exports the detected ads (audio/video saved in the advertising bucket, `ads/<ad_id>/...`, see
`ad_bucket.py`). Entrypoint: `entrypoints/ad_s02_run_exportation.sh` (Scaleway job
`ad_s02_run_exportation`).

## Modes (`EXPORTATION_MODE`)

| Value | Script | What it does |
|---|---|---|
| `run` (default) | `run.py` | Exports the new ads. |
| `recompute_saved_chunks` | `recompute_saved_chunks.py` | Recomputes the audio chunks (fingerprints) of the ads already exported with the current fingerprinter parameters, identified by `fingerprinter.params_hash()`. |

## `run` env vars

| Variable | Default | Purpose |
|---|---|---|
| `SCREENSHOT_ON_EXPORT` | `true` | Makes the screenshot mosaic of each exported ad from its local video, see `s05_enrichment/e02_screenshots/README.md` (Sources). |
| `EXPORT_MERGED_ADS` | `false` | `false`: the ads that are not merged ads. `true`: only the merged ads of an active merge, see below. |
| `START_DATE`, `END_DATE` | unset | Window on the first detection of the ads (Paris time). A merged ad has the first detection of its oldest source ad. |
| `MAX_RUN_DURATION_MIN` | `300` | After this time, the run starts no new (channel, day) group, finishes the started ones and ends normally, before the 6 h timeout of the job. Each group is saved as soon as it is done (see below), so the next run goes on with the ads left. |
| `MAX_ADS` | unset (no limit) | Stops starting new groups once this many ads were taken. |
| `MAX_CONCURRENT_EXPORTS` | CPUs of the job (`SCW_SLS_CPU`) | Ads cut or rebuilt (ffmpeg), uploaded and screenshotted at once. The video encoding is most of the CPU of the run: more exports than CPUs only make them wait for each other and fill the memory of the job (its disk counts in it). The bucket checks have their own limit (50), so they no longer wait behind the exports. |
| `VIDEO_PRESET` | `veryfast` | libx264 preset of the videos cut or rebuilt (was the default `medium`: ~40 % more CPU for a file of about the same size, measured on a synthetic video). |
| `FFMPEG_THREADS` | `2` | Threads of each ffmpeg encoding. |

### What is left to export (`advertising.ad.media_export`)

The run only reads the ads left to export: `media_export` NULL, or an mp4 smaller than
`(duration + 2) × 10 kB`. `media_export` is a JSON written for each ad as soon as its (channel, day)
group is done:

| Key | Content |
|---|---|
| `exported_at` | Upload of `raw.mp4` (UTC, ISO). For an ad found in the bucket: the `LastModified` of `raw.mp4`. |
| `mp4_bytes` | Size of `raw.mp4`. Missing size: the ad is trusted (not exported again). |
| `margin_sec` | Margin of the video before and after the ad: `MARGIN_ON_MEDIA_EXPORT` for an export of this run; for an ad found in the bucket, the margin of `MARGIN_ON_MEDIA_EXPORT_HISTORY` (`ad_bucket.py`) at its `exported_at` (null before the history). |
| `method` | `mediatree_cut` (cut from the mediatree archives) or `merged_rebuild` (merged ad rebuilt from its sources). Absent for an ad found in the bucket: not known. |

An ad with `media_export` NULL is looked up in the bucket (one request on `raw.mp4`): missing or too
small, it is exported; present, its `media_export` is made from it, so the next runs no longer check it.
The column needs no backfill: the first run after the migration checks the bucket as before and fills
it. A folder without `raw.mp4` is not exported again (as before) and stays NULL. The new ads of the
manual corrections (`s04_consolidation/e02_manual_corrections`) are saved without `media_export`: the
next run finds them in the bucket.

To have an ad exported again (video deleted by hand, wrong cut), set its `media_export` back to NULL.

The `no_data` ads whose media are now in the bucket get `fragment_type = 'advertising'` with the same
group commit (before, only at the end of a run, so never when the run timed out).

### Merged ads (`EXPORT_MERGED_ADS=true`)

A merged ad (`advertising.ad_merge`, see `s04_consolidation/README.md`) is a new ad: nothing is in the
advertising bucket for it until it is exported. A run with `EXPORT_MERGED_ADS=false` (default) leaves out
every merged ad, whatever the status of its merge; a run with `EXPORT_MERGED_ADS=true` takes only the
merged ads of an active merge, without video or with a too small mp4, like the default run does for the
other ads. Schedule both runs: a merged ad is only exported by the second one.

For each merged ad (`merged_ads.py`):

1. its video is cut from the mediatree archives of its most recent occurrence, like any ad;
2. when these archives are no longer in the mediatree bucket, it is rebuilt from the videos of the ads it
   was merged from (`ads/<source_ad_id>/raw.mp4`), placed as they were broadcast in the most recent
   occurrence of the merged ad that still has its journal rows (`advertising.ad_merge_occurrence`): the
   pieces, and the short segments absorbed between them. The time covered by no source video (gap
   between two pieces, absorbed segment without video) is black and silent. Without journal rows
   (deleted by a reprocessing of the detection), the pieces are put end to end. The video lasts the
   duration of the merged ad, cut or padded with black at the end, with the export margin
   (`MARGIN_ON_MEDIA_EXPORT`) of black before and after; the mp3 is made from it. The margin of each
   source video is found from its duration, like the screenshots job does (`export_margin_sec`);
3. a source ad without video that is itself a merged ad is rebuilt the same way, recursively (its video
   is only used for this rebuild, not saved). A piece that has no video and can't be rebuilt makes the
   rebuild fail: the merged ad stays in the missing ads of the log, and the next run tries again.

Limits: a rebuilt video is made of the videos of the pieces, cut from other broadcasts than the one of
the layout; where two pieces overlap (up to 1 s, see `e01_merge_split_ads`), the second one starts where
the first one ends. The log says for each rebuilt ad its layout (`journal` with the occurrence, or
`end_to_end`), and the progress counts `rebuilt` apart from `uploaded`.

## `recompute_saved_chunks` env vars

| Variable | Default | Purpose |
|---|---|---|
| `MAX_ADS` | unset (all) | Maximum number of ads processed in the run, e.g. `2000`. |
| `CLEAN_OTHER_CHUNKS` | `false` | `true` keeps only the chunk entries of the current hash on each ad (several when fingerprints were copied by a manual fusion, see `s04_consolidation/README.md`). |
| `CURSOR_BATCH_SIZE` | `500` | Ads read and committed per batch. |
| `AUDIO_WORKERS` | CPUs of the job (`SCW_SLS_CPU`) minus one, at least 1 | Processes computing the fingerprints of each batch. |
| `DOWNLOAD_CONCURRENCY` | `16` | Ads downloaded or being computed at once. The run waits on S3 far more than on the CPU (~25 ms of computation for a 5 s ad). It also caps the audio files waiting on disk for a worker: the disk of a Scaleway job counts in its memory. |
| `WORKER_MAX_TASKS` | `1000` | Ads a worker process computes before being replaced by a new one, so that the memory it keeps does not grow during the whole run. |

Memory: each worker process loads librosa and its dependencies. With 3 GB, 5 workers (6 vCPU)
exceeded the memory of the job; lower `AUDIO_WORKERS` or raise the memory of the job.

Ads with `fragment_type = 'no_data'` (no media, set by the classification) and soft deleted ads
(`deleted_at`, merged by hand into another) are never taken, except a soft deleted ad that is a
source of an active merge: the concatenation of the merge needs its fingerprints. Ads that already have a chunk entry with
the current hash are left out of the query (with `CLEAN_OTHER_CHUNKS=true`, those that also have
entries of another hash are taken, to clean them). The ads
are taken by decreasing number of occurrences (not deleted), so a run with `MAX_ADS` handles the
most broadcast ads first, and the next run continues with the following ones. An audio file that
cannot be decoded is logged and counted as `decode_failed`, and the run goes on.

One ad never stops the run. When a worker process dies (killed by the kernel when the job
exceeds its memory), the whole pool of processes is broken: it is restarted (`pool_restarts`),
and the ads that were computing in it are retried one at a time; an ad that kills its worker
again on its own is skipped and counted as `worker_crashed`. Any other unexpected error on an ad
is counted as `error`. At the end of the run (even when it stops on another error), the job logs
the summary of the failed ads by status (`download_failed`, `decode_failed`, `worker_crashed`,
`error`), with their ids and errors (200 at most per status).

### Merged ads (`advertising.ad_merge`)

A merged ad (see `s04_consolidation`) is never downloaded nor cut in chunks:

- an active merge gets, at the end of each run, the fingerprints of its source ads one after the
  other (as `tools/ad_merge.concatenate_chunks`), once all of them have a chunk entry with the
  current hash. This entry replaces the merged ad's own entry with this hash and is marked
  `"from_sources": true`, so that it is not rebuilt at the next runs. Database only: it is done
  for every merge ready, whatever `MAX_ADS`. A merge whose sources are not all done yet waits for
  the next run (`merges_waiting_for_sources`). Fingerprints copied by a manual fusion
  (`"from_ad_id"`) are handled apart: a source ad gives its own entry, and the copied entries of
  the merged ad are kept. A merged ad soft deleted by a manual fusion is skipped;
- a reverted merge keeps the empty chunks set by `revert_merge`, so that the detection no longer
  matches it.

The occurrences of a source ad are moved to the merged ad, so a source ad is ranked by its own
occurrences plus those of the active merged ads it is a source of: the sources of the most
broadcast merges come first.

The job logs at start the total number of ads, how many already have a chunk with the current
hash, how many are skipped as `no_data`, how many are merged ads (and how many active merges are
left to concatenate), how many are left to process and how many are taken in this run.
