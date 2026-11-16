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
