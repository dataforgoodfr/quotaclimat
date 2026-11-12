# s02_exportation

Exports the detected ads (audio/video saved in the advertising bucket, `ads/<ad_id>/...`, see
`ad_bucket.py`). Entrypoint: `entrypoints/advertising_exportation.sh` (Scaleway job
`advertising_exportation`).

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
| `CLEAN_OTHER_CHUNKS` | `false` | `true` keeps only the chunk entry of the current hash on each ad. |
| `CURSOR_BATCH_SIZE` | `500` | Ads read and committed per batch. |
| `AUDIO_WORKERS` | CPUs of the job (`SCW_SLS_CPU`) minus one, at least 1 | Processes computing the fingerprints of each batch. |
| `DOWNLOAD_CONCURRENCY` | `16` | Audio files downloaded at once. The run waits on S3 far more than on the CPU (~25 ms of computation for a 5 s ad). |

Ads with `fragment_type = 'no_data'` (no media, set by the classification) are never taken. Ads
that already have a chunk entry with the current hash are left out of the query (with
`CLEAN_OTHER_CHUNKS=true`, those that also have other entries are taken, to clean them). The ads
are taken by decreasing number of occurrences (not deleted), so a run with `MAX_ADS` handles the
most broadcast ads first, and the next run continues with the following ones. The job logs at
start the total number of ads, how many already have a chunk with the current hash, how many are
skipped as `no_data`, how many are left to process and how many are taken in this run. An audio file that cannot be decoded is
logged and counted as `decode_failed`, and the run goes on.
