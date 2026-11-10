# s02_exportation

Exports the detected ads (audio/video saved in the advertising bucket, `ads/<ad_id>/...`, see
`ad_bucket.py`). Entrypoint: `entrypoints/advertising_exportation.sh` (Scaleway job
`advertising_exportation`).

## Modes (`EXPORTATION_MODE`)

| Value | Script | What it does |
|---|---|---|
| `run` (default) | `run.py` | Exports the new ads. |
| `recompute_saved_chunks` | `recompute_saved_chunks.py` | Recomputes the audio chunks (fingerprints) of the ads already exported with the current fingerprinter parameters, identified by `fingerprinter.params_hash()`. |

## `recompute_saved_chunks` env vars

| Variable | Default | Purpose |
|---|---|---|
| `MAX_ADS` | unset (all) | Maximum number of ads processed in the run, e.g. `2000`. |
| `CLEAN_OTHER_CHUNKS` | `false` | `true` keeps only the chunk entry of the current hash on each ad. |
| `CURSOR_BATCH_SIZE` | `500` | Ads read and committed per batch. |

Ads with `fragment_type = 'no_data'` (no media, set by the classification) are never taken. Ads
that already have a chunk entry with the current hash are left out of the query (with
`CLEAN_OTHER_CHUNKS=true`, those that also have other entries are taken, to clean them). The ads
are taken by decreasing number of occurrences (not deleted), so a run with `MAX_ADS` handles the
most broadcast ads first, and the next run continues with the following ones. The job logs at
start the total number of ads, how many already have a chunk with the current hash, how many are
skipped as `no_data`, how many are left to process and how many are taken in this run.
