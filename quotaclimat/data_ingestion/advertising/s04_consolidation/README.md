# Stage 4: consolidation

Corrections of the detection, made after the classification (they use its brands), one sub-folder per
step, each with its own entry point:

- `e01_merge_split_ads`: unsupervised merge of the ads split in several pieces by the detection
  (`entrypoints/advertising_merge_split_ads.sh`).
- `e02_manual_corrections`: corrections listed by humans in a Google Sheet, for instance after a check of the
  screenshots (`entrypoints/advertising_manual_corrections.sh`).

## Merging ads

The database operations of a merge are in `tools/ad_merge.py`, to be reused by every script that merges
ads (automatic or manual). They only add to the session, the caller commits:

- `create_merged_ad(session, source_ad_ids, method, duration_sec=None, details=None)`: the ad merging the
  source ads (in broadcast order), with their fingerprints one after the other and no prediction, so that
  the exportation and classification steps process it like a new ad, and the detection recognises it
  when the pieces are broadcast together. Idempotent (stable id from the source ads). A merge reverted
  by `revert_merge` raises `MergeRevertedError` unless `reactivate_reverted=True`: the automatic script
  never redoes a merge a human judged wrong.
- `merge_occurrences(session, merged_ad_id, piece_occurrence_ids, absorbed_occurrence_ids=())`: one
  occurrence of the merged ad replaces one broadcast of the pieces; the occurrences of the pieces and of
  the segments between them are soft deleted (`deleted_at`).
- `revert_merge(session, merged_ad_id)`: restores the source occurrences still present, soft deletes the
  merged occurrences and empties the fingerprints of the merged ad.

History: `advertising.ad_merge` (one row per merged ad: source ads, method, status, details of the
creation) and `advertising.ad_merge_occurrence` (one row per replaced occurrence, with its role `piece`
or `absorbed`). A reprocessing of the detection deletes the occurrences of its window: the rows of
`ad_merge_occurrence` go with them (foreign keys on delete cascade), `ad_merge` stays, and the next run
merges the new occurrences again. Limit: the detection reprocesses whole weeks; for a merged ad
broadcast across the bound of the window, a piece outside the window stays soft deleted without its
journal row.

Example, manual merge of two pieces:

```python
from sqlalchemy.orm import Session
from postgres.database_connection import connect_to_db
from quotaclimat.data_ingestion.advertising.tools.ad_merge import create_merged_ad, merge_occurrences

with Session(connect_to_db(use_custom_json_serializer=True)) as session:
    ad_id = create_merged_ad(session, ["<ad A>", "<ad B>"], method="manual", details={"by": "..."})
    merge_occurrences(session, ad_id, ["<occurrence of A>", "<occurrence of B>"])
    session.commit()
```

## e01_merge_split_ads

Two consecutive segments of a channel with the same brand are pieces of one ad (two ads of the same
brand never follow each other) when only short segments separate them (`AD_MERGE_SHORT_SEC`, default
2 s, whatever their brand) and at most `AD_MERGE_GAP_MAX_SEC` (default 3 s) of uncovered time. The
brand is the canonical one of the dictionary, the raw one of the LLM otherwise. Merged cases: pieces
contiguous, separated by short segments without brand, or by a short segment of another brand. Not
merged (detection errors rather than split ads): the same ad twice in a row, a piece included in the
other one, pieces overlapping by more than 1 s, a gap of 1 to 3 s with nothing between. Pairs sharing a
piece are chained (ad split in 3 pieces or more).

Run regularly after the classification: by default it processes the 7 last days (`AD_MERGE_DAYS`), or
`AD_MERGE_START_DATE` to `AD_MERGE_END_DATE` (UTC) for a backfill, day by day. `AD_MERGE_DRY_RUN`
defaults to `true` (only logs): set it to `false` to write.

Until the merged ad is exported and classified, its occurrences are not in `ad_occurrences_classified`.

## e02_manual_corrections

Corrections listed by humans (or by code) in the private Google Sheet `Corrections_publicites`
(`MANUAL_CORRECTIONS_SPREADSHEET`), in the Drive folder of the reference sheets (`EXTERNAL_SOURCES_DRIVE_FOLDER`).
Share it as **editor** with the Google Sheets editor service account (`GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON`,
the one of `s05_enrichment/e01_brand_groups`): the job writes its columns in it. A spreadsheet readable by
anyone is refused, like the reference sheets.

```bash
MANUAL_CORRECTIONS_DRY_RUN=false poetry run python quotaclimat/data_ingestion/advertising/s04_consolidation/e02_manual_corrections/run.py
```

Run it after the merge of the split ads and before the classification: an ad sent back to the classification
or a new ad is classified by its next run. `MANUAL_CORRECTIONS_DRY_RUN` defaults to `true`: it logs the result of
every row, and writes nothing to the database, the buckets or the sheet. The new ads need ffmpeg (image
`quotaclimat_with_media_tools`).

### Tabs

Every tab is read, except the tabs for humans (name starting with `_`, e.g. `_lisez-moi`). The beginning of the
name of a tab gives its kind, all the tabs of a kind have the same columns: a new batch of corrections can go
to a new tab (`correction_ads_mai`, `nouvelle_ad_mazda`...). A tab with another name is skipped (warning in the
logs).

| Tab name starts with | Columns filled by humans | Columns filled by the job |
|---|---|---|
| `correction_ads` | `ad_id`, `marque`, `secteur`, `categorie`, `type_contenu` (empty: unchanged), free columns (`commentaire`...) | `traite_le`, `resultat`, `message`, `valeurs_precedentes` |
| `nouvelle_ad` | `occurrence_id`, `ad_indicator` (optional), free columns | `traite_le`, `resultat`, `message`, `nouvelle_ad_id` |
| `dechets` | `occurrence_id`, free columns | `traite_le`, `resultat`, `message` |

The columns of the job are added after the last column when missing. Each row is processed once: the rows with
a `traite_le` are skipped. To process a row again (after fixing it), empty its `traite_le`. `resultat`:

- `appliqué`: done;
- `avertissement`: done, but a value does not match the reference data (`message` says which);
- `erreur`: nothing changed (unknown ad or occurrence, unknown code...).

The sheet is written before the database is committed: when it cannot be written, nothing is changed and the
rows are processed again by the next run. A row moved by a human during the run (sorted, inserted above) is
found again by its `ad_id` / `occurrence_id`.

### Correction of an ad (`correction_ads`)

`secteur` and `categorie` are codes (`FOD`, `FOD2`, tables `advertising.ref_ome_secteurs` and
`ref_ome_categories`), `type_contenu` a content type of the classification (`AD`, `BUMPER`, `CHANNEL_PROMO`,
`SPONSORSHIP`, `OTHER`), in upper or lower case. An unknown code is an error. Warnings (reference data read in
the database, see `references.py`): a category of another sector, a brand that the dictionary
(`ref_ome_marques_type_*`) puts in another sector, a brand unknown from the dictionary, the brand inventory and
the brands already predicted (with the known brands of close spelling).

What changes in `advertising.ad`, depending on the ad:

- **an advertising already classified** (`fragment_type` `AD`, final `prediction_status`, see
  `s03_classification/prediction_status.py`): the given values replace `predicted_brand`, `predicted_sector`,
  `predicted_product_category`; `prediction_method` becomes `manual`, `prediction_status` stays;
- **another content type** (`type_contenu` other than `AD`): `fragment_type` changes, `prediction_method`
  becomes `manual`, the prediction stays. `analytics.publicites` only keeps the `AD`: the ad leaves the analyses;
- **an advertising not classified** (another content type corrected to `AD`, `no_sector_found`...): it goes back
  to the classification at the dictionary stage (`prediction_status = ad_type_done`), with the corrected brand and
  sector in its `ad_type` prediction. The dictionary, then the sub-category stage, give its sector and category,
  like for any ad: a given category is ignored (warning). Without sector and without a brand of the dictionary,
  the sub-category stage has no sector (warning).

The values before the correction (`fragment_type`, `prediction_status`, `prediction_method`, `predicted_*`) are
written in `valeurs_precedentes`, and in `advertising.ad.prediction`, in an entry
`{"stage": "manual_correction", "previous": ..., "corrected": ..., "source": {spreadsheet, tab, row}, "at": ...}`
kept by the classification stages. The other steps treat a corrected ad like the others: the canonicalisation of
the brands (`RECANONICALIZE_EXISTING=1`) passes on its brand too.

### New ad from occurrences (`nouvelle_ad`)

For occurrences attributed to the wrong ad by the detection. The rows of a tab not processed yet make one new ad,
or one per value of `ad_indicator` (e.g. `MAZDA` and `RENAULT`: two ads). The id of the ad comes from the
spreadsheet, the tab and the `ad_indicator`: rows added later with the same `ad_indicator` go to the same ad.

1. The video of the ad is cut from the most recent occurrence whose 2-minutes archives are in the mediatree
   bucket, and saved like the exportation does (`ads/<ad_id>/raw.mp3` and `raw.mp4`). Its duration is the most
   frequent duration of the ads of the occurrences (warning when they differ by more than 1 s). No occurrence with
   its archives: the ad is not created (error on the rows).
2. The ad is created without fingerprint (`chunks = []`), with `fragment_type = advertising` and no prediction:
   the classification processes it like a new ad.
3. Each occurrence is replaced by an occurrence of the new ad (id of the detection formula), the old one is soft
   deleted.

Limits: without fingerprint, the detection never attributes a broadcast to the new ad, the next broadcasts of
the spot still go to the old ad. A reprocessing of the detection deletes and recreates the occurrences of its
window: the moved occurrences come back to the old ad, and the rows are not processed again.

### Junk occurrences (`dechets`)

Occurrences that are not ads: soft deleted (`deleted_at`), like the occurrences replaced by a merge, so every
reader of `advertising.ad_occurrence` (dbt, screenshots, merge) ignores them. Same limit with a reprocessing of
the detection.

### Environment variables

| Variable | Default | Usage |
|---|---|---|
| `MANUAL_CORRECTIONS_SPREADSHEET` | `Corrections_publicites` | name of the spreadsheet in the Drive folder |
| `MANUAL_CORRECTIONS_DRY_RUN` | `true` | `false` to write |
| `EXTERNAL_SOURCES_DRIVE_FOLDER` | none | Drive folder of the spreadsheet (same as the dbt import) |
| `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` | none | JSON key of the service account that writes to Google Sheets |
| `POSTGRES_*` | | database |
| `BUCKET`, `BUCKET_SECRET`, `ADVERTISING_BUCKET_NAME`, `MEDIATREE_BUCKET_NAME` | | buckets of the videos (new ads) |
