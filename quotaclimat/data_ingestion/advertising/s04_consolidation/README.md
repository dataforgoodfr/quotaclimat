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

- `create_merged_ad(session, source_ad_ids, method, duration_sec=None, details=None, dictionary=None)`: the
  ad merging the source ads (in broadcast order), with their fingerprints one after the other, so that the
  detection recognises it when the pieces are broadcast together. It takes the classification of the source
  ads when they agree, or the one of the brand dictionary on their whole transcript, or the one of the
  pieces covering the longest duration (`inherit_classification`, see below); otherwise it has no prediction, and the exportation and
  classification steps process it like a new ad. Idempotent (stable id from the source
  ads). A merge reverted by `revert_merge` raises `MergeRevertedError` unless `reactivate_reverted=True`:
  the automatic script never redoes a merge a human judged wrong.
- `inherit_classification(session, merged_ad_id, source_ads, dictionary=None)`: when every source ad is an
  advertising (`fragment_type` `AD`) with a final status whose sub-category is known (not `dict_miss`,
  `dict_tier2_no_kw` or `dict_tier3_no_kw`, still waiting for stage 4), and they all have the same brand
  (compared without case, spaces nor punctuation, like the detection of the pieces), the merged ad gets
  `fragment_type = 'AD'` and:
  1. when the source ads have the same sector and category (an empty category included), their
     classification: `prediction_status` and `prediction_method` `merge_inherited` (a final status, shown in
     the analyses like its pieces), the brand as spelled for the longest piece, the lowest confidence;
  2. otherwise, the classification of the brand dictionary of the classification (`dictionary`, see
     `load_brand_dictionary`), applied like stage 3 (`s03_classification/pipeline/e03_dict_match.py`) to
     the product names and the transcripts of the pieces one after the other, when it decides the sector
     and the category: the brand alone (tier 1) or a product keyword (tiers 2 and 3). Each piece was
     classified on its own part of the transcript, so with its own keywords (one piece says "SUV", the
     other "hybride rechargeable"): the whole transcript holds all of them. `prediction_status` is the one
     of the dictionary (`dict_tier1`, `dict_tier2`, `dict_tier3`), `prediction_method`
     `merge_inherited+<status>`, the brand of the dictionary, no confidence, and `prediction` also gets the
     `dict_match` entry of stage 3;
  3. otherwise (a tier 2 brand without keyword, the dictionary giving the sector only, a tier 3 brand
     without keyword, a brand out of the dictionary, or no dictionary), the sector and category of the
     pieces covering the longest duration; on a tie, the ones of the last piece in broadcast order.
     `prediction_status` `merge_inherited`, `prediction_method` `merge_inherited+majority`, the brand as
     spelled for the longest of these pieces, their lowest confidence.

  In all cases the merged ad keeps the transcripts of the pieces one after the other (the transcription
  stage only processes the ads without transcript: it would mark the merged ad `no_data` while its video
  is not exported), and in `prediction` the `ad_type` entry of the longest piece (of the majority in case
  3) and a `merge_inherited` entry with the source ads, their statuses, methods and classifications
  (`source_classifications`: duration, sector and category of each piece), with `rule:
  inherited_from_dictionary` in case 2, `rule: inherited_from_majority` and `majority_duration_share` (share
  of the duration of the pieces covered by the majority, 0 to 1) in case 3. The source ads keep their own
  prediction. Only a merged ad without classification (`prediction_status` empty) is
  changed: a classification of the merged ad itself or a manual correction is kept. To classify the
  merged ads from their whole video instead, empty their `prediction_status` (and `transcript`): the
  classification processes them like new ads.
- `inherit_classification_of_merges(session, dry_run=False, dictionary=None)`: `inherit_classification`
  for every active merge whose merged ad has no classification: merges created before these rules, or
  whose pieces were classified after the merge. Returns the number of merges per reason (`inherited`,
  `inherited_from_dictionary`, `inherited_from_majority`, `piece_not_classified`,
  `piece_waiting_for_subcategory`, `other_brand`, `other_sector`: the pieces covering the longest duration
  have no sector).
- `load_brand_dictionary(session)`: the brand dictionary of the classification, from its copy in the
  database (dbt seeds `advertising.ref_ome_marques_type_1/2/3`, downloaded from the
  `Dictionnaire_marques_secteurs` Google Sheet, see `my_dbt_project/external_sources.yml`): the merge job
  runs without the Excel file of `s03_classification/reference_data/`. The rows are read in the order the
  seeds were loaded, the order of the tabs: the first rule of a brand with a keyword hit wins, like stage 3.
  `None` when a table is missing (extended perimeter): the second case of `inherit_classification` is then
  skipped (warning).
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
`AD_MERGE_START_DATE` to `AD_MERGE_END_DATE` (UTC) for a backfill, day by day. Then every merged ad still
not classified takes the classification of its pieces when they agree, or the one of the brand dictionary
on their whole transcript, or the one of the pieces covering the longest duration
(`inherit_classification_of_merges`), whatever its date. `AD_MERGE_DRY_RUN` defaults to `true` (only logs, including the number of merged ads
per reason of `inherit_classification_of_merges`): set it to `false` to write. `AD_MERGE_INHERIT_ONLY=true`
skips the merge of the windows: only the merged ads not classified are classified from their pieces.

A merged ad with a piece that is not a classified advertising (another content type, not classified yet,
waiting for its sub-category), or pieces of different brands, has no classification: until it is exported
and classified (or its pieces classified, at the next run), its occurrences are not in
`analytics.publicites`.

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
`quotaclimat_with_media_tools`). In production, a Scaleway job whose definition is updated on every push to `main`
by `.github/workflows/deploy-main.yml` (job id in the GitHub secret `SCALEWAY_JOB_ADVERTISING_MANUAL_CORRECTIONS`).

### Tabs

Every tab is read, except the tabs for humans (name starting with `_`, e.g. `_lisez-moi`). The beginning of the
name of a tab gives its kind, all the tabs of a kind have the same columns: a new batch of corrections can go
to a new tab (`correction_ads_mai`, `nouvelle_ad_mazda`...). A tab with another name is skipped (warning in the
logs).

| Tab name starts with | Columns filled by humans | Columns filled by the job |
|---|---|---|
| `correction_ads` | `ad_id`, `marque`, `secteur`, `categorie`, `type_contenu` (empty: unchanged), free columns (`commentaire`...) | `traite_le`, `resultat`, `message`, `valeurs_precedentes` |
| `nouvelle_ad` | `occurrence_id`, `ad_indicator` (optional), free columns | `traite_le`, `resultat`, `message`, `nouvelle_ad_id`, `nouvelle_occurrence_id` |
| `dechets` | `occurrence_id`, free columns | `traite_le`, `resultat`, `message` |
| `reattribution` | `occurrence_id`, `ad_id`, free columns | `traite_le`, `resultat`, `message`, `nouvelle_occurrence_id` |
| `fusion` | `ad_id`, `target_ad_id`, free columns | `traite_le`, `resultat`, `message`, `nb_occurrences_deplacees` |

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
- **another content type** (`type_contenu` other than `AD`): the ad becomes like a content the classification
  found not to be an advertising. `fragment_type` and the `content_type` of its `ad_type` prediction take the
  content type, `prediction_status` becomes `ad_type_done` (the dictionary stage skips a content type other than
  `AD`), `predicted_brand`, `predicted_sector` and `predicted_product_category` are emptied (a given brand, sector
  or category is ignored, warning), `prediction_method` becomes `manual`. The ad leaves the analyses, including the
  Metabase queries reading `advertising.ad` with a filter on `prediction_status` or `predicted_brand` but not on
  `fragment_type`, and the screenshots of its new occurrences (`s05_enrichment/e02_screenshots` only takes the
  final statuses). The `brand_name` of the classification stays in the `ad_type` prediction: corrected back to
  `AD`, the ad goes back to the classification (next case). An ad corrected before this rule keeps its final
  status and its prediction: empty the `traite_le` of its row to apply the correction again;
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
3. Each occurrence is replaced by an occurrence of the new ad (id of the detection formula, written in
   `nouvelle_occurrence_id`), the old one is soft deleted: it keeps its old `ad_id`, with a `deleted_at`.

Limits: without fingerprint, the detection never attributes a broadcast to the new ad, the next broadcasts of
the spot still go to the old ad. A reprocessing of the detection deletes and recreates the occurrences of its
window: the moved occurrences come back to the old ad, and the rows are not processed again.

### Junk occurrences (`dechets`)

Occurrences that are not ads: soft deleted (`deleted_at`), like the occurrences replaced by a merge, so every
reader of `advertising.ad_occurrence` (dbt, screenshots, merge) ignores them. Same limit with a reprocessing of
the detection.

### Occurrences moved to an existing ad (`reattribution`)

For an occurrence attributed to the wrong ad by the detection, when the right ad already exists: one row per
occurrence, with the id of the right ad (`ad_id`). The occurrence is replaced by an occurrence of this ad (id of
the detection formula, written in `nouvelle_occurrence_id`), the old one is soft deleted (it keeps its old
`ad_id`, with a `deleted_at`), like for a new ad.
Errors: unknown or deleted occurrence, unknown or empty `ad_id`, occurrence already in this ad, occurrence twice
in the tab. Warnings: an ad of another duration than the ad of the occurrence (by more than 1 s), an ad that is
not a classified advertising (the occurrence is then left out of `analytics.publicites`). Same limit with a
reprocessing of the detection.

### Merge of two ads (`fusion`)

For the same spot stored as two ads (e.g. a pair found by `duplicate_ads`), when the data of one of them is wrong:
one row per ad to retire (`ad_id`), with the ad that keeps the occurrences (`target_ad_id`).

0. The active merges of split pieces (`advertising.ad_merge`) whose merged ad or one of the pieces is `ad_id` are
   reverted first (`tools/ad_merge.revert_merge`): the occurrences of the pieces come back, the merged ad loses its
   occurrences and its fingerprints, and the merge is marked `reverted` (never redone). A wrong brand makes wrong
   merges: the merge only joins pieces of the same brand, so a spot classified with the brand of another one is
   glued to it when they are broadcast one after the other. A piece then gives its restored occurrences to the
   target; a merged ad gives them back to its pieces and has none left to move. The `message` names each merge
   reverted, `prediction` keeps them in `reverted_merges`. A merge already reverted changes nothing.
1. Every live occurrence of `ad_id` is moved to `target_ad_id` like a reattribution: replaced by an occurrence of
   the target ad (id of the detection formula; when the target already has an occurrence at the same date on the
   same channel, the two make one), the old one soft deleted. The number of occurrences moved is written in
   `nb_occurrences_deplacees`.
2. The fingerprints of `ad_id` (its `chunks` entries) are copied to `target_ad_id`, with `"from_ad_id": <ad_id>`:
   the detection then matches the spot with both, which helps when they were computed with another fingerprinter
   configuration (another `hash`) or on a slightly different recording. The detection already loops over every
   entry of the current hash of an ad, and `recompute_saved_chunks.py` keeps them all (`CLEAN_OTHER_CHUNKS` only
   removes the entries of the other hashes). Not copied: an entry already in the target, an entry without
   fingerprints, and every entry when the two ads differ by more than 1 s (warning): a matched broadcast takes the
   duration of the target, fingerprints of another duration would cut it wrong. The `message` gives the number
   of entries copied.
3. The ad `ad_id` is **soft deleted**: `advertising.ad.deleted_at` (date of the fusion) and `replaced_by_ad_id`
   (the target) are set, and an entry `{"stage": "manual_fusion", "replaced_by": ..., "nb_occurrences": ...,
   "nb_fingerprint_entries_copied": ..., "reverted_merges": [...], "source": {spreadsheet, tab, row}, "at": ...}` is added to its `prediction`. Nothing is deleted: the ad, its old
   occurrences (with a `deleted_at`) and its screenshots stay, for the history, like the merges of split pieces.

A soft deleted ad is left out of every step:

| Step | What it does with a soft deleted ad |
|---|---|
| detection (`s01_detection/e03_already_identified_advertising.py`) | its fingerprints are no longer matched: the next broadcasts of the spot go to the target (or to a new ad if the target does not match them), also when the detection reprocesses a window |
| exportation (`s02_exportation/run.py`, `recompute_saved_chunks.py`) | not exported, no fingerprints recomputed |
| classification (`s03_classification`, every stage) | not classified |
| manual corrections | an error in every tab: not corrected, no occurrence moved to it (`reattribution`, `nouvelle_ad`), not merged again nor a target of a `fusion`; its brand is not a known brand of the warnings |
| screenshots (`s05_enrichment/e02_screenshots`) | no new screenshot |
| duplicates (`duplicate_ads`) | not compared |
| dbt (`analytics.publicites`, `curated.publicite`, tunnels) | out, like any ad without live occurrence (they read the live occurrences) |

The same id in both columns changes nothing and the row is marked as processed (`appliqué`). The rows of a tab are
processed in order: `A -> B` then `B -> C` works, `B -> C` then `A -> B` is an error (B is deleted, put C as target).
Errors: empty `target_ad_id`, unknown ad, deleted ad or target. Warnings: a target of another duration (by more than 1 s) or that is not a classified
advertising. To undo a fusion, by hand: empty `deleted_at` and `replaced_by_ad_id`, remove from the target the
`chunks` entries whose `from_ad_id` is the ad, and move the occurrences back with a `reattribution` tab.
Limit: a merge of split pieces (`e01_merge_split_ads`, `tools/ad_merge.concatenate_chunks`) takes one entry per hash
of each piece, so a piece holding fingerprints copied by a fusion gives only one of its entries to the merged ad.

### Environment variables

| Variable | Default | Usage |
|---|---|---|
| `MANUAL_CORRECTIONS_SPREADSHEET` | `Corrections_publicites` | name of the spreadsheet in the Drive folder |
| `MANUAL_CORRECTIONS_DRY_RUN` | `true` | `false` to write |
| `EXTERNAL_SOURCES_DRIVE_FOLDER` | none | Drive folder of the spreadsheet (same as the dbt import) |
| `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` | none | JSON key of the service account that writes to Google Sheets |
| `POSTGRES_*` | | database |
| `BUCKET`, `BUCKET_SECRET`, `ADVERTISING_BUCKET_NAME`, `MEDIATREE_BUCKET_NAME` | | buckets of the videos (new ads) |

## duplicate_ads (exploration)

`duplicate_ads/find_duplicates.py` looks for pairs of ads of the database that are the same audio stored twice,
from the fingerprints of the chunk entry of a hash (the current fingerprinter by default). It writes nothing to the
database: the pairs go to a CSV file, to check by eye before merging or correcting anything. Not scheduled.

How the 400k ads are compared without comparing every pair:
- the ads are streamed by increasing duration, and an ad is only compared with the ads at most `--duration-tol`
  seconds shorter (sliding window);
- `--blocking brand` (default) compares only the ads of the same brand (`tools/ad_merge.brand_key`); `none` compares
  all the ads of the window;
- an inverted index of quantized pairs (the keys of `FingerprintsIndex`, a sample of `--index-pairs` pairs per ad)
  keeps the ads sharing at least `--min-shared-keys` keys; the keys shared by more than `--max-posting` ads are
  ignored;
- the candidates are scored with `FingerprintsCompare._score` (tolerances of the detection) on the fingerprint of
  the whole ad: the pairs of its chunks one after the other, shifted by the start of their chunk, so that two copies
  split differently in chunks still match.

Output: the pairs with `score_min` >= `--min-score` are grouped (connected components of the pairs), and every ad
of a group gets its own row (a group of 2 ads has 2 rows), never two ads mixed on one row:

- the CSV (`--output`): `group_id` (smallest ad id of the group), `group_size`, `ad_id`, `nb_matches` (ads of the
  group it matches directly), its best match in the group (`best_match_ad_id`, which has its own row, with
  `best_score_min`, `best_score_max`, `best_shared_keys`), `matches` (every direct match, `ad_id:score_min`), then the
  data of the ad: duration, number of chunks, brand, sector, category, fragment type, first detection date, live
  occurrences. `score_min` is the share of the pairs of the smaller ad found in the other at a consistent time
  offset; `score_max` the same relative to the bigger ad (low when a short ad is only contained in a longer one,
  e.g. a shared jingle);
- `<csv name>_metabase.txt`: a SQL query to paste in Metabase (database holding the `advertising` schema). The ids
  and matching columns of the rows are written in the query (`VALUES`); it loads the data of each ad: mosaic
  (`advertising.ad_screenshot`, column to display as an image), brand, sector, category, classification status,
  transcript, broadcasts, link to the saved video in the advertising bucket (`lien_sauvegarde`, bucket
  `--advertising-bucket`, default `ADVERTISING_BUCKET_NAME` or `advertising-detection`) and Mediatree link of the
  last broadcast (`lien_mediatree`, valid for recent broadcasts only).
- `<csv name>_metabase_multimarques.txt`: a second Metabase query, to inspect the matches between brands. Only the
  groups whose ads have at least two different brands (`brand_key`, ads without brand not counted), still one row per
  ad. The `match_*` columns only describe the best match of the ad with an ad of another brand: scores, shared keys,
  acoustic pre-filter, duration difference, and the mosaic, transcript and saved video of that match, next to the
  ones of the ad. They are empty when the ad has no brand, or is linked to the other brands only through ads of its
  own brand. The header of the query lists how to read the scores. The CSV has the same columns (`group_brands`,
  `is_multibrand_group`, `other_brand_*`). With `--blocking brand` (default), the ads of different brands are never
  compared: these groups only come from `--blocking none`.

Thresholds are not calibrated: start on a brand (`--brand`) or a duration range (`--min-duration`, `--max-duration`)
or a sample (`--max-ads`), check pairs at several score levels, then widen. Other filters: `--date-from`/`--date-to`
(first detection), `--fragment-types` (default `advertising AD`), `--min-occurrences` (default 1: skips the pieces
of merged ads and the junk, whose occurrences are deleted), `--include-unbranded`.

```bash
poetry run python -m quotaclimat.data_ingestion.advertising.s04_consolidation.duplicate_ads.find_duplicates \
    --min-duration 19 --max-duration 21 --output duplicates.csv
```

In a Scaleway job (no access to the local files), `--output-bucket` also uploads the CSV and the Metabase queries to
`<bucket>/<--output-prefix, default duplicate_ads>/<file name>`, with the `BUCKET`/`BUCKET_SECRET` credentials of the
advertising jobs. For instance, as the startup command of a run of the `advertising_exportation` job, which has these
credentials and `POSTGRES_*`:

```bash
python -m quotaclimat.data_ingestion.advertising.s04_consolidation.duplicate_ads.find_duplicates \
    --min-duration 19 --max-duration 21 --output-bucket test-advertising-detection
```
