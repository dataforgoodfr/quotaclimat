# Stage 4: consolidation

Corrections of the detection, made after the classification (they use its brands), one sub-folder per
step, each with its own entry point:

- `e01_merge_split_ads`: unsupervised merge of the ads split in several pieces by the detection
  (`entrypoints/advertising_merge_split_ads.sh`).

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
