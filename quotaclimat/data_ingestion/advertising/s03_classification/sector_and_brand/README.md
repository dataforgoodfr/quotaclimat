# Sector and brand classification

Five stages that tell what each ad advertises: its content type, sector, sub-category and brand. They run after the speech-to-text (`transcription/audio`), which gives them the transcript. The second one, `e2_ocr`, runs the OCR (`transcription/ocr`) at the point where it's needed: after `e1_classify_ad`, which finds the ads it reads, and before `e5_canonicalise_brand`, which uses its text.

The pipeline combines a rules-based dictionary lookup and a vision LLM. The dictionary is the source of truth when it knows the brand; the model fills in the gaps for unknown brands and ambiguous cases.

Each stage selects the ads of the pool (not soft deleted, broadcast since `CLASSIFICATION_START_DATE`, see [../README.md](../README.md)) by their `prediction_status`, processes them, and writes a new status that is either final or picked up by a downstream stage. How a stage runs, the environment variables and the installation: [../README.md](../README.md).

## Stages

| Module | Reads | Writes | Model |
|---|---|---|---|
| `e1_classify_ad` | `prediction_status IS NULL` | `ad_type_done` / `not_ad` / `ad_type_parse_error` / `video_decode_error`; `fragment_type`, `predicted_sector` | VLM |
| `e2_ocr` | `fragment_type='AD'`, `ocr_transcript IS NULL` | `ocr_transcript` (no status) | OCR |
| `e3_dict_match` | `ad_type_done` | `dict_tier1` / `dict_tier2` / `dict_tier3` / `dict_tier2_no_kw` / `dict_tier3_no_kw` / `dict_miss`; sector, sub-category and brand when the dictionary knows them | none |
| `e4_classify_subcat` | `dict_tier2_no_kw` / `dict_tier3_no_kw` / `dict_miss` | `subcat_done` / `subcat_parse_error` / `video_decode_error` / `no_sector_found`; `predicted_product_category` | VLM |
| `e5_canonicalise_brand` | ads with a brand that the dictionary didn't spell, and no `predicted_brand` | `predicted_brand` | none |

Run each with `poetry run python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.<module>`.

### `e1_classify_ad`: content type, sector, brand

- **Selects** the ads with `prediction_status IS NULL` (and not `fragment_type='no_data'`), the newest first; `CHANNEL` keeps those broadcast on one channel.
- **Sends** the video (a presigned URL) and the prompt `prompts/ad_type.yaml`: its system prompt lists the sectors of the taxonomy (`{{ sector_list }}`), its user prompt gives the transcript (`{{ transcript }}`).
- **The answer** must fit `build_ad_classification_model(taxonomy.SECTORS)` (`classification_schema.py`). The schema is sent to the endpoint, which constrains the answer to it (guided decoding), and the answer is then validated with pydantic: `sector_code` is one of the taxonomy's sectors; sector, brand and product are emptied unless `content_type` is `AD`; `cut_off_point` only when `completeness` is `CUT_OFF`. Guided decoding doesn't rule out an invalid answer (truncated, or broken by the thinking), hence `ad_type_parse_error`.
- **Writes**, in `prediction`, one entry with the stamp of the run, the answer and the error, and:

| Outcome | Writes |
|---|---|
| An advertising | `ad_type_done`, `fragment_type='AD'`, `predicted_sector`, `prediction_confidence` (mean of the two confidences, LOW = 1 to HIGH = 3) |
| Another content type | `not_ad`, `fragment_type` = that type (`BUMPER`, `CHANNEL_PROMO`, `SPONSORSHIP`, `OTHER`) |
| An invalid answer | `ad_type_parse_error`, with the answer (its JSON, or the start of its text) kept for inspection |
| A video the server can't read | `video_decode_error` |
| No video in the bucket | `fragment_type='no_data'`: never selected again |
| A timeout, 429 or 5xx | nothing: left for the next run |

### `e2_ocr`: the text on screen

Runs `transcription/ocr/ocr.py` (same options: `LIMIT`, `RERUN=1`, `FPS`). It reads the frames of the ads `e1_classify_ad` found (`fragment_type='AD'`) and writes their on-screen text in `ocr_transcript`; it doesn't change the status. `e5_canonicalise_brand` uses that text to give a brand read on screen the dictionary's spelling.

### `e3_dict_match`: the brand dictionary

No model. It looks up the brand read by `e1_classify_ad` in the [dictionary](#reference-data), whose brands are in three tiers:

| Tier | The brand gives | Status |
|---|---|---|
| 1 | the sector and the sub-category | `dict_tier1` |
| 2 | the sector; a product keyword picks the sub-category | `dict_tier2`, or `dict_tier2_no_kw` without a keyword |
| 3 | nothing on its own (it spans several sectors); only a keyword decides both | `dict_tier3`, or `dict_tier3_no_kw` without a keyword |
| — | unknown brand | `dict_miss` |

- **Brands are compared as keys**: lowercase, without accents, punctuation or spaces, `œ`/`æ` spelled out and subscripts as digits (`advertising/tools/normalize.py`). So `Coca-Cola`, `coca cola` and `COCACOLA` are the same brand. An alias (`alias_marque`) is matched as the brand itself.
- **Writes** `prediction_status` and `prediction_method` = the status above; `predicted_sector` (the dictionary's, or the one of `e1_classify_ad` for `dict_tier3_no_kw` and `dict_miss`); `predicted_product_category` when the dictionary decides it; `predicted_brand` = the dictionary's spelling, except for `dict_tier3_no_kw` and `dict_miss`, left to `e5_canonicalise_brand`.
- `dict_tier2_no_kw`, `dict_tier3_no_kw` and `dict_miss` go to `e4_classify_subcat` for their sub-category.
- **`FULL_RERUN=1`**: after a dictionary change, reruns it over every ad of the pool it already matched, and the new ones. An ad whose dictionary answer (method, sector, sub-category, brand) didn't change is left as it is, with its sub-category and brand. Any other is rewritten as on a first match, so `e4_classify_subcat` and `e5_canonicalise_brand` pick it up again when the dictionary doesn't decide. The log counts the changes (`dict_tier3 -> dict_miss`...).
- **`DRY_RUN=1`** runs everything, then rolls the writes back.

### `e4_classify_subcat`: sub-category

- **First**, the pending ads without a sector, or with a sector that has no sub-categories in the taxonomy, get `no_sector_found`.
- **Selects** the ads at `dict_tier2_no_kw`, `dict_tier3_no_kw` or `dict_miss`.
- **Sends** the video and the prompt `prompts/subcat.yaml` (`{{ sector_code }}`, `{{ sector_label }}`, `{{ ad_subject }}` read by `e1_classify_ad`, `{{ sub_cat_list }}` of the sector, `{{ transcript }}`). The answer schema is built for the ad's sector (`build_subcategory_model`), so the model can only answer a sub-category of that sector.
- **Writes**, adding its entry to `prediction` after those of the previous stages:

| Outcome | Writes |
|---|---|
| An answer | `subcat_done`, `predicted_product_category`, `prediction_method` = `dict_tier2+llm_subcat` (from `dict_tier2_no_kw`) or `llm_subcat` |
| An invalid answer | `subcat_parse_error`, the answer kept for inspection |
| A video the server can't read | `video_decode_error` |
| No video in the bucket | nothing: left pending, for when the exportation (s02) exports it again |

### `e5_canonicalise_brand`: one spelling per brand

- **Selects** the ads with a brand read by `e1_classify_ad` that the dictionary didn't spell (not `dict_tier1`, `dict_tier2`, `dict_tier2_no_kw`, `dict_tier3`) and no `predicted_brand`. With **`RECANONICALISE=1`**, all of them, whatever their `predicted_brand`. Only the ads of the pool are compared: a brand only broadcast before `CLASSIFICATION_START_DATE` is neither in the review tabs nor counted for a group's spelling. Use it after a dictionary change (and `FULL_RERUN`), new verdicts or a threshold change: without it, only the new brands are compared, and only among themselves.
- **The spelling of each brand key**, in order:
  1. **The dictionary's**, when the key is close to a dictionary brand or alias (score ≥ 92, at most 2 letters apart in length), or when that brand is read on screen by the OCR and the key is recognisably it (≥ 70; ≥ 86 for a dictionary name of 5 letters or fewer).
  2. **Otherwise, the clustering**: a key merges into the closest brand already kept that shares a sector with it (score ≥ 88; ≥ 92 for a key of 5 letters or fewer, where one letter matters more). The shared sector keeps Orange the telecom company apart from an orange juice.
  3. **Reviewers' verdicts** from the sheet: `merge` forces two brands together, `split` keeps them apart.

  A group of merged brands takes the spelling of its first one: dictionary spellings first, then the brand with the most ads. A brand the dictionary doesn't spell is written as its key's readable form: lowercase, without punctuation (`gerbly`).
- **Writes** `predicted_brand`, for the ads of the run only.
- **Audits**, rewritten on each run, as CSV files in `data/` and, unless `SHEET_REVIEW=0`, in two tabs of the sheet with a `verdict` column:
  - `brand_merges_audit` (`brand_merges.csv`): the merges a verdict could undo, with how each was decided (`pass`: `dict_override`, `dict_ocr`, `fuzzy`, `allowlist` for a `merge` verdict, `spacing`). Left out: the same key written differently (`gerble` → `Gerblé`), and a brand the dictionary spells by its own name or alias (`gerbe` → `Gerblé`).
  - `brand_near_misses_audit` (`brand_merge_near_misses.csv`): the pairs kept apart that end up with different spellings: `below_threshold` (a shared sector, at most 8 points under the threshold) or `sector_blocked` (close enough, but no shared sector).
  - `brand_dict_gaps.csv` (no tab): brands close to a dictionary entry (86 to 91) that weren't given its spelling, likely missing aliases. `verdict`: `no_ocr`, `screen_backs_variant` (the screen shows the variant's own spelling) or `unconfirmed`.

**Verdicts.** Pick `merge` or `split` in the `verdict` column of the two tabs. The next run collects them, before rewriting the tabs, into the `brand_overrides` tab, which keeps them from one run to the next. Its `key_a` and `key_b` columns hold brand keys (lowercase, no spaces). Two limits:
- A verdict only joins brands among the ads of the run. The ads spelled by the dictionary are not among them, so a `merge` towards a dictionary brand does nothing: to give a misspelling the dictionary's spelling, add it as an alias of that brand.
- A `split` doesn't undo a dictionary spelling given by name (score ≥ 92, `dict_override`); it does undo one given by the screen (`dict_ocr`) and the clustering.

## Reference data

The taxonomy and the brand dictionary are kept in the Google Sheet (`ADVERTISING_SPREADSHEET_ID`):

| Tabs | Loaded by | Content |
|---|---|---|
| `secteurs`, `catégories` | `reference_data/taxonomy.py` | the sectors, and the sub-categories of each sector, with their labels and descriptions for the prompts |
| `marques_type_1`, `marques_type_2`, `marques_type_3` | `reference_data/dictionary.py` | one row per brand and rule: `nom_marque`, `secteur`, `catégorie`, `mots_clés_produit`, `alias_marque` |

They are snapshotted locally in `reference_data/*_versions/` (git-ignored). Stages read the latest snapshot and never touch the network, except on a machine without a snapshot, where the first import fetches the sheet. To pick up edits made in the sheet:

```bash
poetry run python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data.taxonomy
poetry run python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data.dictionary
```

A new snapshot is only written when the content changed.

- **`alias_marque`** lists other names of the brand seen in ads (misheard spellings, short forms), separated by `;`: an ad under an alias is matched as the brand itself, with its spelling, sector and sub-category. Variants that differ only by case, accents, punctuation or spaces are already matched without an alias. The sync fails when an alias is another brand's name or the alias of two brands.
- **`mots_clés_produit`** (types 2 and 3, separated by `;`) are looked for in the product name read by `e1_classify_ad`, and in the transcript only when it found no product. When several keywords match, the one with the most words wins, then the longest; when the most specific ones lead to different sub-categories, none decides.

**To apply a dictionary change** (brands or aliases added, corrected or removed) to past ads: sync the dictionary, run `e3_dict_match` with `FULL_RERUN=1` (with `DRY_RUN=1` first, to see how many ads change), then `e4_classify_subcat`, then `e5_canonicalise_brand` with `RECANONICALISE=1`. Ads whose dictionary answer didn't change keep their sub-category and brand, and cost nothing.

## Statuses

Every value of `prediction_status` written by these stages:

| Status | Set by | Meaning | Then |
|---|---|---|---|
| `NULL` | | not classified yet | `e1_classify_ad` |
| `ad_type_done` | `e1_classify_ad` | an advertising, with its sector | `e3_dict_match` |
| `not_ad` | `e1_classify_ad` | not an advertising (`fragment_type` `BUMPER`, `CHANNEL_PROMO`, `SPONSORSHIP`, `OTHER`) | final |
| `ad_type_parse_error` | `e1_classify_ad` | invalid answer, kept for inspection | final |
| `video_decode_error` | `e1_classify_ad`, `e4_classify_subcat` | the server couldn't read the video | final |
| `dict_tier1`, `dict_tier2`, `dict_tier3` | `e3_dict_match` | the dictionary decides the sector and the sub-category | final |
| `dict_tier2_no_kw` | `e3_dict_match` | the dictionary gives the sector and the brand, no keyword decides the sub-category | `e4_classify_subcat` |
| `dict_tier3_no_kw` | `e3_dict_match` | a brand of several sectors, no keyword decides; the sector of `e1_classify_ad` is kept | `e4_classify_subcat` |
| `dict_miss` | `e3_dict_match` | brand not in the dictionary; the sector of `e1_classify_ad` is kept | `e4_classify_subcat` |
| `subcat_done` | `e4_classify_subcat` | sub-category given by the model | final |
| `subcat_parse_error` | `e4_classify_subcat` | invalid answer, kept for inspection | final |
| `no_sector_found` | `e4_classify_subcat` | no sector, or a sector without sub-categories in the taxonomy | final |

`e3_dict_match` with `FULL_RERUN=1` can send an ad at a final status back to `e4_classify_subcat`, when the dictionary's answer for it changed. `e5_canonicalise_brand` doesn't change the status. The database can also hold statuses written outside this folder (e.g. `merge_inherited`, from the merge of split ads).

`analytics.publicites` keeps the ads at `dict_tier1`, `dict_tier2`, `dict_tier3`, `dict_tier2_no_kw`, `dict_tier3_no_kw`, `dict_miss` and `subcat_done`: the last three already have their sector before `e4_classify_subcat` adds the sub-category.

**`prediction_status` vs `prediction_method`**

- `prediction_status` = where the ad is *now*. It advances as the stages run, and is what each stage's `select_pending` filters on to pick up work. It also records parse errors (`ad_type_parse_error`, `subcat_parse_error`).
- `prediction_method` = how the final answer was *produced*: the stages that contributed to it (`dict_tier1`, `dict_tier2+llm_subcat`, `llm_subcat`...). Empty when no answer was produced.

**`prediction`** holds one entry per stage (`ad_type`, `dict_match`, `subcat`), each with the stamp of its run (stage, model, `run_id`, versions of the prompt, taxonomy, dictionary and sampling, and their `config_hash`), the model's answer (`raw_response`) and the error, if any.
