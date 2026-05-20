# Advertising classification pipeline

Five-stage pipeline that takes raw ads from the S3 bucket, transcribes them and classifies them by the sector and sub-category of the advertised product, while also extracting the product brand and product name.

The pipeline combines a rules-based dictionary lookup and predictions from a vision LLM. The dictionary is the source of truth when a known brand is found, while the LLM fills in the gaps for unknown brands and ambiguous cases.

Each stage reads pending rows filtered by `prediction_status` in the `Ad` table, processes them, and writes a new status that is either final or consumed by a downstream stage.

## Stages

| # | Script                      | Reads (status) | Writes (status) | Notes                                                   |
|---|-----------------------------|---|---|---------------------------------------------------------|
| 1 | `e01_transcribe.py`         | `transcript IS NULL` | sets `transcript`, or `fragment_type='no_data'` | Transcription via Qwen ASR                              |
| 2 | `e02_classify_ad.py`        | `prediction_status IS NULL` | `ad_type_done` / `ad_type_parse_error` / `no_data` | LLM call to get content type, brand, product and sector |
| 3 | `e03_dict_match.py`         | `ad_type_done` | `dict_tier1` / `dict_tier2` / `dict_tier2_no_kw` / `dict_tier3` / `dict_tier3_no_kw` / `dict_miss` | Dictionary lookup, no LLM                               |
| 4 | `e04_classify_subcat.py`    | `dict_tier2_no_kw` / `dict_tier3_no_kw` / `dict_miss` | `subcat_done` / `subcat_parse_error` / `no_data` | LLM call to get the sub-cat                             |
| 5 | `e05_canonicalise_brand.py` | `predicted_brand IS NULL` & has stage-2 brand | populates `predicted_brand` | Fuzzy matching of LLM-extracted brands                  |

Stages 1-4 must run in order. Stage 5 can run any time after stage 2 (it just fills in canonical forms of the brand names the dictionary didn't already provide).

## Quick start

Each stage is a script with a `__main__` block. Run them in order:

```bash
python -m quotaclimat.data_ingestion.advertising.s03_classification.pipeline.e01_transcribe
python -m quotaclimat.data_ingestion.advertising.s03_classification.pipeline.e02_classify_ad
python -m quotaclimat.data_ingestion.advertising.s03_classification.pipeline.e03_dict_match
python -m quotaclimat.data_ingestion.advertising.s03_classification.pipeline.e04_classify_subcat
python -m quotaclimat.data_ingestion.advertising.s03_classification.pipeline.eO5_canonicalise_brand
```

Every stage is idempotent, it picks up only rows in the right input status, so re-running is safe and can be resumed after an interruption.

### Environment variables

All env vars below are read first from the process environment, they can also be supplied via a `.env` file at the project root (each stage calls `load_dotenv()` on startup).

**Database & S3** (required for all stages):

The pipeline uses the `connect_to_db` function from `quotaclimat.db.postgres` to connect to the database, with the 
same environment variables. 

**Model settings**:

The VLM (stages 2 and 4) and ASR (stage 1) endpoints are configured independently via prefixed env vars, validated by `pydantic-settings` (`VLMSettings` and `ASRSettings` in `settings.py`).

| Variable | Stage | Description |
|---|---|---|
| `VLM_MODEL_NAME` | 2, 4 | Model identifier passed in the OpenAI request |
| `VLM_API_URL` | 2, 4 | OpenAI-compatible endpoint (must include `/v1`) |
| `VLM_API_TOKEN` | 2, 4 | Bearer token for the endpoint |
| `ASR_MODEL_NAME` | 1 | ASR model identifier |
| `ASR_API_URL` | 1 | ASR endpoint (must include `/v1`) |
| `ASR_API_TOKEN` | 1 | Bearer token for the endpoint |

**`.env` template**:

```env
# Postgres
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_PASSWORD=

# S3
BUCKET=
BUCKET_SECRET=
ADVERTISING_BUCKET_NAME=
# Optional overrides
# ADVERTISING_BUCKET_REGION=fr-par
# S3_ENDPOINT_URL=

# VLM endpoint (stages 2 & 4)
VLM_MODEL_NAME=qwen3.5-27B-fp8
VLM_API_URL=https://your-vlm-endpoint.example.com/v1
VLM_API_TOKEN=xxx-...

# ASR endpoint (stage 1)
ASR_MODEL_NAME=
ASR_API_URL=https://your-asr-endpoint.example.com/v1
ASR_API_TOKEN=xxx-...
```

**Stage runtime variables** (per stage, all optional):

| Variable | Stages | Default                         | Description |
|---|---|---------------------------------|---|
| `WORKERS` | 1, 2, 4 | 15 / 30                         | Threadpool size for that stage |
| `BATCH_SIZE` | all | 50–1000                         | Rows per DB flush |
| `LIMIT` | all | unset (= no limit)              | Cap rows pulled this run, useful for smoke tests |
| `TTL` | 1, 2, 4 | 300–1200                        | Seconds the S3 presigned URL stays valid |
| `LLM_TIMEOUT` / `ASR_TIMEOUT` | 1, 2, 4 | 300                             | Per-request timeout |
| `RETRIES` | 1, 2, 4 | 0 / 1                           | Transport-error retries (parse errors are never retried) |
| `PROMPT_PATH` | 2, 4 | `.../prompts/ad_type.yaml` etc. | Path to the YAML prompt file |
| `AUDIT_PATH` | 5 | `data/brand_merges.csv`         | CSV dump of brand merges for human review |

### LLM endpoint requirements

The pipeline expects an **OpenAI-compatible chat completions endpoint**. The `API_URL` must include the `/v1` suffix (ex: `https://endpoint.example.com/v1`), since the OpenAI SDK appends `/chat/completions` to it.

Currently the pipeline is only compatible with vision-language models that **natively accept video URLs as message content (ex: Qwen models)**. Stages 2 and 4 send video to the model as:

```python
{"type": "video_url", "video_url": {"url": presigned_s3_url}}
```

This lets the model fetch and process the video directly from S3 (so we don't have to download or decode video frames in the pipelin).

The model in production is **Qwen3.5-27B in FP8**, which requires a **minimum 48 GB VRAM** GPU.

If you want to switch to a model that doesn't natively process videos, you'll need to add a video-processing layer that downloads the video, samples frames, and sends them as a list of images instead.

## Architecture

### Shared threaded runner

Stages 1, 2, and 4 share an execution shape: a thread pool runs per-ad work concurrently, results are batched, and batches are flushed to the DB.

```
[ ad_ids ] ─┬─► process_one(ad_id) ─► StageResult ─┐
            ├─► process_one(ad_id) ─► StageResult ─┤
            └─► process_one(ad_id) ─► StageResult ─┴─► buffer ─► flush(batch) ─► DB
              (ThreadPoolExecutor, N workers)        (every BATCH_SIZE results)
```

Each stage provides three things:

- **`ad_ids`**: the list of pending rows fetched up front by that stage's `select_pending`.
- **`process_one(ad_id) => StageResult`**: does the per-ad work (presign the S3 URL, call the LLM, parse the response). This runs inside a worker thread.
- **`flush(results)`**: receives a list of `StageResult` and writes them to Postgres in one bulk update.

`run_stage` ties them together, creates a `tqdm` progress bar with live status counts, and on `KeyboardInterrupt` cancels pending tasks and flushes whatever is already in the buffer before exiting. Stage 3 (`e03_dict_match.py`) doesn't use the threaded runner because it's CPU-only and pure Python.

### The `prediction` column 

Every LLM and dictionary stage appends a structured entry to the `prediction` JSON column on the row, tagged with a `stage` key (`ad_type`, `dict_match`, `subcat`). Each stage strips only its own prior entry before appending, so the column accumulates one entry per stage and re-running a stage replaces that stage's entry rather than duplicating it. After a full pipeline run, the column contains the full decision trail across stages.

## Stage details

### Stage 1: `e01_transcribe.py`

Pulls every ad with `transcript IS NULL` and a `fragment_type` not yet marked `no_data`. For each, presigns an `.mp3` URL from S3 and sends it to a Qwen ASR endpoint as an `audio_url` chat message. The text is parsed via `qwen_asr.parse_asr_output` and written to `Ad.transcript`.

If the S3 object is missing (FileNotFoundError), the row is marked `fragment_type='no_data'` and skipped by all downstream stages.

### Stages 2 & 4: VLM classification (`e02_classify_ad.py`, `e04_classify_subcat.py`)

Both stages share the same shape: presign a video URL, build a chat message containing the video and a Jinja-rendered user prompt, call the LLM with a forced JSON schema, parse and save.

**Prompts are loaded from YAML** in the `prompts/` folder via `load_prompt(path)`. Each YAML has a `system_prompt` and a `user_prompt` field. The user prompt is a Jinja2 template. Stage 2 fills in `{{ transcript }}`, stage 4 fills in `{{ sector_code }}`, `{{ sector_label }}`, `{{ ad_subject }}`, `{{ sub_cat_list }}`, and `{{ transcript }}`. Stage 2's *system* prompt also has `{{ sector_list }}` substituted with the list of valid sectors before being sent.

**Forced JSON schema**: both stages use OpenAI's structured-output mode. Stage 2 builds its schema dynamically with `build_ad_classification_model(sector_codes)` (in `ad_classification_schema.py`), which produces a Pydantic model where `sector_code` is a `Literal[...]` over the actual codes from the taxonomy. The model's JSON schema is then handed to the LLM as a `response_format`:

```python
response_format = {
    "type": "json_schema",
    "json_schema": {"name": schema_name, "schema": schema},
}
```

The endpoint enforces this schema during generation (guided JSON), so the response is guaranteed to parse and to use only valid sector codes. The Pydantic model also encodes cross-field rules: Part B fields (sector, brand, product) must be null unless `content_type == "AD"`, `cut_off_point` only allowed when `completeness == "CUT_OFF"`, etc.

Stage 4 doesn't pass a schema (it uses `{"type": "json_object"}`) because the sub-category vocabulary is sector-dependent and constraining it would require a per-sector schema build per request.

**Retry policy**: `chat_completion_to_dict` retries transport errors with exponential backoff but doesn't retry JSONDecodeErrors. A parse failure marks the row `*_parse_error` so it can be inspected.

### Stage 3: `e03_dict_match.py` + `dictionary/matcher.py`

Pure Python, no LLM. Loads `reference_data/dictionnaire_marques_secteurs.xlsx` once into three tiers:

- **Tier 1**: brand alone determines `(sector, sub-category)`. Hits become `dict_tier1`.
- **Tier 2**: brand determines sector; product keywords (matched against the LLM's `product_name` plus the transcript) pick the sub-category. Keyword hit => `dict_tier2`. No keyword hit => `dict_tier2_no_kw` (sector kept, sub-category sent to stage 4).
- **Tier 3**: ambiguous brands that span multiple sectors. Only resolves with a keyword hit (`dict_tier3`); without one (`dict_tier3_no_kw`), nothing is committed and the row falls through to stage 4 with the LLM's sector prediction.

Brand keys are normalised (accents stripped, lowercased, punctuation removed) and de-spaced before lookup, so `Coca-Cola`, `coca cola`, and `cocacola` all become the same key.

### Stage 5: `e05_canonicalise_brand.py`

Picks up rows where stage 2 extracted a brand but no canonical form was written (i.e. anything not resolved by the dictionary). Three passes:

1. **Dictionary spelling override**: if the normalised brand key matches a known dictionary entry, use the dictionary's spelling.
2. **Spacing collapse**: `coca cola` and `cocacola` merge to one form, picking the most frequent / spaced / non-noise variant.
3. **Sector-aware fuzzy matching**: `thefuzz.ratio` against existing canonicals, with a tighter threshold for short brand names (where one-character differences matter more) and a sector-shared constraint to prevent `Orange` (telecom) merging with `Orange` (juice).

A `data/brand_merges.csv` audit file is written on every run so merges can be reviewed (and possibly tune the fuzzy matching threshold).

## Taxonomy and dictionary

The taxonomy comes from two Excel files in `reference_data/`:

- `classification_pub_ome.xlsx`: sector list and sub-category list per sector. Loaded by `taxonomy.py` at import time. Provides `SECTOR_LIST` (rendered for the stage 2 prompt) and `get_subcategory_list(sector_code)` (rendered for stage 4).
- `dictionnaire_marques_secteurs.xlsx`: brand => sector/sub-category mappings across the three tiers. Loaded by `BrandDictionary` in `matcher.py`.

=> Weak spot that could be improved in the future as the brand dictionary is a living file that may be updated over time.

## Status reference

Every value `prediction_status` can take:

| Status | Set by | Meaning                                                 |
|---|---|---------------------------------------------------------|
| `NULL` | initial | Not yet classified                                      |
| `ad_type_done` | stage 2 | LLM classification succeeded                            |
| `ad_type_parse_error` | stage 2 | LLM returned malformed JSON; needs inspection           |
| `dict_tier1` | stage 3 | brand alone resolved sector + sub-cat                   |
| `dict_tier2` | stage 3 | Brand resolved sector, keyword resolved sub-cat         |
| `dict_tier2_no_kw` | stage 3 | Brand resolved sector, sub-cat goes to LLM              |
| `dict_tier3` | stage 3 | ambiguous brand resolved by keyword hit                 |
| `dict_tier3_no_kw` | stage 3 | Ambiguous brand, no keyword hit, full LLM fallback      |
| `dict_miss` | stage 3 | No dictionary hit; LLM sector kept, sub-cat goes to LLM |
| `subcat_done` | stage 4 | LLM sub-category classification succeeded               |
| `subcat_parse_error` | stage 4 | LLM returned malformed JSON; needs inspection           |
| `no_data` | stages 1, 2, 4 | S3 vidéo missing (also sets `fragment_type='no_data'`)  |

**prediction_status vs prediction_method**

- `prediction_status`= where the row is *now*. Advances as stages run, and is the field each stage's `select_pending` query filters on to pick up work. Also captures parse errors (`ad_type_parse_error`, `subcat_parse_error`).
- `prediction_method` = how the final answer was *produced*. Records the combination of stages that contributed to the result (`dict_tier1`, `dict_tier2+llm_subcat`, `llm_subcat`, etc.). Set to `None` on parse errors since no answer was produced.