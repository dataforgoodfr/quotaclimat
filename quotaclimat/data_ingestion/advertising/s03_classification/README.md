# Advertising classification pipeline

Classifies the ads detected and exported by s01/s02 (videos in the advertising bucket, rows in `advertising.ad`):

- `transcription/`: what is said, by a speech-to-text model (`audio/`), and the text shown on screen, read frame by frame by an OCR model (`ocr/`).
- `sector_and_brand/`: what is advertised (content type, sector, sub-category, brand), with the brand dictionary deciding whenever it knows the brand. Each stage, the statuses and the reference data are described in [sector_and_brand/README.md](sector_and_brand/README.md).
- `environmental_content/`: work in progress.

Every stage handles the same pool of ads: not soft deleted (`s04_consolidation`), and broadcast since `CLASSIFICATION_START_DATE` (at least one occurrence on or after it). Among them, it only picks up the rows in its input state, so re-running is safe and resumes after an interruption. Each result records a stamp (stage, model, sampling settings, prompt/taxonomy/dictionary versions, `run_id`) to trace it back to its inputs.

## Stages

Run with `poetry run python -m quotaclimat.data_ingestion.advertising.s03_classification.<module>`, in this order.

| Module | Reads | Writes |
|---|---|---|
| `transcription.audio.asr` | `fragment_type != 'no_data'`, `transcript IS NULL` | `transcript`; `''` for a silent ad or one without an audio track, without a request. First, as the prompts of the next stages read it |
| `sector_and_brand.e1_classify_ad` | `prediction_status IS NULL` | `fragment_type`, `predicted_sector`, `prediction_status` = `ad_type_done` / `not_ad` / `ad_type_parse_error` / `video_decode_error`; `fragment_type='no_data'` when the video is missing |
| `sector_and_brand.e2_ocr` (runs `transcription.ocr.ocr`) | `fragment_type='AD'`, `ocr_transcript IS NULL` | `ocr_transcript` (one request per frame). After `e1_classify_ad`, which sets `fragment_type='AD'`, and before `e5_canonicalise_brand`, which reads it |
| `sector_and_brand.e3_dict_match` | `ad_type_done` | `prediction_status` = `dict_tier1` / `dict_tier2` / `dict_tier3` / `dict_tier2_no_kw` / `dict_tier3_no_kw` / `dict_miss` (no model) |
| `sector_and_brand.e4_classify_subcat` | `dict_tier2_no_kw` / `dict_tier3_no_kw` / `dict_miss` | `subcat_done` / `subcat_parse_error` / `video_decode_error` / `no_sector_found`; an ad whose video is missing stays pending, for when the exportation (s02) exports it again |
| `sector_and_brand.e5_canonicalise_brand` | ads with a brand read by `e1_classify_ad` that the dictionary didn't spell, and no `predicted_brand` | `predicted_brand`; audits in `data/brand_*.csv` and in review tabs of the sheet |

## How a stage runs

The stages calling a model share `utils/llm_runner.py`. A stage selects the ids of the ads to process and gives `run_stage` a `process_one(ad_id)`: it loads the ad, calls the model (`call_vlm`, `call_llm` for OCR, the transcription endpoint for ASR) and returns a `StageResult`, a status and the statement storing the result. `run_stage` runs it in `workers` threads, and saves each result in its own transaction as soon as it comes back. The progress bar counts the results by status (`ok`, `invalid_answer`, `left_pending`, `no_video`...).

An ad whose handling fails (an unexpected error, not a model answer) is logged and left for the next run. Connection errors, timeouts, 429 and 5xx are retried once by the `openai` client. OCR reads the frames of an ad one after the other, and writes the ad once all its frames are read.

## Installation

The dependencies are in the optional Poetry group `advertising`:

```bash
poetry install --with advertising
```

## Environment variables

The code reads `os.environ`, like the other advertising stages. For manual runs, copy `.advertising.env.example` (repo root) to `.advertising.env` and fill it in: `s03_classification/__init__.py` loads it before any module reads the environment. A variable already set (shell, Kestra, Scaleway job) wins over the file.

| Variable | Used by |
|---|---|
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT` | every stage (`connect_to_db`) |
| `CLASSIFICATION_START_DATE` (default `2026-01-01`) | every stage; only the ads with an occurrence on or after this date. The others keep what they already have |
| `BUCKET`, `BUCKET_SECRET`, `ADVERTISING_BUCKET_NAME`, `ADVERTISING_BUCKET_REGION` (default `fr-par`) | stages reading videos (presigned URLs, or the video itself for ASR and OCR) |
| `ASR_API_URL`, `ASR_API_TOKEN`, `ASR_MODEL_NAME` | `transcription.audio.asr` |
| `ASR_LANGUAGE` (optional, e.g. `fr`) | same; the language of the audio. Unset: the model detects it for each ad |
| `OCR_API_URL`, `OCR_API_TOKEN`, `OCR_MODEL_NAME` | `e2_ocr` (PaddleOCR-VL) |
| `VLM_API_URL`, `VLM_API_TOKEN`, `VLM_MODEL_NAME` | `e1_classify_ad`, `e4_classify_subcat` |
| `VLM_TEMPERATURE` (1.0), `VLM_TOP_P` (0.9), `VLM_TOP_K` (20), `VLM_PRESENCE_PENALTY` (0.0), `VLM_REPETITION_PENALTY` (1.0), `VLM_REASONING_EFFORT` (`low`), `VLM_ENABLE_THINKING` (`1`, or `0`) | same; the sampling of the model (see [Model endpoints](#model-endpoints)), the default in brackets when unset or empty |
| `ADVERTISING_SPREADSHEET_ID` | reference data sync, `e5_canonicalise_brand` |
| `ADVERTISING_GOOGLE_SERVICE_ACCOUNT_JSON` | same; the service account key: the path of its JSON file, relative to the project root (`secrets/<key>.json`), or the JSON itself, inline (as Kestra passes it; in `.advertising.env`, on one line between single quotes). Unset or empty: `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` (the `s05_enrichment` account). The account must be an editor of the spreadsheet: `e5_canonicalise_brand` writes review tabs |

Per-run options, to pass on the command line (e.g. `LIMIT=5 poetry run python -m ...`):

| Variable | Stages | Effect |
|---|---|---|
| `LIMIT` | all but `e5_canonicalise_brand` | at most this many ads |
| `CHANNEL` | `e1_classify_ad` | only ads broadcast on this channel |
| `RERUN=1` | `asr`, `e2_ocr` | redo the ads already done |
| `FPS` | `e2_ocr` | frames read per second of video, default 1 |
| `FULL_RERUN=1`, `DRY_RUN=1` | `e3_dict_match` | rerun the dictionary on the ads already matched; see [sector_and_brand/README.md](sector_and_brand/README.md) |
| `RECANONICALISE=1`, `SHEET_REVIEW=0` | `e5_canonicalise_brand` | see [sector_and_brand/README.md](sector_and_brand/README.md) |
| `LOGLEVEL` | all | default `INFO` |

## Reference data

The sector taxonomy and the brand dictionary are kept in the Google Sheet and snapshotted locally. How to sync them, and how to apply a dictionary change to past ads: [sector_and_brand/README.md](sector_and_brand/README.md#reference-data).

## Model endpoints

All three endpoints are OpenAI-compatible: their URL includes the `/v1` suffix (e.g. `https://endpoint.example.com/v1`), as the `openai` SDK appends `/chat/completions` or `/audio/transcriptions` to it.

**Vision LLM** (`VLM_*`, used by `e1_classify_ad` and `e4_classify_subcat`). The stages send the video itself, a presigned URL of the bucket, so the model must accept video URLs natively (Qwen models):

```python
{"type": "video_url", "video_url": {"url": presigned_s3_url}}
```

The model fetches and decodes the video itself. The pipeline only reads the start of the video, to check it has at least `MIN_VIDEO_FRAMES` (2) readable frames: Qwen3-VL rejects (400) a video with a single one. A video with fewer is sent as its frames, as images, or not at all when it has none, and the model then only gets the transcript (`video_content` in `utils/llm_runner.py`). To switch to a model that doesn't take videos, `video_content` would have to send sampled frames as images for every video (`utils/frame_splitting.py` already splits a video into frames).

The model in production is **Qwen3.8-27B in FP8**, which needs a GPU with at least 48 GB of VRAM.

The sampling parameters (`VLM_TEMPERATURE`...) shape the answer as much as the prompts (`sector_and_brand/prompts/*.yaml`): with thinking, keep the temperature at 1.0 and the presence penalty at 0.0; without, set the temperature to 0.0. Their defaults are at the top of `utils/llm_runner.py`, and the values of a run are in the stamp of each result (`versions.sampling`).

**Speech-to-text** (`ASR_*`): a model served with `/audio/transcriptions` (e.g. Whisper on vLLM). The audio is sent as a 16 kHz mono WAV; long audio is cut by the server.

**OCR** (`OCR_*`): PaddleOCR-VL through `/chat/completions`, one frame at a time, as an image followed by the text `OCR:` and no system prompt.
