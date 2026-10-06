# RRS — Risk Response System

RRS is a multi-module analysis system built to detect, cluster, and track disinformation narratives in French-language media (TV and radio transcripts). It ingests annotated transcripts from PostgreSQL or S3, applies keyword-dictionary detection and LLM-based clustering pipelines to surface recurring false claims per topic ("subject"), and persists structured results in a dedicated PostgreSQL database.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ DICTIONARIES (rrs/dictionary/)                                  │
│   subjects.py — one entry per topic (e.g. climate, insecurity,  │
│   environmental_health), each with its own keyword list         │
└────────────────────────────┬────────────────────────────────────┘
                             │ upsert_subjects.py / upsert_dictionary.py
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ PostgreSQL (RRS database) — subjects, dictionary tables          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ keyword_detection/                                               │
│   analyse_keywords.py — scans S3 parquet transcripts directly    │
│     with a DuckDB regex built from each subject's dictionary     │
│   import_segments.py — imports already-detected climate          │
│     keyword rows from the quotaclimat DB                         │
│   filter_keywords.py — re-filters those already-detected rows    │
│     against another subject's dictionary (string or lemma match) │
│   import_cases.py — imports annotated cases (Label Studio) for   │
│     the climate subject                                          │
└──────┬─────────────────────┬──────────────────────┬────────────┘
       │                     │                      │
       ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ misinformation_detection/main.py                                 │
│   LLM classification of segments/cases into misinformation risk │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ clustering/main.py                                                │
│   spaCy sentence segmentation → sliding-window chunking          │
│   3-step LLM pipeline (Mistral / Claude Haiku)                   │
│   DB-aware deduplication against active clusters                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ PostgreSQL (RRS database)                                       │
│   subjects · dictionary · segments · cases · clusters           │
│   case_to_clusters                                               │
│   Managed via SQLAlchemy ORM + Alembic migrations               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
rrs/
├── __init__.py
├── README.md
├── database.md                      # Table schema reference
├── Dockerfile
├── Dockerfile.pulsar
├── docker-compose.yml
├── alembic.ini                      # Standalone Alembic config
│
├── alembic/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/                    # Migration files
│
├── dictionary/
│   ├── subjects.py                  # Registry: {subject_name: {keywords, title}}
│   ├── upsert_subjects.py           # Upsert subjects.py entries into the `subjects` table
│   ├── upsert_dictionary.py         # Upsert each subject's keywords into the `dictionary` table
│   └── subject/
│       ├── insecurity.py            # French immigration/insecurity keyword list
│       └── environmental_health.py  # French environmental-health keyword list
│
├── clustering/
│   ├── README.md                    # Clustering-specific docs
│   ├── main.py                      # Runnable entry point (LLM pipeline + DB dedup)
│   ├── steps.py                     # Library: 3-step LLM pipeline building blocks
│   ├── backends.py                  # Library: embedding backends
│   ├── prompts.py                   # LLM prompts per subject
│   ├── cost.py                      # Token cost estimation
│   └── get_data.py                  # PostgreSQL data loader
│
├── keyword_detection/
│   ├── analyse_keywords.py          # S3 parquet keyword detection (any subject, DB-backed dictionary)
│   ├── import_segments.py           # Import already-detected climate segments from quotaclimat DB
│   ├── filter_keywords.py           # Re-filter those segments against another subject's dictionary
│   └── import_cases.py              # Import annotated cases from quotaclimat DB
│
├── misinformation_detection/
│   ├── main.py                      # LLM misinformation classification entry point
│   ├── classifier.py                # Classification logic
│   └── definitions.py               # Per-subject prompts/definitions
│
└── schemas/
    ├── base.py                      # SQLAlchemy declarative base
    └── models.py                    # ORM models for all 6 tables
```

---

## Modules

### Dictionaries — `dictionary/`

Each topic RRS tracks ("subject") is registered in `dictionary/subjects.py` as `{"keywords": [...], "title": "..."}`. A subject's keyword list is a Python list of `{"keyword": str, "high_risk_false_positive": bool, "validated": bool}` dicts (see `dictionary/subject/insecurity.py` and `dictionary/subject/environmental_health.py`). `high_risk_false_positive` keywords are excluded from a match rather than counted; `validated: False` keywords are only counted once enough of them co-occur (see `analyse_keywords.py`'s `NON_VALIDATED_KEYWORD_THRESHOLD`).

- **`upsert_subjects.py`** — upserts every `subjects.py` entry into the `subjects` table, deriving a stable `subject_id` from the subject name.
- **`upsert_dictionary.py`** — upserts every subject's keywords into the `dictionary` table (tagged with that subject's `subject_id`), and deletes rows no longer present in `subjects.py`.

Run `upsert_subjects` before `upsert_dictionary` (the latter's `subject_id` foreign key requires the row to exist).

---

### `keyword_detection/analyse_keywords.py` — Keyword Detection from Raw Transcripts

Scans S3-hosted parquet transcripts with DuckDB, matching a regex built from each subject's dictionary (loaded from the `dictionary`/`subjects` tables, all subjects except `climate` by default). Segments with at least one validated match — and no high-risk-false-positive match — are upserted into the `segments` table, one row per `(segment_id, subject_id)`.

**Required environment variables:** `BUCKET`, `BUCKET_SECRET`, `BUCKET_NAME` (Scaleway S3 credentials).

**Key options:** `--subject` (restrict to one subject), `--channel`, `--start-date`/`--end-date`/`--days-prior`.

---

### `keyword_detection/import_segments.py` — Import Climate Segments

Climate keyword detection already runs as part of the main quotaclimat pipeline (Aho-Corasick over `THEME_KEYWORDS`) and is stored per-transcript in the quotaclimat `keywords` table's `keywords_with_timestamp` column. This script reads those rows (via DuckDB `ATTACH` on the quotaclimat Postgres DB, `POSTGRES_*` env vars) and upserts them into the RRS `segments` table under the `climate` subject.

**Key options:** `--start-date`/`--end-date` (defaults to everything since the most recent `climate` segment already in the RRS DB).

---

### `keyword_detection/filter_keywords.py` — Re-filter Existing Detections for Another Subject

Rather than re-scanning raw transcripts, reuses the keywords already detected by the climate pipeline (loaded the same way as `import_segments.py`) and filters them down to the ones matching a *different* subject's dictionary (default: `environmental_health`) — useful when that subject's keywords already overlap with the climate/biodiversity/pollution theme keywords.

A segment is kept if it has at least one keyword matching the target dictionary's validated, non-high-risk keywords, and none matching its high-risk-false-positive keywords. A match is either an exact (lowercased) string, or the two keywords sharing the same set of lemmas (e.g. "pesticides" vs "pesticide"), using the same spaCy lemmatizer as the main detection pipeline (`quotaclimat/data_processing/mediatree/detect_keywords.py`).

**Key options:** `--subject` (env `SUBJECT`, default `environmental_health`), `--start-date`/`--end-date`.

---

### `keyword_detection/import_cases.py` — Import Annotated Cases

Imports annotated rows from the quotaclimat `analytics.task_global_completion` table (Label Studio annotations) into the RRS `cases` table, for the `climate` subject.

---

### `misinformation_detection/main.py` — LLM Misinformation Classification

Classifies segments/cases with an LLM (Mistral) against per-subject misinformation definitions (`definitions.py`), producing a score and reasoning persisted back to the `cases` table.

**Key environment variables:** `MISTRAL_API_KEY`, `SUBJECT` (default `insecurity`), `MISTRAL_MODEL`.

---

### `clustering/main.py` — Runnable Entry Point

The only runnable script in the clustering package. Runs a daily LLM clustering job that builds a fresh label set from the day's transcripts, deduplicates it against the clusters already in the database, and persists the assignments.

The other files in `clustering/` (`steps.py`, `backends.py`, `prompts.py`, `cost.py`) are library modules consumed by `main.py` — they are no longer runnable on their own.

**Pipeline (per day):**
1. **Generate** — each transcript is sent to the LLM independently; it returns a list of narrative labels present in the text (async, configurable concurrency)
2. **Merge** — generated labels are deduplicated in a hierarchical tournament of LLM merge calls
3. **DB-aware filter** — active clusters (those with at least one case assigned within `--expiry-days` of the run date) are loaded from the database. New candidate labels are compared against them using the embedding + LLM stance hybrid filter; only genuinely new labels survive
4. **Classify** — every transcript is classified against `surviving new labels + active DB labels` (async)
5. **Persist** — assignments and any new clusters are written back to the database

A token cost estimate is printed before each step.

**Output** (in `./bertopic_llm_output_v2/<run-date>/` by default):
- `labels_raw.json` — unmerged labels from step 1
- `labels_merged.json` — labels after merge
- `labels_new.json` — labels that survived DB deduplication
- `labels_new_with_ids.json` — new labels with their stable IDs
- `transcript_label_assignments.csv` — per-transcript label assignments

**Example:**
```bash
python -m rrs.clustering.main \
  --start-date 2025-01-01 \
  --end-date 2025-01-07 \
  --provider anthropic \
  --output-dir ./bertopic_llm_output_v2
```

**Key options:**

| Flag | Default | Description |
|---|---|---|
| `--start-date` / `--end-date` | today | Inclusive date range (`YYYY-MM-DD`) — one job per day in the range |
| `--provider` | `anthropic` | LLM provider: `mistral` or `anthropic` |
| `--max-concurrent` | `1` | Parallel LLM requests for steps 1 and 3 |
| `--merge-batch-size` | `30` | Labels per merge call (step 2) |
| `--merge-max-rounds` | `20` | Max hierarchical merge rounds |
| `--target-clusters` | adaptive | Explicit cluster target; otherwise `sqrt(n_docs)` clamped by min/max |
| `--low-threshold` | `0.40` | Below → auto-keep candidate (zone 1) |
| `--high-threshold` | `0.85` | Above → auto-drop candidate (zone 2) |
| `--embedding-backend` | `mistral` | `mistral` or `sentence-transformer` |
| `--embedding-model` | `dangvantuan/sentence-camembert-large` | Model name for the sentence-transformer backend |
| `--expiry-days` | `30` | DB clusters inactive for this many days before `start-date` are excluded |
| `--initial-labels-file` | — | JSON file of seed labels; overrides built-in seeds |
| `--no-seeds` | off | Start with no seed labels |
| `--skip-merge` | off | Skip the deduplication step |
| `--output-dir` | `./bertopic_llm_output_v2` | Output directory |

All flags can also be supplied via the equivalent environment variables (see `--help`).

---

### `clustering/get_data.py` — PostgreSQL Data Loader

Fetches transcript cases from the RRS PostgreSQL database and returns them as a pandas DataFrame. Also exposes helpers for reading and upserting clusters and case→cluster mappings.

Used by `main.py`. Requires the `RRS_PG_*` environment variables.

---

## Database

### Schema

All tables live in the `public` schema of the RRS PostgreSQL database.

```
subjects
  subject_id     (PK, text)
  name           (text)
  subject_title  (text)
  created_at / updated_at

       │ 1
       │
       ├──────────────────────┬──────────────────────────────────┐
       │ N                    │ N                                │ N
  segments              dictionary                            clusters
    segment_id  (PK)      keyword_id  (PK)                      cluster_id  (PK)
    subject_id  (PK, FK)  subject_id  (FK → subjects)           subject_id  (FK → subjects)
    s3_uri                keyword                                cluster_text
    n_keywords             high_risk_false_positive              created_at / updated_at
    keywords (array)       validated
    channel_name/title/program
    url_mediatree           created_at / updated_at
    created_at / updated_at
       │ 1
       │
       │ N
    cases  ──────────────────────────── case_to_clusters ──── clusters
      case_id      (PK)                   case_id    (PK, FK)
      segment_id   (FK → segments)        cluster_id (PK, FK)
      subject_id   (FK → subjects)        created_at
      model_score
      model_reason
      created_at / updated_at
```

### Migrations

Migrations are managed with a standalone Alembic project inside `rrs/`. Run all commands from the **repo root**:

```bash
# Apply all pending migrations
poetry run alembic -c rrs/alembic.ini upgrade head

# Autogenerate a new migration after model changes
poetry run alembic -c rrs/alembic.ini revision --autogenerate -m "description"

# Check current migration state
poetry run alembic -c rrs/alembic.ini current
```

---

## Environment Variables

### Database (RRS PostgreSQL)

| Variable | Default | Description |
|---|---|---|
| `RRS_PG_HOST` | `localhost` | PostgreSQL host |
| `RRS_PG_PORT` | `5432` | PostgreSQL port |
| `RRS_PG_DATABASE` | `rrs_db` | Database name |
| `RRS_PG_USER` | `user` | Database user |
| `RRS_PG_PASSWORD` | `password` | Database password |

### Database (quotaclimat PostgreSQL — source data)

Used by `import_segments.py`, `filter_keywords.py`, and `import_cases.py` to read already-detected transcripts/cases.

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | quotaclimat PostgreSQL host |
| `POSTGRES_PORT` | `5432` | quotaclimat PostgreSQL port |
| `POSTGRES_DB` | `barometre` | Database name |
| `POSTGRES_USER` | `user` | Database user |
| `POSTGRES_PASSWORD` | `password` | Database password |

### LLM APIs

| Variable | Used by | Description |
|---|---|---|
| `MISTRAL_API_KEY` | `clustering/main.py`, `misinformation_detection/main.py` | Mistral API key |
| `ANTHROPIC_API_KEY` | `clustering/main.py` | Anthropic API key |

### S3 / Object Storage

| Variable | Used by | Description |
|---|---|---|
| `BUCKET` | `keyword_detection/analyse_keywords.py` | S3 access key |
| `BUCKET_SECRET` | `keyword_detection/analyse_keywords.py` | S3 secret key |
| `BUCKET_NAME` | `keyword_detection/analyse_keywords.py`, `import_segments.py`, `filter_keywords.py` | S3 bucket name (used to build the stored `s3_uri`) |

### Other

| Variable | Used by | Description |
|---|---|---|
| `SUBJECT` | `analyse_keywords.py`, `filter_keywords.py`, `misinformation_detection/main.py`, `clustering/main.py`, `pulsar` | Restrict the job to a single subject |

Place these in `rrs/.env` for local development (already in `.gitignore`).

---

## Local Development with Docker

### Prerequisites

- Docker and Docker Compose installed
- A `.env` file in `rrs/` with your credentials (optional — falls back to local defaults)

### Spin up the local database and apply migrations

```bash
cd rrs
docker compose up migrate
```

This starts a local PostgreSQL instance on port `5434` and runs `alembic upgrade head` against it.

### Services

Run any of these with `docker compose up <service>` from `rrs/`. Most read from `rrs_db` and/or the source `barometre`/S3 data — see the env variable tables above for what to set.

| Service | Runs | Notes |
|---|---|---|
| `rrs_db` | Local PostgreSQL for RRS | Port `5434` |
| `migrate` | `alembic upgrade head` | Run first |
| `upsert_subjects` | `dictionary/upsert_subjects.py` | Run before `upsert_dictionary`/`filter_keywords` |
| `upsert_dictionary` | `dictionary/upsert_dictionary.py` | Depends on `upsert_subjects` |
| `import_segments` | `keyword_detection/import_segments.py` | Climate segments from the quotaclimat DB |
| `filter_keywords` | `keyword_detection/filter_keywords.py` | Re-filters those segments for `SUBJECT` (default `environmental_health`) |
| `import_cases` | `keyword_detection/import_cases.py` | Annotated cases from the quotaclimat DB |
| `analyse_keywords` | `keyword_detection/analyse_keywords.py` | Raw S3 transcript scan; depends on `upsert_dictionary` |
| `detect_misinformation` | `misinformation_detection/main.py` | LLM misinformation classification |
| `clustering` | `clustering/main.py` | LLM narrative clustering |
| `pulsar` | Pulsar-based ingestion (see `Dockerfile.pulsar`) | |
| `console` | `sleep 12000` | Interactive shell, see below |

### Open an interactive shell

```bash
docker compose up console -d
docker compose exec console bash
```

The `rrs/` directory is volume-mounted at `/app/rrs/` so code changes take effect immediately without rebuilding.

### Override credentials with a `.env` file

Create `rrs/.env` with any of the `RRS_PG_*` variables to override the local defaults:

```env
RRS_PG_HOST=my-remote-host
RRS_PG_PORT=5432
RRS_PG_DATABASE=my_db
RRS_PG_USER=my_user
RRS_PG_PASSWORD=my_password
```

Docker Compose automatically reads this file when present.

### Rebuild after dependency changes

```bash
docker compose build
```

---

## CI/CD — GitHub Actions

The workflow at `.github/workflows/rrs.yaml` runs Alembic migrations automatically:

| Trigger | Job | Environment |
|---|---|---|
| PR opened or commit pushed to PR | `migrate-test` | `test` |
| Merge to `main` | `migrate-prod` | `production` |

Both jobs use identical secret names; GitHub injects the right credentials based on the target environment. This avoids duplicating variable names with prefixes like `TEST_RRS_PG_HOST` vs `PROD_RRS_PG_HOST`.

**Setup:** In GitHub → repo Settings → Environments, create `test` and `production` environments and add these 5 secrets under each with their respective values:

- `RRS_PG_HOST`
- `RRS_PG_PORT`
- `RRS_PG_DATABASE`
- `RRS_PG_USER`
- `RRS_PG_PASSWORD`

The workflow only fires when files under `rrs/**` change, so it never runs unnecessarily on unrelated PRs.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| LLM clustering | Mistral Small 2506, Claude Haiku 4.5 |
| Embeddings | `dangvantuan/sentence-camembert-large` (SentenceTransformers) or `mistral-embed` |
| NLP / tokenisation | spaCy `fr_core_news_sm` |
| Async LLM calls | `asyncio`, `tqdm.asyncio` |
| Data loading | pandas |
| S3 querying | DuckDB |
| Database ORM | SQLAlchemy 2.x |
| Migrations | Alembic |
| Database | PostgreSQL 15 |
| Containers | Docker, Docker Compose |
