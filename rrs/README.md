# RRS — Risk Response System

RRS is a multi-module analysis system built to detect, cluster, and track disinformation narratives in French-language media (TV and radio transcripts). It ingests annotated transcripts from PostgreSQL or HuggingFace, applies NLP and LLM-based clustering pipelines to surface recurring false claims, and persists structured results in a dedicated PostgreSQL database.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ DATA SOURCES                                                    │
│   PostgreSQL (analytics.task_global_completion)                 │
│   HuggingFace (DataForGood/climateguard-training)               │
│   S3 / Scaleway parquet files                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ keyword_detection/analyse_keywords.py                           │
│   DuckDB on S3 parquet — French insecurity keyword detection   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ PRE-PROCESSING (shared)                                         │
│   spaCy sentence segmentation → sliding-window chunking        │
│   Configurable window size and token overlap                    │
└──────┬─────────────────────┬──────────────────────┬────────────┘
       │                     │                      │
       ▼                     ▼                      ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│ cluster.py   │  │ cluster_llm.py   │  │ cluster_llm_          │
│              │  │                  │  │ timeseries.py         │
│ BERTopic     │  │ 3-step LLM       │  │ 7-day warmup +       │
│ static       │  │ pipeline         │  │ sliding-window       │
│ clustering   │  │ (Mistral/Claude) │  │ incremental updates  │
└──────┬───────┘  └────────┬─────────┘  └──────────┬───────────┘
       │                   │                        │
       └───────────────────┴────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ PostgreSQL (RRS database)                                       │
│   subjects · dictionary · segments · cases · clusters          │
│   case_to_clusters                                              │
│   Managed via SQLAlchemy ORM + Alembic migrations              │
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
├── docker-compose.yml
├── alembic.ini                      # Standalone Alembic config
│
├── alembic/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/                    # Migration files
│
├── clustering/
│   ├── README.md                    # Clustering-specific docs
│   ├── cluster.py                   # BERTopic static clustering
│   ├── cluster_llm.py               # LLM 3-step pipeline
│   ├── cluster_llm_timeseries.py    # Incremental daily clustering
│   └── get_data.py                  # PostgreSQL data loader
│
├── keyword_detection/
│   ├── analyse_keywords.py          # S3 parquet keyword detection
│   └── dictionary.py               # French insecurity keyword lists
│
└── schemas/
    ├── base.py                      # SQLAlchemy declarative base
    └── models.py                    # ORM models for all 6 tables
```

---

## Modules

### `clustering/cluster.py` — BERTopic Static Clustering

Fits a seeded BERTopic model on a corpus of French climate transcripts and produces a static topic map.

**Input:** HuggingFace dataset (`DataForGood/climateguard-training`) or a local `.json`/`.csv` file.

**Processing:**
- 18 seeded topics covering climate skepticism narratives (ZFE, renewables, electric cars, etc.)
- French sentence embeddings via `dangvantuan/sentence-camembert-large`
- Dimensionality reduction with UMAP + density clustering with HDBSCAN
- Automatic topic merging by cosine similarity (default threshold: 0.92)
- Outlier reduction with configurable strategy

**Output** (in `./bertopic_output/` by default):
- `sentences_with_topics.csv` — sentence-level topic assignments
- `topic_summary.csv` — per-topic mesinfo fact-check statistics

**Example:**
```bash
# From HuggingFace, France 2025 data, 3-sentence windows
python -m rrs.clustering.cluster \
  --output-dir ./bertopic_output \
  --window-size 3 \
  --overlap-tokens 30 \
  --country france \
  --year 2025

# From a local CSV file
python -m rrs.clustering.cluster \
  --input ./data/claims.csv \
  --text-column claim_text \
  --output-dir ./bertopic_output
```

**Key options:**

| Flag | Default | Description |
|---|---|---|
| `--input` | — | Local file path; omit to load from HuggingFace |
| `--hf-dataset` | `DataForGood/climateguard-training` | HuggingFace dataset |
| `--country` | `france` | Country filter; pass `""` for all |
| `--year` | `2025` | Year filter; pass `0` to disable |
| `--window-size` | `3` | Sentences per chunk |
| `--overlap-tokens` | `30` | Overlap between consecutive windows |
| `--merge-threshold` | `0.92` | Cosine similarity threshold for topic merging |
| `--nr-topics` | — | Hard merge down to N topics after fitting |
| `--no-reduce-outliers` | off | Disable outlier reassignment |
| `--output-dir` | `./bertopic_output` | Output directory |

---

### `clustering/cluster_llm.py` — LLM 3-Step Pipeline

Uses an LLM to generate, merge, and classify narrative labels without requiring a predefined topic list. Supports Mistral Small and Claude Haiku 4.5.

**Pipeline:**
1. **Generate** — each transcript is sent to the LLM independently; it returns a list of narrative labels present in the text (async, configurable concurrency)
2. **Merge** — all generated labels are deduplicated in a hierarchical tournament of LLM merge calls (30 labels per batch by default)
3. **Classify** — every transcript is classified against the final consolidated label list (async)
4. **Report** — mesinfo fact-check scores aggregated per label

A **token cost estimate** is printed before each step so you can abort early if the run would be too expensive.

**Output** (in `./bertopic_llm_output/` by default):
- `labels.json` — final label list with stable MD5-derived IDs
- `labels_raw.json` — unmerged labels from step 1
- `transcript_label_assignments.csv` — per-transcript label assignments
- `label_mesinfo_stats.csv` — fact-check stats per label
- `labels_merge_progress.json` — merge step intermediate state

**Example:**
```bash
# Using Mistral (default), from HuggingFace
python -m rrs.clustering.cluster_llm \
  --output-dir ./bertopic_llm_output \
  --provider mistral \
  --country france \
  --start-date 2025-01-01 \
  --end-date 2025-03-31

# Using Claude Haiku, from a local file
python -m rrs.clustering.cluster_llm \
  --input ./data/claims.csv \
  --text-column claim_text \
  --provider anthropic \
  --output-dir ./output_anthropic
```

**Key options:**

| Flag | Default | Description |
|---|---|---|
| `--provider` | `mistral` | LLM provider: `mistral` or `anthropic` |
| `--max-concurrent` | `1` | Parallel LLM requests for steps 1 and 3 |
| `--merge-batch-size` | `30` | Labels per merge call (step 2) |
| `--merge-max-rounds` | `20` | Max hierarchical merge rounds |
| `--initial-labels-file` | — | JSON file of seed labels; overrides built-in seeds |
| `--no-seeds` | off | Start with no seed labels |
| `--skip-merge` | off | Skip the deduplication step |
| `--start-date` / `--end-date` | — | Date range filter (`YYYY-MM-DD`) |
| `--output-dir` | `./bertopic_llm_output` | Output directory |

---

### `clustering/cluster_llm_timeseries.py` — Incremental Daily Clustering

Builds and evolves a label taxonomy day-by-day over a date range. Designed for tracking how new narratives emerge over time.

**Pipeline:**

*Warmup phase (first N days, default 7):*
- Runs the full 3-step LLM pipeline on the warmup window to establish an initial taxonomy

*Incremental phase (remaining days):*
- For each day, generates candidate labels from the past `sliding_window_days` of data
- Filters candidates against the existing taxonomy using CamemBERT embeddings and a 3-zone hybrid strategy:
  - **Zone 1** (similarity < 0.40) — auto-keep: sufficiently different topic
  - **Zone 2** (similarity > 0.85) — auto-drop: near-exact match already in taxonomy
  - **Zone 3** (in between) — LLM judges whether the candidate is genuinely novel
- Adds confirmed new labels to the taxonomy and records their first-seen date
- Classifies today's documents against the updated taxonomy

**Output** (in `./timeseries_output/` by default):
- `labels.json` — cumulative label taxonomy (updated each day)
- `label_evolution.json` — per-label first-seen dates
- `transcript_label_assignments.csv` — daily assignments, appended incrementally
- `labels_<date>.json` — per-day snapshots (with `--save-daily-labels`)

**Example:**
```bash
python -m rrs.clustering.cluster_llm_timeseries \
  --start-date 2025-01-01 \
  --end-date 2025-06-30 \
  --provider mistral \
  --warmup-days 7 \
  --output-dir ./timeseries_output \
  --save-daily-labels
```

**Key options:**

| Flag | Default | Description |
|---|---|---|
| `--start-date` | required | Start of date range (`YYYY-MM-DD`) |
| `--end-date` | required | End of date range (`YYYY-MM-DD`) |
| `--warmup-days` | `7` | Days used for initial taxonomy |
| `--sliding-window-days` | `7` | Window size for candidate generation |
| `--low-threshold` | `0.40` | Below → auto-keep (zone 1) |
| `--high-threshold` | `0.85` | Above → auto-drop (zone 2) |
| `--top-k-context` | `10` | Closest existing labels shown to LLM per zone-3 candidate |
| `--embedding-model` | `dangvantuan/sentence-camembert-large` | SentenceTransformer model |
| `--save-daily-labels` | off | Write a `labels_<date>.json` snapshot per day |
| `--provider` | `mistral` | LLM provider: `mistral` or `anthropic` |

---

### `clustering/get_data.py` — PostgreSQL Data Loader

Fetches annotated transcript data from a production PostgreSQL instance (`analytics.task_global_completion`) and returns it as a HuggingFace `Dataset`.

Used by the clustering scripts as an alternative to loading from HuggingFace. Requires the `RRS_PG_*` environment variables (or the equivalent `PG_*` vars in `.env`).

---

### `keyword_detection/analyse_keywords.py` — Insecurity Keyword Detection

Scans S3-hosted parquet files with DuckDB and flags transcripts containing French insecurity keywords (from `dictionary.py`) along with nearby correlate words (within ~150 characters).

**Input:** Parquet files on Scaleway Object Storage.

**Output:** Excel file with flagged segments, keyword matches, and a `has_nearby_correlate` boolean column.

**Required environment variables:**

| Variable | Description |
|---|---|
| `BUCKET` | S3 access key (Scaleway) |
| `BUCKET_SECRET` | S3 secret key |
| `BUCKET_NAME` | S3 bucket name |

---

## Database

### Schema

All tables live in the `public` schema of the RRS PostgreSQL database.

```
subjects
  subject_id  (PK, text)
  name        (text)
  created_at / updated_at

       │ 1
       │
       ├─────────────────────────────────────────────────┐
       │ N                                               │ N
  segments                                           clusters
    segment_id  (PK)                                   cluster_id  (PK)
    subject_id  (FK → subjects)                        subject_id  (FK → subjects)
    s3_uri                                             cluster_text
    n_keywords                                         created_at / updated_at
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

dictionary
  keyword_id              (PK)
  keyword                 (string)
  high_risk_false_positive (boolean)
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

### LLM APIs

| Variable | Used by | Description |
|---|---|---|
| `MISTRAL_API_KEY` | `cluster_llm.py`, `cluster_llm_timeseries.py` | Mistral API key |
| `ANTHROPIC_API_KEY` | `cluster_llm.py`, `cluster_llm_timeseries.py` | Anthropic API key |
| `HF_TOKEN` | `cluster.py`, `cluster_llm.py`, `cluster_llm_timeseries.py` | HuggingFace token (for private datasets) |

### S3 / Object Storage

| Variable | Used by | Description |
|---|---|---|
| `BUCKET` | `keyword_detection/analyse_keywords.py` | S3 access key |
| `BUCKET_SECRET` | `keyword_detection/analyse_keywords.py` | S3 secret key |
| `BUCKET_NAME` | `keyword_detection/analyse_keywords.py` | S3 bucket name |

Place these in `rrs/clustering/.env` for local development (already in `.gitignore`).

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
| Static clustering | BERTopic, HDBSCAN, UMAP |
| LLM clustering | Mistral Small 2506, Claude Haiku 4.5 |
| Embeddings | `dangvantuan/sentence-camembert-large` (SentenceTransformers) |
| NLP / tokenisation | spaCy `fr_core_news_sm` |
| Async LLM calls | `asyncio`, `tqdm.asyncio` |
| Data loading | pandas, HuggingFace `datasets` |
| S3 querying | DuckDB |
| Database ORM | SQLAlchemy 2.x |
| Migrations | Alembic |
| Database | PostgreSQL 15 |
| Containers | Docker, Docker Compose |
