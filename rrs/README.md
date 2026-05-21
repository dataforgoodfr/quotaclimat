# RRS — Risk Response System

RRS is a multi-module analysis system built to detect, cluster, and track disinformation narratives in French-language media (TV and radio transcripts). It ingests annotated transcripts from PostgreSQL or HuggingFace, applies NLP and LLM-based clustering pipelines to surface recurring false claims, and persists structured results in a dedicated PostgreSQL database.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ DATA SOURCES                                                    │
│   PostgreSQL (RRS database — cases table)                       │
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
┌─────────────────────────────────────────────────────────────────┐
│ cluster_llm_v2.py                                               │
│   3-step LLM pipeline (Mistral / Claude Haiku)                  │
│   DB-aware deduplication against active clusters                │
└────────────────────────────┬────────────────────────────────────┘
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
│   ├── cluster_llm_v2.py            # Runnable entry point (LLM pipeline + DB dedup)
│   ├── cluster.py                   # Library: text loading + sentence chunking
│   ├── cluster_llm.py               # Library: 3-step LLM pipeline building blocks
│   ├── cluster_llm_timeseries.py    # Library: embedding backends + novelty filter
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

### `clustering/cluster_llm_v2.py` — Runnable Entry Point

The only runnable script in the clustering package. Runs a daily LLM clustering job that builds a fresh label set from the day's transcripts, deduplicates it against the clusters already in the database, and persists the assignments.

The other files in `clustering/` (`cluster.py`, `cluster_llm.py`, `cluster_llm_timeseries.py`) are library modules consumed by `cluster_llm_v2.py` — they are no longer runnable on their own.

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
python -m rrs.clustering.cluster_llm_v2 \
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

Used by `cluster_llm_v2.py`. Requires the `RRS_PG_*` environment variables.

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
| `MISTRAL_API_KEY` | `cluster_llm_v2.py` | Mistral API key |
| `ANTHROPIC_API_KEY` | `cluster_llm_v2.py` | Anthropic API key |

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
