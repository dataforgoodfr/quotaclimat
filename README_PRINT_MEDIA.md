# Print Media Pipeline Documentation

## Overview

This project complements previous work of the Observatoire des Médias sur l'Écologie by extending the media coverage analysis—from television and radio—to include print media. The aim is to broaden the scope of the observatory to include newspapers, magazines, and online press through automated data collection and processing.

**Objective:**
- Analyze how environmental issues are covered in the French press, focusing on:
  1. **Climate change**
  2. **Biodiversity loss**
  3. **Depletion of natural resources**

The pipeline leverages Factiva's APIs & streams to retrieve, process, and analyze press articles, extracting and quantifying environmental discourse on these three major crises.

---

## Installation & Setup

**For installation instructions, environment setup, and dependencies, please refer to the main [README.md](README.md).**

This includes:
- Python environment setup with pyenv/poetry
- Docker configuration
- Database setup (PostgreSQL)
- DBT usage

---

## Database Architecture

### Factiva Tables

The print media pipeline uses several PostgreSQL tables defined in [`postgres/schemas/factiva_models.py`](postgres/schemas/factiva_models.py):

#### 1. `factiva_articles`
**Main table storing article data, keyword analysis, and prediction flags.**

- **Article metadata**: title, body, snippet, byline, source information, publication dates
- **Factiva metadata**: accession number (AN - primary key), document type, action (add/rep/del)
- **Keyword counts**: Separated by crisis type (climate, biodiversity, resources) and HRFP vs non-HRFP (see HRFP section below)
- **Keyword lists**: JSON arrays containing all detected keywords with metadata
- **Prediction flags**: Pre-calculated boolean columns (`predict_climat`, `predict_biodiversite`, etc.) indicating if article meets crisis/causal link thresholds. Calculated using keyword scores, HRFP multipliers, article length segments, and configurable thresholds. Updated on every processing run.
- **Duplicate detection**: `duplicate_status` field marks article duplicates (issue with Factiva API) for exclusion from analysis
- **Soft deletion**: `is_deleted` flag for handling deletion events

#### 2. `stats_factiva_articles`
**Time series statistics on article counts per source and datetime.**

- **Composite primary key**: (`source_code`, `publication_datetime`)
- **Count field**: Number of articles published at that datetime for that source
- Used to calculate total article volumes and percentages of environmental coverage

#### 3. `source_classification`
**Classification of media sources by type.**

- **Fields**: `source_code` (primary key), `source_name`, `source_type`
- **Source types**: PQN (national daily press), PQR (regional daily press), Magazine, Web, Agence Presse
- Used to segment analysis by media type

#### 4. `dictionary` & `keyword_macro_category`
**Climate/environmental keywords configuration.**

- **dictionary**: Keywords with their theme, category, language, and HRFP risk flag
- **keyword_macro_category**: Cross-sectoral categories (agriculture, transport, energy, etc.)
- Synced from Excel files maintained by experts (see main README for update process)

#### 5. `print_media_crises_indicators` (DBT Model)
**Aggregated environmental indicators for dashboards.**

Defined in [`my_dbt_project_print_media/models/dashboards/print_media_crises_indicators.sql`](my_dbt_project_print_media/models/dashboards/print_media_crises_indicators.sql)

This DBT model (simplified):
- **Aggregates** pre-calculated prediction flags from `factiva_articles` by temporal granularity and source
- Handles duplicate articles (excludes them from calculations)
- **Note**: Crisis labeling logic runs in Python (optimized SQL) before DBT, not in DBT itself
- Filters to 4+ day old data for data completeness
- Outputs daily counts of articles by crisis type and causal links

**Key columns**:
- `publication_day`, `source_code`, `source_name`, `source_type`
- `count_total_articles`: Total articles published that day
- `count_climat`, `count_biodiversite`, `count_ressources`: Articles meeting threshold per crisis
- `count_at_least_one_crise`: Articles covering at least one environmental crisis
- Causal link counts: `count_climat_constat`, `count_climat_cause`, `count_climat_consequence`, `count_climat_solution`, etc.

### Database Migrations

**Alembic** is used for database schema migrations in the `alembic_factiva/` directory.

- Migrations are automatically applied on deployment via the entrypoint script
- To create new migrations: see main README.md section on "SQL Tables evolution"
- Factiva models use a separate base (`FactivaBase`) to maintain independent migration history from audiovisual data

---

## Data Pipeline - Scaleway Jobs

The print media pipeline consists of 3 Docker containers running as **Scaleway serverless jobs**.

### CI/CD Deployment

Docker images are built and deployed via **GitHub Actions**: [`.github/workflows/deploy-main-print-media.yml`](.github/workflows/deploy-main-print-media.yml)

- **Triggers**: Push to `main` or `dev/print_media` branches, or manual workflow dispatch
- **Process**: 
  1. Bump version with Poetry
  2. Build 3 Docker images
  3. Push to Scaleway Container Registry
  4. Update Scaleway job definitions with new image versions
- **Jobs run daily** via Scaleway's cron scheduler

---

### Job 1: `factiva_stats_to_s3` 📊

**Summary**: Extracts article count statistics from Factiva's Time Series API and stores them in Scaleway S3.

**General principle:**
- The main goal is to retrieve the **total number of articles (with no keyword/regex filter applied)**, for each source (media) and each day from Factiva. This gives a neutral baseline for how much content each media produces, on top of which we can later compute the share of environmental coverage.

- For each extraction job, an **extraction ID** is returned by Factiva; this ID is also stored in S3. This allows the pipeline to later check for and recover previous extractions that may have failed or were not downloaded, ensuring completeness and robustness over time.

**Docker**: [`Dockerfile_factiva_stats_to_s3`](Dockerfile_factiva_stats_to_s3)  
**Python entry point**: [`quotaclimat/data_ingestion/factiva/factiva_to_s3/factiva_stats_to_s3.py`](quotaclimat/data_ingestion/factiva/factiva_to_s3/factiva_stats_to_s3.py)

**Key features**:
- Submits Time Series job with source codes, date range, and filters
- Polls for completion and downloads results
- Stores analytics IDs in S3 for tracking and recovery
- Marks processed IDs with `_PROCESSED` suffix
- Automatically retries unprocessed IDs from the last month

**Configuration** (via environment variables):
- `FACTIVA_USERKEY`: Factiva API user key
- `EXTRACT_STATS`: Enable/disable new extraction (default: true)
- `SELECT_DATE`: Use custom date range (default: false)
- `START_DATE` / `END_DATE`: Custom date range (when SELECT_DATE=true)
- `FACTIVA_DAYS_BEFORE_TODAY`: Lookback days from today (default: 7)
- `FACTIVA_STATS_ANALYSIS_DURATION`: Duration of analysis period (default: 7)

---

### Job 2: `factiva_to_s3` 📰

**One-sentence summary**: Consumes Factiva's real-time article stream via Pub/Sub and stores articles in Scaleway S3.

**General principle:**
- The job listens to the Factiva **Google Pub/Sub live feed** of articles, which provides every update (add, replace, delete) in near real-time.
- For each message received, the article is optionally filtered by a configurable list of accepted sources (defined in `followed_sources.json`).
- Articles are saved in batches (max 1000 per file) onto S3, partitioned by year and month, with traceability and ordering preserved by filenames.
- This job ensures that all content published by followed press sources is captured for detailed downstream analysis and keyword extraction.

**Docker**: [`Dockerfile_factiva_to_s3`](Dockerfile_factiva_to_s3)  
**Python entry point**: [`quotaclimat/data_ingestion/factiva/factiva_to_s3/factiva_to_s3.py`](quotaclimat/data_ingestion/factiva/factiva_to_s3/factiva_to_s3.py)

**Key features**:
- Handles streaming credentials from Factiva API
- Pulls messages in batches with acknowledgment
- Stops after consecutive empty pulls (configurable)
- Filters by source_code before uploading to S3
- Supports mock mode for testing with local JSON files
- Files are partitioned by year/month in S3

**Configuration** (via environment variables):
- `FACTIVA_USERKEY`: Factiva API user key
- `FACTIVA_SUBSCRIPTION_ID`: Pub/Sub subscription ID
- `FACTIVA_BATCH_SIZE`: Number of messages to pull per batch (default: 100)
- `FACTIVA_MAX_EMPTY_PULLS`: Max consecutive empty pulls before stopping (default: 5)
- `MOCK_MODE`: Run with local mock data instead of streaming (default: false)

---

### Job 3: `s3_factiva_to_postgre` 🔄

**One-sentence summary**: Processes articles and statistics from S3, detects keywords, and loads everything into PostgreSQL.

**General principle:**
- This job is the **central orchestrator** that transforms and loads raw data (from S3) into clean, queryable PostgreSQL tables ready for analysis and dashboards.
- For **article files**, it extracts full text, applies environmental keyword detection logic, calculates breakdowns per crisis and causal link, and upserts the results in the database. It also ensures only the relevant sources are kept.
- For **statistical files**, it loads simple time series of article counts per source/day.
- Additionally, it performs duplicate detection, updates the keyword dictionary, and triggers DBT to compute all final analysis indicators used in Metabase.
- Supports a specialized 'UPDATE' mode to re-process all articles (if dictionary/logic changes) without re-importing from S3.

**Docker**: [`Dockerfile_s3_factiva_to_postgre`](Dockerfile_s3_factiva_to_postgre)  
**Python entry point**: [`quotaclimat/data_processing/factiva/s3_to_postgre/s3_factiva_to_postgre.py`](quotaclimat/data_processing/factiva/s3_to_postgre/s3_factiva_to_postgre.py)

**Key components**:

1. **Dictionary Update**: Syncs keyword dictionary from Python constants to PostgreSQL tables

2. **Article Processing**:
   - Downloads unprocessed article files from S3 (lookback: 30 days by default)
   - Extracts keywords from article text (title + body + snippet + art)
   - Calculates keyword counts by crisis type and causal link
   - Stores both individual keyword lists and aggregated counts
   - Handles article actions: add (insert), rep (update), del (delete)
   - Handles bulk events: source_delete (removes all articles from a source)
   - Filters to only process articles from followed sources
   - Marks files as `_PROCESSED` in S3 after successful processing

3. **Statistics Processing**:
   - Downloads unprocessed stats files from S3
   - Upserts time series data into `stats_factiva_articles` table
   - Marks files as `_PROCESSED` in S3

4. **Duplicate Detection**:
   - Identifies duplicates by matching: source_code, title, snippet, body, word_count
   - Assigns duplicate status: `NOT_DUP`, `DUP_UNIQUE_VERSION`, or `DUP`
   - The most recent version (by modification_datetime) is kept as unique version
   - DBT model excludes duplicates from indicator calculations

5. **Prediction Flags Calculation** (new):
   - Pre-calculates crisis predictions for ALL articles using optimized SQL
   - Computes scores from keyword counts + HRFP multipliers
   - Determines article length segment (short/medium/long/very long)
   - Compares scores to configurable thresholds (per segment)
   - Updates 15 boolean flags: `predict_climat`, `predict_biodiversite`, `predict_ressources`, and causal links
   - Triggered on every run

6. **DBT Models Execution** (simplified):
   - Runs `print_media_crises_indicators` model with `--full-refresh`
   - Aggregates pre-calculated prediction flags by temporal granularity and source

6. **UPDATE Mode** (optional):
   - Re-detects keywords on existing articles in PostgreSQL
   - Useful when keyword dictionary changes
   - Processes articles in batches (configurable size)
   - Supports filtering by date range, source code, or crisis type
   - Only updates articles where keyword counts actually changed

**Configuration** (via environment variables):

*Normal mode (S3 ingestion)*:
- `PROCESS_ARTICLES`: Process article files (default: true)
- `PROCESS_STATS`: Process statistics files (default: true)
- `LOOKBACK_DAYS`: Days to look back for unprocessed files (default: 30)
- `UPDATE_DICTIONARY`: Update dictionary tables before processing (default: false)
- `DETECT_DUPLICATES`: Run duplicate detection (default: true)
- `CALCULATE_PREDICTIONS`: Calculate prediction flags for all articles (default: true)
- `RUN_DBT`: Run DBT models after processing (default: true)

*UPDATE mode (keyword re-detection)*:
- `UPDATE`: Enable UPDATE mode (default: false)
- `START_DATE_UPDATE`: Start date for update on publication_datetime field (YYYY-MM-DD)
- `END_DATE`: End date for update on publication_datetime field (YYYY-MM-DD)
- `SOURCE_CODE_UPDATE`: Comma-separated source codes to update (optional)
- `BIODIVERSITY_ONLY`: Only update articles with biodiversity keywords (default: false)
- `RESSOURCE_ONLY`: Only update articles with resource keywords (default: false)
- `CLIMATE_ONLY`: Only update articles with climate keywords (default: false)

*Prediction calculation configuration* (used in step 5):
- `MULTIPLIER_HRFP_CLIMAT`: Multiplier for climate HRFP keywords
- `MULTIPLIER_HRFP_BIODIV`: Multiplier for biodiversity HRFP keywords
- `MULTIPLIER_HRFP_RESSOURCE`: Multiplier for resource HRFP keywords
- `CONSIDER_ARTICLE_LENGTH`: Enable length-based thresholds (default: false)
- `WORD_COUNT_THRESHOLD`: Word count segments (e.g., "350-600" or "350-600-900")
- `THRESHOLD_BIOD_CLIM_RESS`: Thresholds for biodiv, climat, ressource
  * Single threshold: "3,2,2" (when CONSIDER_ARTICLE_LENGTH=false)
  * Multi-threshold: "3,2,2 - 4,3,3 - 5,4,4" (when CONSIDER_ARTICLE_LENGTH=true)
- `THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT`: Biodiversity causal link thresholds
  * Single: "1,1,1,1" or Multi: "1,1,1,1 - 2,2,2,2 - 3,3,3,3"
- `THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT`: Climate causal link thresholds
  * Single: "2,1,1,1" or Multi: "2,1,1,1 - 3,2,2,2 - 4,3,3,3"
- `THRESHOLD_RESS_CONST_SOLUT`: Resource causal link thresholds
  * Single: "1,1" or Multi: "1,1 - 2,2 - 3,3"

**Entrypoint script**: [`docker-entrypoint-s3-factiva-to-postgre.sh`](docker-entrypoint-s3-factiva-to-postgre.sh)
- Runs Alembic migrations before starting the Python job
- Ensures database schema is up-to-date

---

## Notebooks

### API Factiva Notebooks

**Location**: [`quotaclimat/data_ingestion/factiva/notebook_api_factiva/`](quotaclimat/data_ingestion/factiva/notebook_api_factiva/)

These notebooks help interact with Factiva's APIs for setup and exploration:

- **`estimate_nb_articles.ipynb`**: Estimate the number of articles available for a given query before creating an extraction
- **`create_snapshot_extraction.ipynb`**: Create a snapshot extraction (one-time) for a specific date range
- **`create_streaming_instance.ipynb`**: Set up a new streaming subscription for real-time article ingestion

**Use cases**:
- Initial setup of Factiva streaming subscriptions
- Testing queries before launching production extractions
- Estimating data volumes for capacity planning
- Manual extraction for historical data backfill

### Optimal Thresholds Exploration Notebooks

**Location**: [`quotaclimat/data_processing/factiva/explo_optimal_thresholds/`](quotaclimat/data_processing/factiva/explo_optimal_thresholds/)

These notebooks help determine the optimal keyword detection thresholds:

- **`1_apply_llm_predictions.ipynb`**: Apply LLM predictions to articles for labeling
- **`2_apply_keyword_detection.ipynb`**: Apply keyword detection logic to articles
- **`3_determine_optimal_keyword_thresholds.ipynb`**: Statistical analysis to find optimal thresholds by comparing keyword detection vs LLM predictions

**Purpose**:
- **Reference labelling**: Use LLM to manually label a sample of articles
- **Keyword detection**: Apply the current keyword detection logic to the same articles
- **Optimization**: Compare the two approaches to find thresholds that maximize accuracy (F1-score, precision, recall)
- **Validation**: Ensure keyword-based detection aligns with expert/LLM judgment

**Workflow**:
1. Select a representative sample of articles
2. Get LLM predictions for these articles (expensive but accurate)
3. Apply keyword detection with various threshold combinations
4. Calculate metrics (precision, recall, F1) for each threshold combination
5. Select optimal thresholds that balance precision and recall
6. Update configuration in DBT model or environment variables

The results from these notebooks inform the threshold values used in production:
- `THRESHOLD_BIOD_CLIM_RESS`: Global thresholds per crisis type
- `THRESHOLD_*_CONST_CAUSE_CONSE_SOLUT`: Causal link specific thresholds

---

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FACTIVA API                                 │
│  (Real-time streaming + Time Series Analytics)                     │
└────────────┬───────────────────────────┬─────────────────────────┬─┘
             │                           │                         │
             ▼                           ▼                         ▼
    ┌─────────────────┐      ┌─────────────────────┐    ┌──────────────────┐
    │  factiva_to_s3  │      │ factiva_stats_to_s3 │    │  Manual Notebooks│
    │   (Job 2)       │      │     (Job 1)         │    │   (Exploration)  │
    └────────┬────────┘      └──────────┬──────────┘    └──────────────────┘
             │                           │
             │   Article JSON files      │   Stats JSON files
             │   (stream data)           │   (time series)
             │                           │
             ▼                           ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    S3 OBJECT STORAGE                        │
    │  Articles: factiva/articles/year_*/month_*/*_stream.json   │
    │  Stats: factiva/nb_articles/year_*/month_*/*_stats.json    │
    └──────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                  ┌──────────────────────────────┐
                  │ s3_factiva_to_postgre        │
                  │       (Job 3)                │
                  │                              │
                  │ 1. Download files            │
                  │ 2. Extract keywords          │
                  │ 3. Upsert to PostgreSQL      │
                  │ 4. Detect duplicates         │
                  │ 5. Calculate predictions     │
                  │ 6. Run DBT models            │
                  └────────┬─────────────────────┘
                           │
                           ▼
            ┌──────────────────────────────────────────┐
            │         POSTGRESQL DATABASE              │
            │                                          │
            │  ┌────────────────────────────────────┐ │
            │  │  factiva_articles                  │ │
            │  │  (articles + keywords + flags)     │ │
            │  └────────────────────────────────────┘ │
            │                                          │
            │  ┌────────────────────────────────────┐ │
            │  │  stats_factiva_articles            │ │
            │  │  (time series counts)              │ │
            │  └────────────────────────────────────┘ │
            │                                          │
            │  ┌────────────────────────────────────┐ │
            │  │  print_media_crises_indicators     │ │
            │  │  (DBT model - dashboard metrics)   │ │
            │  └────────────────────────────────────┘ │
            └──────────────────┬───────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      METABASE        │
                    │   (Visualization)    │
                    └──────────────────────┘
```

---

## Key Concepts


### HRFP (High Risk of False Positive)

Keywords are classified as HRFP or non-HRFP based on their specificity:

- **Non-HRFP keywords**: Highly specific to environmental topics (e.g., "réchauffement climatique", "biodiversité", "transition énergétique")
- **HRFP keywords**: More generic terms that could appear in non-environmental contexts (e.g., "climat", "environnement", "ressource")

**Strategy**:
- HRFP keywords can be included with a multiplier < 1 if needed
- This allows fine-tuning precision vs recall in article detection

### Duplicate Detection

Articles can appear multiple times in the stream due to issue with Factiva API (in an ideal world there would be duplicates):

- **Duplicates identified by**: source_code + title + snippet + body + word_count
- **Status values**:
  - `NOT_DUP`: Unique article (no duplicates)
  - `DUP_UNIQUE_VERSION`: The version to keep (most recent modification_datetime)
  - `DUP`: Duplicate to exclude from analysis
- **Impact on analysis**: 
  - Source-days with 2+ DUP are completely excluded (we consider the data of bad quality if there are 2+ duplicates for a given day x source)

### Causal Links

Articles are analyzed for specific causal links within each crisis:

**Climate**:
- Constat (observation/facts)
- Causes
- Consequences
- Solutions (atténuation + adaptation)

**Biodiversity**:
- Concepts généraux (general concepts)
- Causes
- Consequences
- Solutions

**Resources**:
- Constat
- Solutions

**Labeling logic**: An article is counted for a causal link ONLY if:
1. The causal link score meets its threshold, AND
2. The global crisis score meets its threshold

This ensures articles are substantive on the topic, not just mentioning it in passing.

---

## Further Reading

- **Main project README**: [README.md](README.md)
- **Factiva API documentation**: (https://developer.dowjones.com/documents/factiva_integration-factiva_analytics)
- **Metabase dashboards**: Access via internal deployment (ask team for URL)
