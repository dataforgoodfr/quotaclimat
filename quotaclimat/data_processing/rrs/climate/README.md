# LLM Clustering — `cluster_llm.py`

Assigns climate-discussion narrative labels to TV/radio transcripts using Claude Haiku. Given a dataset of transcripts, it discovers what recurring claims they make, builds a compact label taxonomy, and maps every transcript to the labels that describe it.

## How it works

### Pre-processing

Each transcript is split into overlapping sentence windows using spaCy (French model `fr_core_news_sm`). Default: windows of 3 sentences with 30-token overlap. Windows that are too short (< 20 chars) are dropped. The result is a `{doc_id → [sentences]}` dictionary used by all three steps.

---

### Step 1 — Label generation (async, per transcript)

Each transcript's sentences are sent to the LLM independently with a small fixed prompt. The model returns 1–2 French narrative labels describing the climate claim(s) expressed, or `[]` if none are present.

**Why isolated prompts?** Keeping each call small and self-contained avoids the token cost of sharing a growing label list across documents. Deduplication is handled in Step 2.

All calls run concurrently (controlled by `--max-concurrent`). Results are deduplicated by lowercase key and merged with the optional seed labels to form the raw candidate list.

Seed labels (hardcoded in `SEED_LABELS`) are pre-populated known narratives. They can be overridden via `--initial-labels-file` or disabled with `--no-seeds`.

---

### Step 2 — Hierarchical label merging

The raw label list (often hundreds of entries) is reduced to a compact taxonomy through a tournament-style merge:

```
While len(labels) > batch_size:
    shuffle labels randomly
    split into chunks of batch_size
    merge each chunk in parallel  ←  LLM call per chunk
    flatten results

Final call: merge the remaining ≤ batch_size labels in one call
```

Each LLM call receives a batch of at most `--merge-batch-size` labels (default 30) and is told to aggressively collapse similar claims into one. The shuffle before each round ensures labels from different parts of the list eventually get compared together. The process stops when the surviving labels fit in a single batch.

After every round the current label list is written (sorted alphabetically) to `labels_merge_progress.json` in the output directory, so progress can be monitored while the job runs.

The final merged list is saved to `labels.json` with integer IDs assigned in order.

---

### Step 3 — Classification (async, per transcript)

Each transcript is classified against the final label list. The LLM selects all labels from the list that match the concepts expressed in the transcript's sentences. Only labels that appear verbatim in the final list are accepted (fuzzy matches are discarded).

All calls run concurrently (same `--max-concurrent` limit as Step 1). Results are saved to `transcript_label_assignments.csv`.

---

### Step 4 — Mesinfo statistics

If the dataset contains `mesinfo_correct` and `mesinfo_incorrect` columns (fact-checking scores from the HuggingFace dataset), per-label statistics are computed: how many documents assigned to each label were rated as correct or incorrect misinformation. Saved to `label_mesinfo_stats.csv`.

---

## Token / cost estimation

Before each step the script estimates the number of tokens and approximate cost using `client.messages.count_tokens` (no inference, tokenisation only):

- **Step 1**: samples 20 documents, extrapolates to the full dataset
- **Step 2**: simulates the hierarchical rounds assuming 50% reduction per round, counts tokens on one representative batch
- **Step 3**: samples 20 documents with the final label list included, extrapolates

Pricing constants (`_INPUT_COST_PER_M`, `_OUTPUT_COST_PER_M`) are at the top of the file and should be updated if Anthropic changes rates.

---

## Data source

By default loads from the HuggingFace dataset `DataForGood/climateguard-training` (requires `HF_TOKEN` in `.env`). Filtered to France and July 2025 by default.

A local `.json` or `.csv` file can be used instead via `--input`.

---

## Output files

| File | Description |
|---|---|
| `labels_raw.json` | All candidate labels before merging (seeds + Step 1 output) |
| `labels_merge_progress.json` | Current label list after each merge round (overwritten each round) |
| `labels.json` | Final merged labels with integer IDs |
| `transcript_label_assignments.csv` | One row per (transcript, label) assignment |
| `label_mesinfo_stats.csv` | Per-label mesinfo counts (if available) |

---

---

# Timeseries Clustering — `cluster_llm_timeseries.py`

An incremental variant that evolves the label taxonomy day-by-day over a date range, rather than processing a static month-slice in one batch.

## How it works

### Warmup phase

The first N days (default 7) are processed exactly like `cluster_llm.py`: Step 1 generates labels, Step 2 merges them into a compact taxonomy, Step 3 classifies every warmup transcript. This establishes the initial label set.

---

### Incremental days

For each day after the warmup window:

**Step 3a — Classify with "other" escape hatch**

Each transcript is classified against the current label list. The prompt includes an extra option: if none of the existing labels apply and the transcript expresses a distinct new narrative, the model returns `["other"]` instead.

**Step 1b + 2b — Discover new clusters**

All "other" docs are collected at end of day. Step 1 runs on them to generate candidate labels for the new narratives. Step 2b then tries to absorb each candidate into the **fixed** existing taxonomy (up to `--max-merge-attempts` rounds, default 3), with progressively stronger prompting on each retry. A candidate that maps to an existing label is discarded; one that remains unmatched after all attempts is appended as a genuinely new label. The existing taxonomy is never reduced or restructured during this step.

**Step 3b — Reclassify**

The "other" docs are re-classified against the updated taxonomy so they receive proper labels rather than being left unassigned.

---

### Label evolution tracking

Every label is recorded in `label_evolution.json` with the date it first entered the taxonomy. Warmup labels receive the warmup end date; labels discovered on incremental days receive that day's date.

---

## Output files

| File | Description |
|---|---|
| `labels.json` | Current label list with integer IDs (updated each time new clusters are found) |
| `labels_YYYY-MM-DD.json` | Per-day label snapshot (optional, `--save-daily-labels`) |
| `transcript_label_assignments.csv` | One row per (transcript, label) assignment with a `date` column |
| `other_docs.csv` | Doc IDs flagged as "other" before reclassification (for audit) |
| `label_evolution.json` | `[{label, label_id, first_seen_date}]` — full history of when each label entered the taxonomy |

---

## Usage

```bash
python -m quotaclimat.data_processing.rrs.climate.cluster_llm_timeseries \
  --start-date 2025-07-01 --end-date 2025-07-31 \
  --warmup-days 7 \
  --output-dir ./timeseries_output \
  --max-concurrent 3
```

### Key options

| Flag | Default | Description |
|---|---|---|
| `--start-date` | required | Start of the date range (`YYYY-MM-DD`) |
| `--end-date` | required | End of the date range (`YYYY-MM-DD`) |
| `--warmup-days` | `7` | Days used for the warmup phase |
| `--max-merge-attempts` | `3` | Rounds to try absorbing a new candidate into existing labels before adding it as-is |
| `--save-daily-labels` | off | Save a `labels_<date>.json` snapshot after each day |

All other flags from `cluster_llm.py` are supported: `--merge-batch-size`, `--max-concurrent`, `--window-size`, `--overlap-tokens`, `--no-seeds`, `--initial-labels-file`, `--skip-merge`, `--country`, `--input`, etc.

---

# `cluster_llm.py` — Static Batch Mode

## Usage

```bash
python -m quotaclimat.data_processing.rrs.climate.cluster_llm \
  --output-dir ./bertopic_llm_output \
  --filter-year 2025 --filter-month 7 \
  --merge-batch-size 30 \
  --max-concurrent 3
```

### Key options

| Flag | Default | Description |
|---|---|---|
| `--output-dir` | `./bertopic_llm_output` | Where to write all output files |
| `--filter-year` | `2025` | Keep only records from this year |
| `--filter-month` | `7` | Keep only records from this month (1–12) |
| `--date-column` | `data_item_start` | Dataset column holding the broadcast date |
| `--merge-batch-size` | `30` | Labels per merge call in Step 2 |
| `--max-concurrent` | `1` | Max parallel Anthropic requests |
| `--window-size` | `3` | Sentences per chunk in pre-processing |
| `--overlap-tokens` | `30` | Token overlap between consecutive chunks |
| `--no-seeds` | off | Start with no seed labels |
| `--initial-labels-file` | — | JSON list of seed labels (overrides built-in) |
| `--skip-merge` | off | Skip Step 2 entirely |
| `--input` | — | Local `.json`/`.csv` instead of HuggingFace |
| `--country` | `france` | Country filter (empty string = all countries) |

### Environment variables

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Required — Claude API key |
| `HF_TOKEN` | Required when loading from HuggingFace |
