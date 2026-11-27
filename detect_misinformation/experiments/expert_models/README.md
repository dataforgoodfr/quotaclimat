# Expert Models

Per-topic fine-tuning datasets built from source PDFs (books/reports) that
debunk misinformation on a given subject. Each topic gets its own model
directory; a shared pipeline turns each source PDF into training
conversations.

## Layout

```
expert_models/
├── scripts/
│   ├── parse_pdf_to_jsonl.py       # PDF -> per-subsection JSONL
│   ├── generate_conversations.py   # per-subsection JSONL -> affirmation/debunk conversations
│   └── train_lora.py               # conversations -> LoRA fine-tuned model, pushed to the Hub
├── chat_templates/                 # optional Jinja chat templates for train_lora.py --chat-template
├── <model>/                        # e.g. biodiversity, insecurity
│   └── data/
│       ├── raw/                    # source PDF(s)
│       ├── parsed/                 # output of parse_pdf_to_jsonl.py
│       └── conversations/          # output of generate_conversations.py
├── train_output/                   # local adapter/merged-model output of train_lora.py (gitignored)
└── README.md
```

Each `<model>/data/` directory is gitignored (see `.gitignore`) — only the
scripts and this README are tracked. Source PDFs and generated data stay
local.

To add a new topic: create `expert_models/<model>/data/raw/` and drop the
source PDF(s) in it. Everything else is inferred from `<model>`.

## Pipeline

### 1. Parse the PDF into sections — `parse_pdf_to_jsonl.py`

Source PDFs generally have no usable outline/bookmarks, so chapters and
subsections are detected from font metadata instead of a table of contents
or blank lines (ordinary paragraph breaks look identical to real section
breaks, so `\n\n` is not a usable signal):

- **Chapters**: the largest heading-size cluster on a page starts a new
  chapter; a smaller cluster on the same page (if present) becomes a
  `kicker`/eyebrow label (e.g. "PREMIÈRE FAUSSE INFORMATION" above a chapter
  title). A page with only a kicker-tier heading (e.g. a part divider with no
  dedicated title font) still starts its own chapter, so its content isn't
  silently dropped.
- **Subsections**: within a chapter, a paragraph block set entirely in bold
  text at roughly body-text size marks a new subsection. Chapters whose PDF
  has no such convention yield a single untitled subsection spanning the
  whole chapter.

Output: one JSONL record per subsection in `data/parsed/<pdf_name>.jsonl`,
with `id`, `chapter_title`, `subsection_title`, `char_count`, `start_page`,
`end_page`, `text`.

```bash
# Defaults to the single PDF in data/raw and writes data/parsed/<pdf_name>.jsonl
uv run climateguard/expert_models/scripts/parse_pdf_to_jsonl.py biodiversity \
    --stop-after-keyword REMERCIEMENTS

uv run climateguard/expert_models/scripts/parse_pdf_to_jsonl.py insecurity
```

`--stop-after-keyword TEXT` drops all chapters after the first one whose
title/kicker matches TEXT (case-insensitive) — use it when the source has
trailing back matter (e.g. a duplicated table of contents, publisher
catalog) that isn't part of the book's content. Omit it if there's none.

See `--help` for `--pdf`/`--out` overrides.

### 2. Generate training conversations — `generate_conversations.py`

For each parsed subsection, a teacher LLM (Mistral by default, or Claude)
is asked to generate `--n-pairs` affirmation/debunk pairs grounded strictly
in that subsection's text:

- **Half TRUE** affirmations — statements the text supports. Debunk: a
  short, one-sentence confirmation.
- **Half FALSE** affirmations — myths/misconceptions the text contradicts.
  Debunk: a short explanation (2-4 sentences) citing concrete facts or
  figures from the text.

Output: one JSONL record per pair in `data/conversations/<pdf_name>.jsonl`,
as a two-turn conversation:

```json
{
  "messages": [
    {"role": "user", "content": "<affirmation>"},
    {"role": "assistant", "content": "<debunk>"}
  ],
  "metadata": {
    "is_true": true,
    "chapter_title": "...",
    "subsection_title": "...",
    "start_page": 12,
    "end_page": 14,
    "source_id": "02_01_..."
  }
}
```

```bash
# Requires MISTRAL_API_KEY (default provider) or ANTHROPIC_API_KEY in .env
uv run climateguard/expert_models/scripts/generate_conversations.py biodiversity

uv run climateguard/expert_models/scripts/generate_conversations.py insecurity \
    --provider claude --n-pairs 6
```

Useful flags:
- `--limit N` — process only the first N subsections (for a quick test).
- `--dry-run` — list what would be processed without calling the API.
- `--concurrency N` — max concurrent API calls (default 5).
- `--overwrite` — regenerate from scratch instead of resuming (by default,
  reruns skip subsections already present in the output file).

### 3. Fine-tune a model — `train_lora.py`

LoRA fine-tunes a base chat model on one or more topics' conversation files
with TRL (SFT or DPO — see `--method` below), then pushes the result to the
Hugging Face Hub as a **private** model under the `DataForGood` org. Pairs
are split into train/eval by source excerpt (`source_id`), so pairs
generated from the same subsection never leak across the split.

Three independent choices:

- `--backend {accelerate,unsloth}` — training stack. `accelerate` (default)
  uses plain transformers + peft + trl and runs anywhere, including CPU/MPS.
  `unsloth` is faster/lighter but needs a CUDA GPU and the `unsloth` package.
- `--quant {4bit,none}` — `4bit` (default) performs real QLoRA via
  bitsandbytes NF4 quantization and requires a CUDA GPU + the `bitsandbytes`
  package. `none` is a plain LoRA over full/half-precision weights — the
  fallback for testing the whole pipeline locally on a laptop before
  spending GPU time.
- `--method {sft,dpo,sft_dpo}` — `sft` (default) supervises directly on the
  debunk text + `[TRUE]`/`[FALSE]` verdict tag. `dpo` preference-tunes
  instead: for each example, the chosen completion is the debunk text with
  the correct verdict tag and the rejected completion is the *same debunk
  text* with the tag flipped. There's no independent second response to
  prefer between in this data, so this design isolates the preference signal
  to the TRUE/FALSE calibration specifically (see `add_dpo_columns` in the
  script) — it won't teach response style/quality the way DPO normally would
  with genuinely different candidate responses. `sft_dpo` runs both in
  order, continuing to train the *same* LoRA adapter through the DPO stage
  (SFT warm-start, then preference-tune) rather than training two separate
  adapters — the standard recipe. `--dpo-beta` (default 0.1) controls the KL
  penalty; `--dpo-learning-rate` (default `5e-6`) and `--dpo-epochs`
  (default `1`) control the DPO stage independently of `--learning-rate`/
  `--epochs` (which control the SFT stage) — DPO is typically far more
  sensitive to a high learning rate than SFT. With `sft_dpo`, the verdict
  eval (see below) runs once after each stage, so you can see whether DPO
  actually improved on the SFT checkpoint.

`unsloth` and `bitsandbytes` live in the `cuda` optional dependency group,
since they're dead weight (and `unsloth` won't even import) on a non-CUDA
machine. On the GPU box, install them with:

```bash
uv sync --extra cuda
```

`--method dpo` works around two real `trl`/`transformers` version-compatibility
bugs in `train_lora.py` (`_import_dpo` and the `model.warnings_issued` patch
right before `DPOTrainer` is constructed) — unrelated to this project's code,
they're needed because `trl`'s `DPOTrainer` import chain and `__init__` both
assume APIs that changed in the pinned `transformers` version. Safe to
remove if a future `trl`/`transformers` bump fixes them upstream (try
without them first; the script will error clearly if they're still needed).

A topic-aware system prompt is built automatically (see
`build_system_prompt`/`TOPIC_DESCRIPTIONS` in the script — add an entry there
for each new topic) and prepended to every training example; override it
with `--system-prompt`. `--chat-template {default,chatml,qwen3,mistral}` swaps
in a simple, known-good template from `chat_templates/*.jinja` instead of the
checkpoint's own — useful for keeping formatting (and assistant-only loss
masking via `{% generation %}` tags) consistent across different base
models. `qwen3` matches `chatml` but wraps each assistant turn in an empty
`<think></think>` block, matching Qwen3's non-reasoning training format.
`default` uses the checkpoint's built-in template unchanged. `chatml` and
`qwen3` terminate every turn with `{{ eos_token }}` (the tokenizer's real
end-of-sequence token), not a hard-coded `"<|im_end|>"` string — if those
ever mismatch, `generate()`'s default stopping check never fires on the
literal text, so the model runs on past its turn and hallucinates a new one
(`train_lora.py`'s `stop_token_ids`/`truncate_at_next_turn`, used by both
`run_eval` and `run_inference.py`, guard against this for already-trained
checkpoints that predate this fix).

```bash
# Local smoke test on a laptop (CPU/MPS, tiny model, no push)
uv run climateguard/expert_models/scripts/train_lora.py biodiversity \
    --backend accelerate --quant none \
    --checkpoint Qwen/Qwen2.5-1.5B-Instruct --epochs 1 --limit 20

# Real QLoRA run on a GPU box, combining both topics, pushed to the Hub
uv run climateguard/expert_models/scripts/train_lora.py biodiversity insecurity \
    --backend unsloth --quant 4bit --push

# QLoRA with unsloth on a specific checkpoint, the qwen3 chat template, one epoch
uv run climateguard/expert_models/scripts/train_lora.py biodiversity insecurity \
    --backend unsloth --quant 4bit \
    --checkpoint unsloth/Qwen3.5-9B --chat-template qwen3 \
    --epochs 1 --push

# Same, but DPO instead of SFT
uv run climateguard/expert_models/scripts/train_lora.py biodiversity insecurity \
    --backend unsloth --quant 4bit --method dpo --push

# SFT warm-start, then DPO on the same adapter
uv run climateguard/expert_models/scripts/train_lora.py biodiversity insecurity \
    --backend unsloth --quant 4bit --method sft_dpo --push
```

The adapter is always saved locally to `train_output/adapter`. Pass `--push`
to also upload it (adapter-only) to `DataForGood/<checkpoint>-<topics>-qlora`
(or `-lora` when `--quant none`), or override with `--hub-repo`. Add
`--merge-and-push` to additionally merge the adapter into the base weights
and push a full deployable model — only supported with `--quant none`, since
merging a 4-bit-quantized base isn't (push the QLoRA adapter and merge it
against the full-precision base separately if you need a merged model from a
4-bit run).

### 4. Run inference on a CSV — `run_inference.py`

Pulls a trained model from the Hub (`--hub-repo`, either a LoRA adapter repo
or a merged full model — detected automatically) and runs it over a CSV of
media transcripts, writing an Excel file.

Input CSV columns (required): `id, channel_name, datetime, plaintext, url`.
Output: those columns plus `verdict` and `debunk`.

Two model calls per row, both using the same model with different system
prompts:

1. **Relevance check** — is `plaintext` about the topic at all? Uses a
   dedicated system prompt built from a topic definition (see
   `RELEVANCE_DEFINITIONS` in the script — add an entry per topic; falls
   back to `TOPIC_DESCRIPTIONS` from `train_lora.py` if none is set) asking
   for a `[RELEVANT]`/`[NOT_RELEVANT]` tag. Rows tagged `NOT_RELEVANT` skip
   classification entirely: `verdict` is set straight to `"NOT_RELEVANT"`
   and `debunk` holds the model's one-line justification. An unparseable
   relevance response is treated as relevant (fails open, so a parsing
   hiccup doesn't silently drop a row) rather than as not-relevant. Skip
   this step entirely with `--skip-relevance-check`.
2. **Classification** (only for rows that passed step 1) — the same
   topic-aware system prompt used at training time; `verdict` becomes
   `TRUE`/`FALSE`/`UNPARSEABLE` from the model's `[TRUE]`/`[FALSE]` tag, and
   `debunk` is the rest of the response with the tag stripped.

```bash
# Requires HF_TOKEN with read access to the (private) hub_repo
uv run climateguard/expert_models/scripts/run_inference.py biodiversity \
    --hub-repo DataForGood/Qwen3.5-9B-biodiversity-sft_dpo-qlora --quant 4bit
```

Defaults to `data/inference/input.csv` → `data/inference/output.xlsx` under
the topic directory; override with `--input`/`--output`. `--chat-template`
should match what the model was trained with. `--limit` and `--quant none`
(CPU/MPS) are useful for a quick local test before a real GPU-box run.

See `--help` on any script for the full option list.

## RAG agent (no fine-tuning)

Alternative to the LoRA models: index the parsed reference text and let a base chat model (Qwen3) call a `search_reference` tool.

```bash
# 1. chunk (<=600 chars) + embed -> <topic>/data/rag/{chunks.jsonl,embeddings.npy,meta.json}
uv run python climateguard/expert_models/scripts/build_rag_index.py biodiversity [--embedder e5|bge-m3|mistral]

# 2. run the agent over <topic>/data/inference/input.csv -> output_rag.xlsx
uv run python climateguard/expert_models/scripts/run_rag_agent.py biodiversity --quant none --limit 5 --trace trace.jsonl
```

Verdict is binary: `[FLAGGED]` (falsehood, manipulation or inaction discourse on environmental health) / `[NOT_FLAGGED]`. Prompts are in French; the agent reasons (Qwen3 thinking) before each tool call and may skip retrieval. After every search an explicit evaluation step marks the retrieved snippets `[PERTINENT]` or `[NON_PERTINENT]`; if not relevant, the agent rephrases its query and searches again (up to `--max-tool-rounds`). By default only `FLAGGED` rows are written (`--no-only-flagged` keeps all). The output has `evaluations` and `evidence` columns (`grounded` = a relevant source was found, `ungrounded` = searched but nothing relevant so the verdict rests on the model's own knowledge, `no_search`). Inaction discourse is flagged from its wording alone, without searching.

Same agent on the Mistral API (`mistral-small-2603` by default, needs `MISTRAL_API_KEY`; native function calling, output `output_rag_mistral.xlsx`):

```bash
uv run python climateguard/expert_models/scripts/run_rag_agent_mistral.py biodiversity --limit 5 --trace trace.jsonl
```
