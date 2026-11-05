# Misinformation detection experiments

Research code migrated from the `climateguard` repository: model fine-tuning (`finetuning/`: DeBERTa, ModernBERT, SetFit, LLM SFT/DPO/GRPO, Mistral/OpenAI fine-tuning APIs), claim extraction / matching / NLI experiments (`experiments/`) and topic expert models with RAG (`expert_models/`).

This code is not deployed: production jobs are in `../jobs`.

## Environment

The dependencies are optional Poetry groups of the root `pyproject.toml`:

```bash
poetry install --with misinformation-dev                           # CPU / Mac
poetry install --with misinformation-dev,misinformation-dev-cuda   # GPU machine (unsloth, bitsandbytes)
```

### CUDA torch

Poetry resolves every group into a single `poetry.lock`, which holds a single `torch`: the CPU build from the `pytorch-cpu` source (shared with the `rrs` group, whose Docker image must stay small). On a GPU machine, replace it with the CUDA build after `poetry install`, picking the index that matches the CUDA driver:

```bash
poetry run pip install --force-reinstall torch --index-url https://download.pytorch.org/whl/cu128
```

A later `poetry install` / `poetry sync` reinstalls the CPU build: rerun the command above.

### Differences with the former climateguard environment

- `anthropic` follows the `rrs` group (`^0.102.0`) instead of `>=0.121.0`.
- `pandas` follows main's `modin` (`<2.3`) instead of `>=2.3.2`.
- `psycopg2` is replaced by `psycopg2-binary`.
