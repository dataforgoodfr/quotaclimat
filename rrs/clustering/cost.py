import math
import random

from rrs.clustering.prompts import (
    SYSTEM_PROMPT,
    _step1_prompt,
    _step2_prompt,
    _step3_prompt,
)
from rrs.clustering.providers import PROVIDER_ANTHROPIC, PROVIDER_MISTRAL
from transformers import AutoTokenizer

# USD per million tokens — update if providers change pricing.
# Mistral: https://mistral.ai/technology/
# Anthropic: https://www.anthropic.com/pricing
_PRICING: dict[str, dict[str, float]] = {
    PROVIDER_MISTRAL: {"input": 0.15, "output": 0.60},
    PROVIDER_ANTHROPIC: {"input": 0.80, "output": 4.00},
}

_AVG_OUTPUT_TOKENS_PER_DOC = 50
_TOKENIZER_REPO = "mistralai/Mistral-Small-3.2-24B-Instruct-2506"
_tokenizer = None


def _pricing_note(provider: str) -> str:
    p = _PRICING.get(provider, _PRICING[PROVIDER_MISTRAL])
    ref = (
        "mistral.ai/technology/"
        if provider == PROVIDER_MISTRAL
        else "anthropic.com/pricing"
    )
    return f"rates: ${p['input']}/M input, ${p['output']}/M output — verify at {ref}"


def _cost(total_input: float, total_output: float, provider: str) -> float:
    p = _PRICING.get(provider, _PRICING[PROVIDER_MISTRAL])
    return (total_input / 1_000_000) * p["input"] + (total_output / 1_000_000) * p[
        "output"
    ]


def _get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        _tokenizer = AutoTokenizer.from_pretrained(_TOKENIZER_REPO)
    return _tokenizer


def _estimate_tokens(text: str) -> int:
    return max(1, len(_get_tokenizer().encode(text)))


def estimate_step1_tokens(
    sentences_by_doc: dict,
    provider: str = PROVIDER_MISTRAL,
    sample_size: int = 20,
) -> None:
    """Sample documents, estimate input tokens locally, and print a cost estimate."""
    doc_ids = list(sentences_by_doc.keys())
    n_docs = len(doc_ids)
    sample = random.sample(doc_ids, min(sample_size, n_docs))

    token_counts: list[int] = []
    for doc_id in sample:
        prompt_text = SYSTEM_PROMPT + _step1_prompt(sentences_by_doc[doc_id])
        token_counts.append(_estimate_tokens(prompt_text))

    avg_input = sum(token_counts) / len(token_counts)
    total_input = avg_input * n_docs
    total_output = _AVG_OUTPUT_TOKENS_PER_DOC * n_docs
    total_cost = _cost(total_input, total_output, provider)

    print(
        f"\n--- Step 1 token estimate ({len(sample)}/{n_docs} docs sampled) [{provider}] ---"
    )
    print(
        f"  Input tokens/doc : avg {avg_input:,.0f}  (min {min(token_counts):,}  max {max(token_counts):,})"
    )
    print(f"  Total input tokens: ~{total_input:,.0f}")
    print(
        f"  Total output tokens: ~{total_output:,.0f}  (est. {_AVG_OUTPUT_TOKENS_PER_DOC} per doc)"
    )
    print(f"  Estimated cost: ~${total_cost:.4f}  ({_pricing_note(provider)})")


def estimate_step2_tokens(
    label_list: list[str],
    batch_size: int = 30,
    provider: str = PROVIDER_MISTRAL,
) -> None:
    """Estimate step 2 cost by simulating hierarchical rounds, assuming 50% reduction per round."""
    n = len(label_list)
    rounds: list[int] = []
    while n > batch_size:
        rounds.append(math.ceil(n / batch_size))
        n = max(1, n // 2)
    rounds.append(1)

    total_calls = sum(rounds)
    sample_labels = label_list[: min(batch_size, len(label_list))]
    tokens_per_call = _estimate_tokens(SYSTEM_PROMPT + _step2_prompt(sample_labels))

    total_input = total_calls * tokens_per_call
    total_output = total_calls * (tokens_per_call // 2)
    total_cost = _cost(total_input, total_output, provider)

    print(
        f"\n--- Step 2 token estimate ({len(rounds)} rounds, calls per round: {rounds}) [{provider}] ---"
    )
    print(f"  Total calls: {total_calls}")
    print(f"  Total input tokens: ~{total_input:,.0f}")
    print(f"  Total output tokens: ~{total_output:,.0f}")
    print(f"  Estimated cost: ~${total_cost:.4f}  ({_pricing_note(provider)})")


def estimate_step3_tokens(
    sentences_by_doc: dict,
    label_list: list[str],
    provider: str = PROVIDER_MISTRAL,
    sample_size: int = 20,
) -> None:
    """Estimate step 3 cost by sampling documents and estimating tokens locally."""
    doc_ids = list(sentences_by_doc.keys())
    n_docs = len(doc_ids)
    sample = random.sample(doc_ids, min(sample_size, n_docs))

    token_counts: list[int] = []
    for doc_id in sample:
        prompt_text = SYSTEM_PROMPT + _step3_prompt(
            sentences_by_doc[doc_id], label_list
        )
        token_counts.append(_estimate_tokens(prompt_text))

    avg_input = sum(token_counts) / len(token_counts)
    total_input = avg_input * n_docs
    total_output = _AVG_OUTPUT_TOKENS_PER_DOC * n_docs
    total_cost = _cost(total_input, total_output, provider)

    print(
        f"\n--- Step 3 token estimate ({len(sample)}/{n_docs} docs sampled) [{provider}] ---"
    )
    print(
        f"  Input tokens/doc : avg {avg_input:,.0f}  (min {min(token_counts):,}  max {max(token_counts):,})"
    )
    print(f"  Total input tokens: ~{total_input:,.0f}")
    print(
        f"  Total output tokens: ~{total_output:,.0f}  (est. {_AVG_OUTPUT_TOKENS_PER_DOC} per doc)"
    )
    print(f"  Estimated cost: ~${total_cost:.4f}  ({_pricing_note(provider)})")
