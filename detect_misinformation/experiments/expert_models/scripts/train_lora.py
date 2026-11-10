"""LoRA fine-tune a chat model on expert-model affirmation/debunk conversations.

Reads the conversation JSONL produced by generate_conversations.py
(data/conversations/<pdf_name>.jsonl) for one or more expert-model topics and
fine-tunes a base chat model on them with TRL, via either SFT or DPO.

Three orthogonal choices:

- `--method {sft,dpo,sft_dpo}` - `sft` (default) supervises directly on the
  debunk text + verdict tag. `dpo` instead preference-tunes: for each
  example, the chosen completion is the debunk text with the correct
  "[TRUE]"/"[FALSE]" tag and the rejected completion is the *same debunk
  text* with the tag flipped - there's no independent second response to
  prefer between in this data, so this isolates the preference signal to
  the TRUE/FALSE calibration specifically (see add_dpo_columns). `sft_dpo`
  runs both, in order, continuing to train the *same* LoRA adapter in the
  DPO stage rather than starting over - the standard SFT-warm-start-then-
  preference-tune recipe. Its two stages have independent learning
  rate/epoch controls (--learning-rate/--epochs for SFT, --dpo-learning-rate
  /--dpo-epochs for DPO), since DPO is typically far more sensitive to a
  high LR than SFT. The verdict eval (see below) runs after each stage when
  --method sft_dpo, so you can see whether the DPO stage actually helped.

- `--backend {unsloth,accelerate}` - which training stack to use.
  `unsloth` is faster and more memory-efficient but needs a CUDA GPU and the
  `unsloth` package. `accelerate` uses plain transformers + peft + trl and
  runs anywhere (CUDA, CPU, or Apple Silicon/MPS).

- `--quant {4bit,none}` - `4bit` performs QLoRA (bitsandbytes NF4
  quantization) and requires a CUDA GPU + the `bitsandbytes` package. `none`
  is a plain LoRA over full/half-precision weights - slower and more memory
  hungry, but the only option that works locally on CPU/MPS. Use it to
  smoke-test the pipeline on a laptop before running real QLoRA training on
  a GPU box.

`unsloth` and `bitsandbytes` are in the `cuda` optional dependency group
(they're dead weight on a non-CUDA machine) - install with
`uv sync --extra cuda` on the GPU box before using --backend unsloth or
--quant 4bit.

A topic-aware system prompt (see build_system_prompt/TOPIC_DESCRIPTIONS) is
prepended to every training example automatically; override it with
--system-prompt. --chat-template swaps in a simple, known-good ChatML or
Mistral template from chat_templates/*.jinja instead of the checkpoint's own
(often more complex) one - useful for keeping formatting, and assistant-only
loss masking, consistent across different base models.

That system prompt instructs the model to always start its reply with an
explicit "[TRUE]" or "[FALSE]" verdict (the tag comes first, not last - a
verdict placed after the explanation competes with the model's pretrained
tendency to stop cleanly once a complete thought is finished, which needs
much more training signal to override than committing to the tag as the
very first token). Training examples are tagged to match (using the
is_true ground truth from generate_conversations.py's metadata), so the
model is actually trained to state the veridicity of the statement, not
just asked to at inference time.

The resulting adapter (and, with --merge-and-push, a merged full model - only
supported with --quant none) is pushed to the Hugging Face Hub as a private
model under the DataForGood org.

After training, the model is run once over the held-out eval set with that
same system prompt; the generated verdict is parsed and compared against
ground truth. Per-example predictions are saved to
<output-dir>/eval_predictions.jsonl and accuracy/precision/recall/F1/
confusion-matrix stats are printed. Skip with --skip-eval.

Usage:
    python train_lora.py <model> [<model> ...] [--backend accelerate|unsloth]
        [--quant 4bit|none] [--checkpoint HF_MODEL_ID] [--push] [--merge-and-push]
        [--hub-repo REPO_ID] [--epochs N] ...

Examples:
    # Local smoke test on a laptop (CPU/MPS, no quantization, no push)
    python train_lora.py biodiversity --backend accelerate --quant none \\
        --checkpoint Qwen/Qwen2.5-1.5B-Instruct --epochs 1 --limit 20

    # Real QLoRA run on a GPU box, combining two topics, pushed to the Hub
    python train_lora.py biodiversity insecurity --backend unsloth --quant 4bit --push

    # Same, but DPO instead of SFT
    python train_lora.py biodiversity insecurity --backend unsloth --quant 4bit --method dpo --push

    # SFT warm-start, then DPO on the same adapter
    python train_lora.py biodiversity insecurity --backend unsloth --quant 4bit --method sft_dpo --push
"""

import argparse
import json
import os
import random
import re
from pathlib import Path
from typing import Any

import torch
from datasets import Dataset
from dotenv import load_dotenv
from huggingface_hub import login
from tqdm import tqdm

EXPERT_MODELS_DIR = Path(__file__).resolve().parents[1]
CHAT_TEMPLATES_DIR = EXPERT_MODELS_DIR / "chat_templates"

DEFAULT_CHECKPOINT = {
    "accelerate": "Qwen/Qwen2.5-7B-Instruct",
    "unsloth": "unsloth/Qwen2.5-7B-Instruct-bnb-4bit",
}

TARGET_MODULES = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]

# One clause per topic, stitched into the system prompt below. Add an entry
# here for every new expert_models/<topic> directory.
TOPIC_DESCRIPTIONS = {
    "biodiversity": (
        "biodiversity, pesticides and industrial agriculture - in particular the "
        "misinformation surrounding the French 'loi Duplomb', pesticide "
        "reintroduction (acetamipride), and their effects on ecosystems and "
        "human health"
    ),
    "insecurity": (
        "migration - common misconceptions and myths about migrants, asylum "
        "seekers and immigration policy"
    ),
}

SYSTEM_PROMPT_TEMPLATE = """\
You are an assistant trained by Data For Good and Quotaclimat to fact-check claims about {topics}.

Your knowledge on these topics comes from verified reference material (books \
and reports written by subject-matter experts) that you were fine-tuned on. \
Always start your reply with exactly the tag "[TRUE]" or "[FALSE]" (nothing \
before it), indicating whether the user's statement is accurate, then \
continue with your explanation:
- If it is accurate, briefly confirm it.
- If it is false, exaggerated, or misleading, say so clearly and explain why \
in a few sentences, citing concrete facts or figures.

Stay strictly grounded in what your training material supports - do not \
speculate or invent facts. Be concise and direct, with no hedging language. \
Always answer in the same language as the user's message.\
"""


def build_system_prompt(models: list[str]) -> str:
    descriptions = [TOPIC_DESCRIPTIONS.get(m, m.replace("_", " ")) for m in models]
    if len(descriptions) == 1:
        topics = descriptions[0]
    else:
        topics = "; and ".join(descriptions)
    return SYSTEM_PROMPT_TEMPLATE.format(topics=topics)


VERDICT_RE = re.compile(r"\[?\s*(TRUE|FALSE)\s*\]?", re.IGNORECASE)


def verdict_tag(is_true: bool) -> str:
    return "[TRUE]" if is_true else "[FALSE]"


def extract_verdict(text: str) -> bool | None:
    match = VERDICT_RE.search(text)
    if not match:
        print(text)
        return None
    return match.group(1).upper() == "TRUE"


def stop_token_ids(tokenizer) -> list[int]:
    """Token ids that should end generation.

    chat_templates/chatml.jinja and qwen3.jinja hard-code the literal turn
    terminator "<|im_end|>" rather than the tokenizer's own eos_token. If a
    checkpoint's real eos_token_id is a *different* token, generate()'s
    default stopping check (tokenizer.eos_token_id only) never fires on
    "<|im_end|>" - so the model runs past the end of its turn and, with
    nothing telling it to stop, falls back on its pretrained instinct to
    keep going by hallucinating a new "user" turn and replying to it, then
    another, until max_new_tokens is exhausted. Stop on both.
    """
    ids = {tokenizer.eos_token_id}
    im_end_id = tokenizer.convert_tokens_to_ids("<|im_end|>")
    if im_end_id is not None and im_end_id != tokenizer.unk_token_id:
        ids.add(im_end_id)
    return list(ids)


TURN_MARKER_RE = re.compile(r"\n\s*(user|assistant|system)\s*\n")


def truncate_at_next_turn(text: str) -> str:
    """Defense in depth: if a runaway generation still slipped past
    stop_token_ids (e.g. an unanticipated template), cut it at the first
    sign of a hallucinated new turn rather than keeping the whole thing."""
    match = TURN_MARKER_RE.search(text)
    return text[: match.start()].strip() if match else text


# ── Data ─────────────────────────────────────────────────────────────────────


def load_conversations(models: list[str], limit: int | None) -> Dataset:
    rows: list[dict[str, Any]] = []
    for model_name in models:
        conv_dir = EXPERT_MODELS_DIR / model_name / "data" / "conversations"
        jsonl_files = sorted(conv_dir.glob("*.jsonl"))
        if not jsonl_files:
            raise SystemExit(f"No conversation JSONL files found in {conv_dir}")
        for path in jsonl_files:
            with path.open(encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    meta = rec.get("metadata", {})
                    rows.append(
                        {
                            "messages": rec["messages"],
                            "affirmation": rec["messages"][0]["content"],
                            "source_id": f"{model_name}:{meta.get('source_id', '')}",
                            "is_true": meta.get("is_true"),
                            "topic": model_name,
                        }
                    )
    if not rows:
        raise SystemExit("No conversation records found.")
    if limit:
        rows = rows[:limit]
    return Dataset.from_list(rows)


def group_train_test_split(dataset: Dataset, test_size: float, seed: int) -> tuple[Dataset, Dataset]:
    """Split by source_id so pairs generated from the same excerpt don't leak
    across train/test."""
    ids = sorted(set(dataset["source_id"]))
    rng = random.Random(seed)
    rng.shuffle(ids)
    n_test = max(1, int(len(ids) * test_size))
    test_ids = set(ids[:n_test])
    train_idx = [i for i, sid in enumerate(dataset["source_id"]) if sid not in test_ids]
    test_idx = [i for i, sid in enumerate(dataset["source_id"]) if sid in test_ids]
    return dataset.select(train_idx), dataset.select(test_idx)


def add_chat_text(dataset: Dataset, tokenizer, system_prompt: str) -> Dataset:
    def _format(example):
        user_msg, assistant_msg = example["messages"]
        tagged_assistant = {
            "role": "assistant",
            "content": f"{verdict_tag(example['is_true'])} {assistant_msg['content']}",
        }
        messages = [{"role": "system", "content": system_prompt}, user_msg, tagged_assistant]
        return {
            "text": tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=False,
                # Debunks are short, direct answers - never train the model to
                # emit a <think> block, regardless of the checkpoint's default.
                enable_thinking=False,
            )
        }

    return dataset.map(_format)


def add_dpo_columns(dataset: Dataset, system_prompt: str) -> Dataset:
    """Build prompt/chosen/rejected columns for DPO training.

    There's no naturally-occurring second (worse) response to prefer
    against - generate_conversations.py produces one correct debunk per
    affirmation, not multiple candidates. So the rejected completion reuses
    the exact same debunk text with the *opposite* verdict tag: chosen is
    "[TRUE] <debunk>"/"[FALSE] <debunk>" (matching the ground truth),
    rejected is the same text with the tag flipped. This isolates the
    preference signal specifically to the TRUE/FALSE calibration - the same
    behavior the tagged SFT training targets - rather than general response
    quality/style, which this data can't support a meaningful preference
    over. The tag is the *first* token specifically so chosen/rejected
    diverge immediately - DPO's per-token logprob signal is strongest right
    where the two sequences first differ, and a tag buried after a long
    shared explanation gives a much weaker training signal. DPOTrainer
    applies the chat template itself (via processing_class), so unlike
    add_chat_text this doesn't need the tokenizer.
    """

    def _format(example):
        user_msg, assistant_msg = example["messages"]
        return {
            "prompt": [{"role": "system", "content": system_prompt}, user_msg],
            "chosen": [
                {
                    "role": "assistant",
                    "content": f"{verdict_tag(example['is_true'])} {assistant_msg['content']}",
                }
            ],
            "rejected": [
                {
                    "role": "assistant",
                    "content": f"{verdict_tag(not example['is_true'])} {assistant_msg['content']}",
                }
            ],
            # Same non-thinking-mode requirement as SFT training - see add_chat_text.
            "chat_template_kwargs": {"enable_thinking": False},
        }

    # DPOTrainer validates that each example's keys exactly match one of a
    # fixed set of supported combinations; "messages" alongside "prompt"/
    # "chosen"/"rejected" isn't one of them, so drop it (run_eval uses the
    # separate "affirmation" column instead - see load_conversations).
    return dataset.map(_format, remove_columns=["messages"])


def apply_chat_template_override(tokenizer, name: str) -> None:
    """Replace the tokenizer's chat template with one of chat_templates/*.jinja.

    A model's built-in template can be verbose or inconsistent across
    checkpoints; pinning a simple, known-good ChatML/Mistral template makes
    training (and assistant-only loss masking via {% generation %} tags)
    consistent regardless of which base model is used.
    """
    if name == "default":
        return
    template_path = CHAT_TEMPLATES_DIR / f"{name}.jinja"
    if not template_path.exists():
        available = ", ".join(p.stem for p in sorted(CHAT_TEMPLATES_DIR.glob("*.jinja")))
        raise SystemExit(f"Unknown --chat-template '{name}'. Available: default, {available}")
    tokenizer.chat_template = template_path.read_text()


# ── Model loading ────────────────────────────────────────────────────────────


def load_model_and_tokenizer(args: argparse.Namespace):
    if args.backend == "unsloth":
        return _load_unsloth(args)
    return _load_accelerate(args)


def _load_unsloth(args: argparse.Namespace):
    try:
        from unsloth import FastLanguageModel
    except ImportError as exc:
        raise SystemExit(
            "The 'unsloth' backend requires a CUDA GPU and the unsloth package. "
            "Install it with the 'cuda' extra: uv sync --extra cuda. "
            "Use --backend accelerate to run without it."
        ) from exc

    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name=args.checkpoint,
        max_seq_length=args.max_length,
        dtype=None,
        load_in_4bit=args.quant == "4bit",
        token=os.getenv("HF_TOKEN"),
    )
    apply_chat_template_override(tokenizer, args.chat_template)
    model = FastLanguageModel.get_peft_model(
        model,
        r=args.lora_r,
        target_modules=TARGET_MODULES,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        bias="none",
        use_gradient_checkpointing="unsloth",
        random_state=args.seed,
    )
    return model, tokenizer


def _load_accelerate(args: argparse.Namespace):
    from peft import LoraConfig, TaskType, get_peft_model
    from transformers import AutoModelForCausalLM, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(args.checkpoint, token=os.getenv("HF_TOKEN"))
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    apply_chat_template_override(tokenizer, args.chat_template)

    model_kwargs: dict[str, Any] = {"token": os.getenv("HF_TOKEN")}

    if args.quant == "4bit":
        try:
            from transformers import BitsAndBytesConfig
        except ImportError as exc:
            raise SystemExit(
                "--quant 4bit requires a CUDA GPU and the bitsandbytes package. "
                "Install it with the 'cuda' extra: uv sync --extra cuda. "
                "Use --quant none to test locally."
            ) from exc
        if not torch.cuda.is_available():
            raise SystemExit(
                "--quant 4bit (QLoRA) requires a CUDA GPU. Use --quant none to "
                "run a plain LoRA locally on CPU/MPS."
            )
        model_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16,
            bnb_4bit_use_double_quant=True,
        )
        model_kwargs["device_map"] = "auto"
    elif torch.cuda.is_available():
        model_kwargs["torch_dtype"] = torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
        model_kwargs["device_map"] = "auto"
    else:
        # CPU/MPS fallback for local smoke testing.
        model_kwargs["torch_dtype"] = torch.float32

    model = AutoModelForCausalLM.from_pretrained(args.checkpoint, **model_kwargs)

    if args.quant == "4bit":
        from peft import prepare_model_for_kbit_training

        model = prepare_model_for_kbit_training(model)

    if not torch.cuda.is_available() and args.quant == "none":
        device = "mps" if torch.backends.mps.is_available() else "cpu"
        model = model.to(device)

    lora_config = LoraConfig(
        r=args.lora_r,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        target_modules=TARGET_MODULES,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()
    return model, tokenizer


# ── Eval ─────────────────────────────────────────────────────────────────────


def run_eval(
    model,
    tokenizer,
    eval_dataset: Dataset,
    system_prompt: str,
    backend: str,
    max_length: int,
    max_new_tokens: int,
    output_dir: Path,
) -> None:
    """Generate a verdict + answer for every eval example, compare the verdict
    to the is_true ground truth, and print classification stats."""
    if backend == "unsloth":
        from unsloth import FastLanguageModel

        FastLanguageModel.for_inference(model)
    model.eval()
    device = next(model.parameters()).device

    if device.type == "mps":
        # generate() hits a fatal (uncatchable) Metal backend assertion with
        # some tensor sizes on Apple Silicon; run eval generation on CPU
        # instead. Slower, but this only affects local smoke tests - real
        # training runs on CUDA, which isn't affected.
        print("[info] MPS generate() has a known backend crash bug; running eval on CPU instead.")
        model.to("cpu")
        device = torch.device("cpu")

    # PEFT keeps LoRA A/B matrices in float32 for numerical stability even on
    # a bf16/4-bit-compute base model. SFTTrainer's forward pass handles that
    # dtype mix internally via autocast; a bare model.generate() call here
    # doesn't, and raises "expected scalar type BFloat16 but found Float"
    # without it.
    use_bf16_autocast = device.type == "cuda" and torch.cuda.is_bf16_supported()
    eos_ids = stop_token_ids(tokenizer)

    rows = []
    for example in tqdm(eval_dataset, desc="eval"):
        affirmation = example["affirmation"]
        chat = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": affirmation},
        ]
        prompt_text = tokenizer.apply_chat_template(
            chat, tokenize=False, add_generation_prompt=True, enable_thinking=False
        )
        inputs = tokenizer(
            prompt_text,
            return_tensors="pt",
            truncation=True,
            max_length=max(1, max_length - max_new_tokens),
        ).to(device)

        try:
            with torch.no_grad(), torch.autocast(
                device_type="cuda", dtype=torch.bfloat16, enabled=use_bf16_autocast
            ):
                output_ids = model.generate(
                    **inputs, max_new_tokens=max_new_tokens, do_sample=False, eos_token_id=eos_ids
                )
            response = tokenizer.decode(
                output_ids[0][inputs["input_ids"].shape[1] :], skip_special_tokens=True
            ).strip()
            response = truncate_at_next_turn(response)
        except RuntimeError as exc:
            # A single generation failure (e.g. an MPS backend allocation bug,
            # or a transient CUDA OOM) shouldn't lose the rest of the eval pass.
            print(f"[warn] Generation failed for {example['source_id']}: {exc!r}")
            response = ""

        ground_truth = bool(example["is_true"])
        predicted = extract_verdict(response)
        rows.append(
            {
                "source_id": example["source_id"],
                "affirmation": affirmation,
                "ground_truth_is_true": ground_truth,
                "predicted_is_true": predicted,
                "correct": None if predicted is None else predicted == ground_truth,
                "response": response,
            }
        )

    results_path = output_dir / "eval_predictions.jsonl"
    with results_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"Eval predictions saved to {results_path}")

    tp = tn = fp = fn = unparseable = 0
    for row in rows:
        if row["predicted_is_true"] is None:
            unparseable += 1
        elif row["ground_truth_is_true"] and row["predicted_is_true"]:
            tp += 1
        elif not row["ground_truth_is_true"] and not row["predicted_is_true"]:
            tn += 1
        elif not row["ground_truth_is_true"] and row["predicted_is_true"]:
            fp += 1
        else:
            fn += 1

    total = tp + tn + fp + fn
    print("\n── Eval verdict accuracy (TRUE = affirmation is accurate) ───")
    print(f"  Total evaluated : {len(rows)}  (unparseable verdict: {unparseable})")
    if total == 0:
        print("  No parseable verdicts to score.")
    else:
        accuracy = (tp + tn) / total
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        print(f"  Accuracy        : {accuracy:.3f}")
        print(f"  Precision (TRUE): {precision:.3f}")
        print(f"  Recall (TRUE)   : {recall:.3f}")
        print(f"  F1 (TRUE)       : {f1:.3f}")
        print("\n  Confusion matrix:")
        print("                     Pred TRUE      Pred FALSE")
        print(f"  GT  TRUE               {tp:>5}          {fn:>5}")
        print(f"  GT  FALSE              {fp:>5}          {tn:>5}")
    print("───────────────────────────────────────────────────────────")


# ── Hub push ─────────────────────────────────────────────────────────────────


def push_adapter(model, tokenizer, repo_id: str):
    print(f"Pushing adapter to {repo_id} (private) ...")
    model.push_to_hub(repo_id, token=os.getenv("HF_TOKEN"), private=True)
    tokenizer.push_to_hub(repo_id, token=os.getenv("HF_TOKEN"), private=True)


def merge_and_push(model, tokenizer, repo_id: str, output_dir: Path):
    print("Merging LoRA adapter into base weights ...")
    merged = model.merge_and_unload()
    merged_dir = output_dir / "merged"
    merged_dir.mkdir(parents=True, exist_ok=True)
    merged.save_pretrained(merged_dir)
    tokenizer.save_pretrained(merged_dir)
    print(f"Pushing merged model to {repo_id} (private) ...")
    merged.push_to_hub(repo_id, token=os.getenv("HF_TOKEN"), private=True)
    tokenizer.push_to_hub(repo_id, token=os.getenv("HF_TOKEN"), private=True)


# ── Main ─────────────────────────────────────────────────────────────────────


def default_repo_id(args: argparse.Namespace) -> str:
    checkpoint_slug = args.checkpoint.split("/")[-1]
    topics = "-".join(args.models)
    suffix = "qlora" if args.quant == "4bit" else "lora"
    return f"DataForGood/{checkpoint_slug}-{topics}-{args.method}-{suffix}"


# (stage_method, learning_rate, epochs) tuples to run, in order, for each --method.
def training_stages(args: argparse.Namespace) -> list[tuple[str, float, int]]:
    if args.method == "sft":
        return [("sft", args.learning_rate, args.epochs)]
    if args.method == "dpo":
        return [("dpo", args.dpo_learning_rate, args.dpo_epochs)]
    return [  # sft_dpo: SFT warm-start, then DPO continues training the same adapter
        ("sft", args.learning_rate, args.epochs),
        ("dpo", args.dpo_learning_rate, args.dpo_epochs),
    ]


def _import_dpo():
    """DPOTrainer's import chain hits a real trl/transformers incompatibility:
    trl.import_utils computes several `_xxx_available` flags by calling
    transformers' `_is_package_available`, which returns a `(bool, version)`
    tuple - but trl assigns that directly to a flag it later reads as a plain
    bool in `if is_xxx_available():` guards (e.g. for the optional mergekit/
    llm_blender-based callbacks). A non-empty tuple is always truthy in
    Python, so those guards always take the "available" branch and try to
    import packages that aren't installed - and, for mergekit specifically,
    can never be installed here, since it pins pydantic<2.11 against this
    project's pydantic>=2.11.9. Fix the flags in place before importing the
    DPO trainer, which pulls in that chain (callbacks.py -> judges.py).
    """
    import trl.import_utils as trl_import_utils

    for name in dir(trl_import_utils):
        if name.startswith("_") and name.endswith("_available"):
            value = getattr(trl_import_utils, name)
            if isinstance(value, tuple):
                setattr(trl_import_utils, name, bool(value[0]))

    from trl import DPOConfig, DPOTrainer

    return DPOConfig, DPOTrainer


def run_training_stage(
    args: argparse.Namespace,
    method: str,
    learning_rate: float,
    epochs: int,
    model,
    tokenizer,
    base_dataset: Dataset,
    system_prompt: str,
    output_dir: Path,
) -> Dataset:
    """Build the method-specific dataset columns, train one stage, return the
    eval split used (so the caller can run the shared verdict eval on it).

    `model` is trained in place - for the "sft_dpo" method this is called
    twice on the same peft model, so the DPO stage continues optimizing the
    LoRA weights the SFT stage already trained, rather than starting fresh.
    """
    if method == "sft":
        dataset = add_chat_text(base_dataset, tokenizer, system_prompt)
    else:
        dataset = add_dpo_columns(base_dataset, system_prompt)
    train_dataset, eval_dataset = group_train_test_split(dataset, args.test_size, args.seed)
    print(f"[{method}] Train: {len(train_dataset)}  Eval: {len(eval_dataset)}")
    if method == "sft":
        print(f"\n[{method}] Sample:\n{train_dataset[0]['text']}\n")
    else:
        print(f"\n[{method}] Sample chosen:\n{train_dataset[0]['chosen']}\n")
        print(f"[{method}] Sample rejected:\n{train_dataset[0]['rejected']}\n")

    max_steps = epochs * max(
        1, -(-len(train_dataset) // (args.train_batch_size * args.gradient_accumulation_steps))
    )
    common_kwargs = dict(
        output_dir=str(output_dir),
        learning_rate=learning_rate,
        per_device_train_batch_size=args.train_batch_size,
        per_device_eval_batch_size=args.eval_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        weight_decay=args.weight_decay,
        warmup_steps=min(10, max_steps // 10 or 1),
        max_steps=max_steps,
        eval_strategy="steps",
        logging_strategy="steps",
        logging_steps=max(1, max_steps // 20),
        eval_steps=max(1, max_steps // 10),
        save_strategy="no",
        lr_scheduler_type="linear",
        max_grad_norm=args.max_grad_norm,
        bf16=torch.cuda.is_available() and torch.cuda.is_bf16_supported(),
        fp16=torch.cuda.is_available() and not torch.cuda.is_bf16_supported(),
        report_to="wandb" if args.wandb else "none",
    )

    if method == "sft":
        from trl import SFTConfig, SFTTrainer

        training_args = SFTConfig(
            dataset_text_field="text", max_length=args.max_length, packing=False, **common_kwargs
        )
        trainer_cls, trainer_kwargs = SFTTrainer, {}
    else:
        DPOConfig, DPOTrainer = _import_dpo()
        training_args = DPOConfig(beta=args.dpo_beta, max_length=args.max_length, **common_kwargs)
        trainer_cls, trainer_kwargs = DPOTrainer, {}
        if not hasattr(model, "warnings_issued"):
            # DPOTrainer.__init__ unconditionally sets model.warnings_issued[...],
            # an attribute transformers no longer initializes on PreTrainedModel.
            model.warnings_issued = {}

    if args.wandb:
        import wandb

        wandb.login(key=os.getenv("WANDB_KEY"))
        wandb.init(
            entity=os.getenv("WANDB_ENTITY", "gmguarino"),
            project="expert-models-lora",
            config=training_args.to_dict(),
            name=f"{'-'.join(args.models)}-{args.checkpoint.split('/')[-1]}-{args.backend}-{args.quant}-{method}",
            reinit=True,
        )

    trainer = trainer_cls(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        processing_class=tokenizer,
        **trainer_kwargs,
    )
    trainer.train()

    if args.wandb:
        import wandb

        wandb.finish()

    return eval_dataset


def main(args: argparse.Namespace) -> None:
    if os.getenv("HF_TOKEN"):
        login(token=os.getenv("HF_TOKEN"))

    output_dir = Path(args.output_dir) if args.output_dir else EXPERT_MODELS_DIR / "train_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = load_conversations(args.models, args.limit)
    print(f"Loaded {len(dataset)} conversation examples from: {', '.join(args.models)}")

    system_prompt = args.system_prompt or build_system_prompt(args.models)
    print(f"\nSystem prompt:\n{system_prompt}\n")

    model, tokenizer = load_model_and_tokenizer(args)

    eval_dataset = None
    for method, learning_rate, epochs in training_stages(args):
        print(f"\n=== Stage: {method} (lr={learning_rate}, epochs={epochs}) ===")
        eval_dataset = run_training_stage(
            args, method, learning_rate, epochs, model, tokenizer, dataset, system_prompt, output_dir
        )
        if not args.skip_eval:
            print(f"\n=== Eval after {method} stage ===")
            run_eval(
                model,
                tokenizer,
                eval_dataset,
                system_prompt,
                args.backend,
                args.max_length,
                args.eval_max_new_tokens,
                output_dir,
            )

    adapter_dir = output_dir / "adapter"
    model.save_pretrained(adapter_dir)
    tokenizer.save_pretrained(adapter_dir)
    print(f"Adapter saved to {adapter_dir}")

    if not args.push:
        print("Skipping Hub push (pass --push to publish).")
        return

    repo_id = args.hub_repo or default_repo_id(args)

    if args.merge_and_push:
        if args.quant == "4bit":
            print(
                "Warning: merging a 4-bit QLoRA adapter back into the base model "
                "is not supported here - pushing the adapter only. Re-run with "
                "--quant none to produce a mergeable full model."
            )
            push_adapter(model, tokenizer, repo_id)
        else:
            merge_and_push(model, tokenizer, repo_id, output_dir)
    else:
        push_adapter(model, tokenizer, repo_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("models", nargs="+", help="Expert model topic(s), e.g. 'biodiversity' 'insecurity'")
    parser.add_argument("--backend", choices=["accelerate", "unsloth"], default="accelerate")
    parser.add_argument("--quant", choices=["4bit", "none"], default="4bit", help="4bit = QLoRA (needs CUDA); none = plain LoRA, works on CPU/MPS")
    parser.add_argument(
        "--method",
        choices=["sft", "dpo", "sft_dpo"],
        default="sft",
        help="sft = supervised fine-tuning on the debunk text; dpo = preference-tune the TRUE/FALSE "
        "verdict by preferring the correct tag over the flipped one for the same debunk text; "
        "sft_dpo = run sft first, then continue training the same LoRA adapter with dpo",
    )
    parser.add_argument(
        "--dpo-beta", type=float, default=0.1, help="DPO KL penalty coefficient (used with --method dpo or sft_dpo)"
    )
    parser.add_argument(
        "--dpo-learning-rate",
        type=float,
        default=5e-6,
        help="Learning rate for the DPO stage (used with --method dpo or sft_dpo) - "
        "DPO is typically far more sensitive to a high LR than SFT, so this is independent of --learning-rate",
    )
    parser.add_argument(
        "--dpo-epochs",
        type=int,
        default=1,
        help="Epochs for the DPO stage (used with --method dpo or sft_dpo)",
    )
    parser.add_argument("--checkpoint", type=str, default=None, help="Base model (default depends on --backend)")
    parser.add_argument(
        "--chat-template",
        type=str,
        default="default",
        help="'default' uses the checkpoint's own template, or a name from chat_templates/*.jinja (e.g. 'chatml', 'qwen3', 'mistral')",
    )
    parser.add_argument(
        "--system-prompt",
        type=str,
        default=None,
        help="Override the auto-generated (topic-aware) system prompt prepended to every example",
    )
    parser.add_argument("--max-length", type=int, default=2048)
    parser.add_argument("--learning-rate", type=float, default=2e-4)
    parser.add_argument("--train-batch-size", type=int, default=2)
    parser.add_argument("--eval-batch-size", type=int, default=2)
    parser.add_argument("--gradient-accumulation-steps", type=int, default=4)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--max-grad-norm", type=float, default=0.3)
    parser.add_argument("--lora-r", type=int, default=16)
    parser.add_argument("--lora-alpha", type=int, default=32)
    parser.add_argument("--lora-dropout", type=float, default=0.05)
    parser.add_argument("--test-size", type=float, default=0.15)
    parser.add_argument("--seed", type=int, default=3407)
    parser.add_argument("--limit", type=int, default=None, help="Cap the number of examples (for quick local tests)")
    parser.add_argument("--output-dir", type=str, default=None, help="Where to save the adapter (default: expert_models/train_output)")
    parser.add_argument("--push", action=argparse.BooleanOptionalAction, default=False, help="Push the result to the Hugging Face Hub")
    parser.add_argument("--merge-and-push", action=argparse.BooleanOptionalAction, default=False, help="Also merge the adapter into the base model before pushing (only with --quant none)")
    parser.add_argument("--hub-repo", type=str, default=None, help="Override the Hub repo id (default: DataForGood/<checkpoint>-<topics>-<lora|qlora>)")
    parser.add_argument(
        "--skip-eval",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Skip the post-training TRUE/FALSE verdict eval pass over the eval set",
    )
    parser.add_argument(
        "--eval-max-new-tokens",
        type=int,
        default=300,
        help="Max new tokens to generate per eval example",
    )
    parser.add_argument("--wandb", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--env-file", type=str, default=".env")

    args = parser.parse_args()

    env_path = Path(args.env_file)
    load_dotenv(env_path if env_path.exists() else None)

    if args.checkpoint is None:
        args.checkpoint = DEFAULT_CHECKPOINT[args.backend]

    if args.push and not os.getenv("HF_TOKEN"):
        raise SystemExit("Error: --push requires HF_TOKEN set (in .env or the environment).")

    main(args)
