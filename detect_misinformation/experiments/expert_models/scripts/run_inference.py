"""Run a fine-tuned expert model (pulled from the HF Hub) over a CSV of media
transcripts, extract its TRUE/FALSE verdict per row, and write an Excel file.

Input CSV columns (required): id, channel_name, datetime, plaintext, url
Output columns: id, channel_name, datetime, plaintext, url, verdict, debunk

Two model calls per row (same model, different system prompts):

1. Relevance check - is `plaintext` about the topic at all? Uses a separate
   system prompt (see RELEVANCE_SYSTEM_PROMPT_TEMPLATE/RELEVANCE_DEFINITIONS)
   asking for a "[RELEVANT]"/"[NOT_RELEVANT]" tag. Rows the model tags
   NOT_RELEVANT skip classification entirely - `verdict` is set directly to
   "NOT_RELEVANT" and `debunk` holds the model's one-line justification.
   Unparseable relevance responses are treated as relevant (fail open, so a
   parsing hiccup doesn't silently drop a row from classification). Skip
   this step with --skip-relevance-check.

2. Classification (only for rows that passed step 1) - `plaintext` is sent
   as the user message with the same topic-aware system prompt used at
   training time (see build_system_prompt in train_lora.py). The model is
   expected to start its reply with "[TRUE]" or "[FALSE]" (see
   train_lora.py's SYSTEM_PROMPT_TEMPLATE and the "move verdict tag to start
   of response" fix); that tag becomes `verdict` ("TRUE", "FALSE", or
   "UNPARSEABLE" if the model didn't produce a recognizable tag) and the
   rest of the response (tag stripped) becomes `debunk`.

`--hub-repo` can point at either a LoRA adapter repo (pushed via
train_lora.py's default adapter-only push) or a merged full model
(--merge-and-push) - detected automatically from the repo's file listing.

Usage:
    python run_inference.py <model> --hub-repo REPO_ID [--quant 4bit|none]
        [--input PATH] [--output PATH] [--batch-size N] [--limit N] ...

Example:
    python run_inference.py biodiversity \\
        --hub-repo DataForGood/Qwen3.5-9B-biodiversity-sft_dpo-qlora --quant 4bit
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import torch
from dotenv import load_dotenv
from huggingface_hub import HfApi
from tqdm import tqdm

SCRIPTS_DIR = Path(__file__).resolve().parent
EXPERT_MODELS_DIR = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from train_lora import (  # noqa: E402
    TOPIC_DESCRIPTIONS,
    VERDICT_RE,
    apply_chat_template_override,
    build_system_prompt,
    extract_verdict,
    stop_token_ids,
    truncate_at_next_turn,
)

REQUIRED_COLUMNS = ["id", "channel_name", "datetime", "plaintext", "url"]

# Per-topic relevance definitions for the pre-classification filter step.
# Falls back to TOPIC_DESCRIPTIONS (train_lora.py) for any topic without a
# dedicated entry here.
RELEVANCE_DEFINITIONS = {
    "biodiversity": (
        "La santé environnementale est définie ici comme l'ensemble des atteintes à la santé humaine "
        "liées à l'exposition à des facteurs environnementaux physiques, chimiques et biologiques, "
        "ainsi que des politiques, controverses et pratiques de prévention qui leur sont associées.\n"
        "Sont notamment incluses les expositions aux substances chimiques persistantes (PFAS, métaux "
        "lourds tels que le cadmium ou le plomb, pesticides et biocides), la contamination de l'eau, "
        "de l'air, des sols et des chaînes alimentaires, ainsi que les nuisances et polluants tels que "
        "le bruit, les particules fines et les perturbateurs endocriniens.\n"
        "Sont couverts aussi bien les faits d'exposition et leurs impacts sanitaires (cancers, atteintes "
        "neurologiques, troubles de la reproduction) que les seuils réglementaires, les responsabilités "
        "industrielles et les enjeux de reconnaissance et d'indemnisation. Cette thématique est "
        "fréquemment articulée à celles portant sur l'agriculture, l'industrie, l'alimentation et la "
        "régulation sanitaire."
    ),
}

RELEVANCE_SYSTEM_PROMPT_TEMPLATE = """\
You determine whether a media transcript excerpt is relevant to a specific \
topic, defined as follows:

{definition}

Given the excerpt below, decide whether it discusses this topic, even \
indirectly (related debates, policies, controversies, or consequences).

Always start your reply with exactly the tag "[RELEVANT]" or \
"[NOT_RELEVANT]" (nothing before it), then briefly justify your answer in \
one sentence, in the same language as the excerpt.\
"""


def build_relevance_system_prompt(model: str) -> str:
    definition = RELEVANCE_DEFINITIONS.get(model) or TOPIC_DESCRIPTIONS.get(model, model.replace("_", " "))
    return RELEVANCE_SYSTEM_PROMPT_TEMPLATE.format(definition=definition)


RELEVANCE_RE = re.compile(r"\[?\s*(NOT_RELEVANT|RELEVANT)\s*\]?", re.IGNORECASE)


def extract_relevance(text: str) -> bool | None:
    match = RELEVANCE_RE.search(text)
    if not match:
        return None
    return match.group(1).upper() == "RELEVANT"


def strip_tag(text: str, tag_re: re.Pattern) -> str:
    """Remove a leading [TAG] (and following whitespace) from a response,
    leaving just the explanation."""
    match = tag_re.match(text.strip())
    if not match:
        return text.strip()
    return text.strip()[match.end() :].strip()


def repo_is_adapter(hub_repo: str, token: str | None) -> bool:
    files = HfApi().list_repo_files(hub_repo, token=token)
    return "adapter_config.json" in files


def load_model_and_tokenizer(hub_repo: str, quant: str, chat_template: str):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    token = os.getenv("HF_TOKEN")
    is_adapter = repo_is_adapter(hub_repo, token)

    model_kwargs: dict[str, Any] = {"token": token}
    if quant == "4bit":
        from transformers import BitsAndBytesConfig

        if not torch.cuda.is_available():
            raise SystemExit(
                "--quant 4bit requires a CUDA GPU. Use --quant none to run on CPU/MPS."
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
        model_kwargs["torch_dtype"] = torch.float32

    if is_adapter:
        from peft import AutoPeftModelForCausalLM

        print(f"Loading LoRA adapter from {hub_repo} ...")
        model = AutoPeftModelForCausalLM.from_pretrained(hub_repo, **model_kwargs)
    else:
        print(f"Loading full model from {hub_repo} ...")
        model = AutoModelForCausalLM.from_pretrained(hub_repo, **model_kwargs)

    tokenizer = AutoTokenizer.from_pretrained(hub_repo, token=token)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"  # required for correct batched generation
    apply_chat_template_override(tokenizer, chat_template)

    if not torch.cuda.is_available() and quant == "none":
        device = "mps" if torch.backends.mps.is_available() else "cpu"
        model = model.to(device)

    model.eval()
    return model, tokenizer


def run_inference(
    model,
    tokenizer,
    texts: list[str],
    system_prompt: str,
    batch_size: int,
    max_length: int,
    max_new_tokens: int,
) -> list[str]:
    device = next(model.parameters()).device
    if device.type == "mps":
        # generate() hits a fatal (uncatchable) Metal backend assertion with
        # some tensor sizes on Apple Silicon - see train_lora.py's run_eval.
        print("[info] MPS generate() has a known backend crash bug; running inference on CPU instead.")
        model.to("cpu")
        device = torch.device("cpu")
    use_bf16_autocast = device.type == "cuda" and torch.cuda.is_bf16_supported()
    eos_ids = stop_token_ids(tokenizer)

    responses: list[str] = []
    for start in tqdm(range(0, len(texts), batch_size), desc="inference"):
        batch = texts[start : start + batch_size]
        prompts = [
            tokenizer.apply_chat_template(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
                tokenize=False,
                add_generation_prompt=True,
                enable_thinking=False,
            )
            for text in batch
        ]
        inputs = tokenizer(
            prompts,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=max(1, max_length - max_new_tokens),
        ).to(device)

        try:
            with torch.no_grad(), torch.autocast(
                device_type="cuda", dtype=torch.bfloat16, enabled=use_bf16_autocast
            ):
                output_ids = model.generate(
                    **inputs,
                    max_new_tokens=max_new_tokens,
                    do_sample=False,
                    pad_token_id=tokenizer.pad_token_id,
                    eos_token_id=eos_ids,
                )
            for i in range(len(batch)):
                gen_ids = output_ids[i][inputs["input_ids"].shape[1] :]
                text = tokenizer.decode(gen_ids, skip_special_tokens=True).strip()
                responses.append(truncate_at_next_turn(text))
        except RuntimeError as exc:
            print(f"[warn] Generation failed for rows {start}-{start + len(batch)}: {exc!r}")
            responses.extend([""] * len(batch))

    return responses


def main(args: argparse.Namespace) -> None:
    if not args.input.exists():
        raise SystemExit(f"Input CSV not found: {args.input}")

    df = pd.read_csv(args.input, dtype=str, keep_default_na=False)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"Input CSV is missing required columns: {missing}")

    if args.limit:
        df = df.head(args.limit)
    print(f"Loaded {len(df)} rows from {args.input}")

    system_prompt = args.system_prompt or build_system_prompt([args.model])
    print(f"\nSystem prompt:\n{system_prompt}\n")

    relevance_system_prompt = args.relevance_system_prompt or build_relevance_system_prompt(args.model)
    if not args.skip_relevance_check:
        print(f"\nRelevance system prompt:\n{relevance_system_prompt}\n")

    model, tokenizer = load_model_and_tokenizer(args.hub_repo, args.quant, args.chat_template)

    texts = df["plaintext"].tolist()
    n = len(texts)
    verdicts: list[str | None] = [None] * n
    debunks: list[str] = [""] * n

    if args.skip_relevance_check:
        classify_idx = list(range(n))
    else:
        relevance_responses = run_inference(
            model,
            tokenizer,
            texts,
            relevance_system_prompt,
            args.batch_size,
            args.max_length,
            args.relevance_max_new_tokens,
        )
        classify_idx = []
        not_relevant = 0
        for i, response in enumerate(relevance_responses):
            is_relevant = extract_relevance(response)
            if is_relevant is False:
                verdicts[i] = "NOT_RELEVANT"
                debunks[i] = strip_tag(response, RELEVANCE_RE)
                not_relevant += 1
            else:
                # True or unparseable (None): fail open into classification
                # rather than silently dropping a row on a parsing hiccup.
                classify_idx.append(i)
        print(f"\nRelevance: {not_relevant} filtered as NOT_RELEVANT, {len(classify_idx)} proceeding to classification")

    if classify_idx:
        subset_texts = [texts[i] for i in classify_idx]
        responses = run_inference(
            model,
            tokenizer,
            subset_texts,
            system_prompt,
            args.batch_size,
            args.max_length,
            args.max_new_tokens,
        )
        for idx, response in zip(classify_idx, responses):
            is_true = extract_verdict(response)
            verdicts[idx] = "UNPARSEABLE" if is_true is None else ("TRUE" if is_true else "FALSE")
            debunks[idx] = strip_tag(response, VERDICT_RE)

    df["verdict"] = verdicts
    df["debunk"] = debunks

    counts = df["verdict"].value_counts().to_dict()
    print(f"\nVerdict counts: {counts}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(args.output, index=False, engine="openpyxl")
    print(f"Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("model", help="Expert model topic, e.g. 'biodiversity' (used for default paths and the topic-aware system prompt)")
    parser.add_argument("--hub-repo", type=str, required=True, help="HF Hub repo id to load (LoRA adapter or merged model)")
    parser.add_argument("--quant", choices=["4bit", "none"], default="4bit", help="4bit = QLoRA inference (needs CUDA); none = full/half precision, works on CPU/MPS")
    parser.add_argument(
        "--chat-template",
        type=str,
        default="default",
        help="'default' uses the checkpoint's own template, or a name from chat_templates/*.jinja (e.g. 'chatml', 'qwen3', 'mistral') - should match what the model was trained with",
    )
    parser.add_argument("--system-prompt", type=str, default=None, help="Override the auto-generated (topic-aware) system prompt for the classification step")
    parser.add_argument(
        "--skip-relevance-check",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Skip the relevance-filter step and classify every row directly",
    )
    parser.add_argument(
        "--relevance-system-prompt",
        type=str,
        default=None,
        help="Override the auto-generated (topic-definition-based) system prompt for the relevance-check step",
    )
    parser.add_argument(
        "--relevance-max-new-tokens", type=int, default=100, help="Max tokens to generate per row for the relevance-check step"
    )
    parser.add_argument("--input", type=Path, default=None, help="Input CSV (default: expert_models/<model>/data/inference/input.csv)")
    parser.add_argument("--output", type=Path, default=None, help="Output XLSX (default: expert_models/<model>/data/inference/output.xlsx)")
    parser.add_argument("--max-length", type=int, default=4096, help="Total token budget (prompt + generation)")
    parser.add_argument("--max-new-tokens", type=int, default=400, help="Max tokens to generate per row for the classification step")
    parser.add_argument("--batch-size", type=int, default=2 , help="Rows per generation batch")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows (for quick tests)")
    parser.add_argument("--env-file", type=str, default=".env")

    args = parser.parse_args()

    env_path = Path(args.env_file)
    load_dotenv(env_path if env_path.exists() else None)

    if not os.getenv("HF_TOKEN"):
        raise SystemExit("Error: HF_TOKEN not set (needed to pull a private model). Add it to your .env file or environment.")

    model_dir = EXPERT_MODELS_DIR / args.model
    if args.input is None:
        args.input = model_dir / "data" / "inference" / "input.csv"
    if args.output is None:
        args.output = model_dir / "data" / "inference" / "output.xlsx"

    main(args)
