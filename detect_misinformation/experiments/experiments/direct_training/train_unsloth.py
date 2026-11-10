import argparse
import logging
import math
import os
import re
import uuid
from typing import Optional, Union

import numpy as np
import pandas as pd
import torch
import unsloth
from datasets import DatasetDict, concatenate_datasets, load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from sklearn.metrics import classification_report
from tqdm import tqdm
from trl import SFTConfig, SFTTrainer
from unsloth import FastLanguageModel
from unsloth.chat_templates import get_chat_template

import wandb

load_dotenv()
login(token=os.getenv("HF_TOKEN"))


def get_data(args):
    dataset = load_dataset("DataForGood/climateguard")

    dataset = dataset.filter(lambda example: example["comments"] == [])
    # dataset = dataset.filter(lambda example: example["year"] == 2025)
    dataset = dataset.filter(
        lambda example: isinstance(example["misinformation"], bool)
    )
    dataset = dataset.filter(
        lambda example: not isinstance(example["misinformation"], str)
    )
    dataset = dataset.map(
        lambda example: {
            "text": example["plaintext"],
            "value": int(example["misinformation"]),
        }
    )
    if args.test_split == "time":
        concat_dataset = concatenate_datasets([dataset["train"], dataset["test"]])

        concat_dataset = concat_dataset.sort(["year", "month", "day"])
        stop_index = math.floor(len(concat_dataset) * 0.80)
        stop_date = pd.to_datetime(
            concat_dataset.select([stop_index])["start"][0]
        ).date()
        print(f"Setting train data cutoff at {stop_date.strftime('%Y-%m-%d')}")
        _train_data = concat_dataset.filter(
            lambda example: pd.to_datetime(example["start"]).date() <= stop_date
        )
        _test_data = concat_dataset.filter(
            lambda example: pd.to_datetime(example["start"]).date() > stop_date
        )
        dataset = DatasetDict(
            {
                "train": _train_data,
                "test": _test_data,
            }
        )
    if args.balance_data:
        positive_cases = dataset["train"].filter(
            lambda example: example["misinformation"]
        )
        negative_cases = (
            dataset["train"]
            .filter(lambda example: not example["misinformation"])
            .shuffle()
            .select(range(len(positive_cases)))
        )
        dataset["train"] = concatenate_datasets([positive_cases, negative_cases])
    dataset = dataset.select_columns(["id", "text", "value"])
    # Print dataset stats:
    print("Train dataset example split:")
    print(dataset["train"].to_pandas()["value"].value_counts())
    print("Test dataset example split:")
    print(dataset["test"].to_pandas()["value"].value_counts())
    return dataset


def parse_response(response: Optional[Union[int, str]]):
    """Parse response containing only a score."""
    if isinstance(response, int):
        return response
    match = re.match(r"^[^\d]*(\d+)", response)
    if match:
        score = int(match.group(1))  # Extract score as an integer
    else:
        print("Could not parse response")
        score = 0
    return score


def test_model(args, test_dataset, model, tokenizer, max_new_tokens, device="cuda"):
    print("Evaluating model on test set...")
    model.eval()
    results = []
    raw_results = []
    for example in tqdm(test_dataset):
        input_conv = [example["messages"][0]]
        chat_args = dict(
            tokenize=False,
            add_generation_prompt=False,
        )
        if args.reasoning_model:
            chat_args.update(dict(enable_thinking=False))
        inputs = tokenizer.apply_chat_template(input_conv, **chat_args)
        inputs = tokenizer(
            text=inputs,
            return_tensors="pt",
            max_length=args.max_length - max_new_tokens,
        )
        inputs = inputs.to(device)
        with torch.no_grad():
            output_tokens = model.generate(**inputs, max_new_tokens=max_new_tokens)
        prediction = tokenizer.decode(
            output_tokens[0][inputs["input_ids"].size(1) :], skip_special_tokens=True
        )
        raw_results.append(prediction)
        results.append(int(parse_response(prediction)))

    report = classification_report(
        test_dataset.to_pandas()["value"].astype(int),
        results,
    )
    print(f"Classification on test set: \n{report}")
    df_results = pd.DataFrame(
        {
            "predictions": results,
            "labels": test_dataset.to_pandas()["value"].astype(int),
            "responses": raw_results,
        }
    )
    print(df_results.head())
    df_results.to_csv("tests.csv", index=False)


def formatting_prompts_func(examples, tokenizer, args):
    convos = examples["messages"]
    chat_args = dict(
        tokenize=False,
        add_generation_prompt=False,
    )
    if args.reasoning_model:
        chat_args.update(dict(enable_thinking=False))
    chats = [tokenizer.apply_chat_template(convo, **chat_args) for convo in convos]
    return {
        "chat": chats,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-length", type=int, default=4096)
    parser.add_argument("--max-new-tokens", type=int, default=512)
    parser.add_argument("--learning-rate", type=float, default=1e-4)
    parser.add_argument("--train-batch-size", type=int, default=8)
    parser.add_argument("--eval-batch-size", type=int, default=8)
    parser.add_argument("--gradient-accumulation-steps", type=int, default=1)
    parser.add_argument("--lora-r", type=int, default=16)
    parser.add_argument("--lora-alpha", type=int, default=16)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--max-grad-norm", type=float, default=0.1)
    parser.add_argument("--epochs", type=int, default="10")
    parser.add_argument(
        "--checkpoint",
        type=str,
        default="unsloth/Mistral-Small-3.2-24B-Instruct-2506-bnb-4bit",
    )
    parser.add_argument("--chat-template", type=str, default="default")
    parser.add_argument("--test-split", type=str, default="default")
    parser.add_argument("--balance-data", action=argparse.BooleanOptionalAction)
    parser.add_argument("--wandb", action=argparse.BooleanOptionalAction)
    parser.add_argument("--lora-4-bit", action=argparse.BooleanOptionalAction)
    parser.add_argument("--lora-8-bit", action=argparse.BooleanOptionalAction)
    parser.add_argument("--lora-16-bit", action=argparse.BooleanOptionalAction)
    parser.add_argument("--reasoning-model", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    print(args)

    if args.wandb:
        print("reporting to wandb")
        wandb.login(key=os.getenv("WANDB_KEY"))

    OUTPUT_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "climateguard_train"
    )
    prompt = """You are an assistant helping editors to moderate TV and radio content.
You will be provided with a transcript delimited by triple backticks.
Bare in mind that the transcript may be missing punctuation and may be of very low quality, with incorrect vocabulary, cuts in the wrong places, or may include some phonetic transcription.

Does the text promote climate change misinformation that undermines well-established scientific consensus, such as denying the existence of climate change or the factors that contribute to it ?

Answer just a number between 0 - does not promote - and 1 - certainly promotes.

text: {transcript}"""

    device = "cuda" if torch.cuda.is_available() else None
    if not device:
        device = "mps" if torch.backends.mps.is_available() else "cpu"

    logging.info(f"Using device: {device}")
    logging.info(f"bfloat available:: {torch.cuda.is_bf16_supported()}")

    if not args.lora_4_bit and not args.lora_8_bit and not args.lora_16_bit:
        args.lora_4_bit = True

    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name=args.checkpoint,
        max_seq_length=args.max_length,
        dtype=None,
        load_in_4bit=True if args.lora_4_bit else False,
        load_in_fp8=True if args.lora_8_bit else False,
        # load_in_16_bit=True if args.lora_16_bit else False,
        token=os.getenv("HF_TOKEN"),
    )
    if args.chat_template != "default":
        tokenizer.chat_template = open(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "chat_templates",
                f"{args.chat_template}_chat_template.jinja",
            )
        ).read()

    model = FastLanguageModel.get_peft_model(
        model,
        r=args.lora_r,
        target_modules=[
            "q_proj",
            "k_proj",
            "v_proj",
            "o_proj",
            "gate_proj",
            "up_proj",
            "down_proj",
        ],
        lora_alpha=args.lora_alpha,
        lora_dropout=0,
        bias="none",
        use_gradient_checkpointing="unsloth",
        random_state=3407,
        use_rslora=False,
        loftq_config=None,
    )
    dataset = get_data(args)
    dataset = dataset.map(
        lambda example: {
            "messages": [
                {"role": "user", "content": prompt.format(transcript=example["text"])},
                {"role": "assistant", "content": str(example["value"])},
            ]
        }
    )

    dataset = dataset.map(
        lambda examples: formatting_prompts_func(examples, tokenizer, args),
        batched=True,
    )

    train_dataset = dataset["train"].train_test_split(test_size=0.15)
    test_dataset = dataset["test"]

    print(f"\n📝 Single Sample: {train_dataset['train'][0]['messages']}")
    max_steps = args.epochs * int(
        np.ceil(
            len(train_dataset["train"])
            / (args.train_batch_size * args.gradient_accumulation_steps)
        )
    )
    training_args = SFTConfig(
        eval_strategy="steps",
        learning_rate=args.learning_rate,
        per_device_train_batch_size=args.train_batch_size,
        per_device_eval_batch_size=args.eval_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        weight_decay=args.weight_decay,
        warmup_steps=5,
        max_steps=max_steps,
        logging_strategy="steps",
        logging_steps=max_steps // 20,
        eval_steps=max_steps // 10,
        optim="adamw_8bit",
        lr_scheduler_type="linear",
        max_grad_norm=args.max_grad_norm,
        output_dir=OUTPUT_DIR,
        report_to="wandb" if args.wandb else None,
    )

    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=train_dataset["train"],
        eval_dataset=train_dataset["test"],
        dataset_text_field="chat",
        max_seq_length=args.max_length,
        packing=False,
        args=training_args,
    )

    if args.wandb:
        run = wandb.init(
            entity=os.getenv("WANDB_ENTITY", "gmguarino"),
            project="classification",
            config=training_args.to_dict(),
            name=f"gmguarino/{args.checkpoint.split('/')[1]}-climateguard"
            + str(uuid.uuid4()).split("-")[0],
        )
    trainer_stats = trainer.train()

    FastLanguageModel.for_inference(model)  # Enable native 2x faster inference

    test_model(
        args,
        test_dataset,
        model,
        tokenizer,
        max_new_tokens=args.max_new_tokens,
        device=device,
    )
    model.save_pretrained(f"{OUTPUT_DIR}/adapter")
    tokenizer.save_pretrained(f"{OUTPUT_DIR}/adapter")
    model.push_to_hub(
        f"gmguarino/{args.checkpoint.split('/')[1]}-climateguard-lora",
        token=os.getenv("HF_TOKEN"),
    )
    tokenizer.push_to_hub(
        f"gmguarino/{args.checkpoint.split('/')[1]}-climateguard-lora",
        token=os.getenv("HF_TOKEN"),
    )
