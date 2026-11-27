import argparse
import logging
import os
import re
import uuid
from datetime import datetime
from functools import partial
from typing import Optional, Union

import evaluate
import numpy as np
import torch
from datasets import Dataset, DatasetDict, load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from peft import LoraConfig, PeftModel, TaskType, get_peft_model
from sklearn.metrics import classification_report
from tqdm import tqdm
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    DataCollatorForLanguageModeling,
    Trainer,
    TrainingArguments,
)
from trl import SFTConfig, SFTTrainer

import wandb


def setup_logging():
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Configure logging
    # log_filename = f"logs/training_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            # logging.FileHandler(log_filename),
            logging.StreamHandler(),  # Also log to console
        ],
    )

    return logging.getLogger(__name__)


logger = setup_logging()


load_dotenv()
login(token=os.getenv("HF_TOKEN"))


def get_data():
    dataset = load_dataset("DataForGood/climateguard")

    dataset = dataset.filter(lambda example: example["comments"] == [])
    dataset = dataset.filter(
        lambda example: isinstance(example["misinformation"], bool)
    )
    dataset = dataset.filter(
        lambda example: not isinstance(example["misinformation"], str)
    )
    dataset = dataset.map(
        lambda example: {
            "text": example["plaintext"],
            "value": 10 * int(example["misinformation"]),
        }
    )
    dataset = dataset.select_columns(["id", "text", "value"])
    return dataset


def create_lora_model(base_model):
    # Load base model

    GLU_MODULES = ["w1", "w2", "w3"]
    MHA_MODULES = ["q_proj", "k_proj", "v_proj"]
    CONV_MODULES = ["in_proj", "out_proj"]
    target_modules = MHA_MODULES + CONV_MODULES + GLU_MODULES
    # Create LoRA configuration
    lora_config = LoraConfig(
        r=8,
        lora_alpha=32,
        target_modules=target_modules,
        lora_dropout=0.05,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
    )

    # Apply LoRA to model
    model = get_peft_model(base_model, lora_config)
    model.print_trainable_parameters()

    return model


def preprocess_logits_for_metrics(logits, labels):
    if isinstance(logits, tuple):
        logits = logits[0]
    return logits.argmax(dim=-1)


def compute_metrics(eval_pred, tokenizer, rouge):
    predictions, labels = eval_pred
    if isinstance(predictions, tuple):
        predictions = predictions[0]

    decoded_preds = tokenizer.batch_decode(predictions, skip_special_tokens=True)
    labels = np.where(labels != -100, labels, tokenizer.pad_token_id)
    decoded_labels = tokenizer.batch_decode(labels, skip_special_tokens=True)

    result = rouge.compute(
        predictions=decoded_preds, references=decoded_labels, use_stemmer=True
    )

    prediction_lens = [
        np.count_nonzero(pred != tokenizer.pad_token_id) for pred in predictions
    ]
    result["gen_len"] = np.mean(prediction_lens)

    return {k: round(v, 4) for k, v in result.items()}


def parse_response(response: Optional[Union[int, str]]):
    """Parse response containing only a score."""
    if isinstance(response, int):
        return response
    match = re.match(r"^[^\d]*(\d+)", response)
    if match:
        score = int(match.group(1))  # Extract score as an integer
    else:
        score = 0
    return score


def test_model(test_dataset, model, tokenizer, max_new_tokens, device="cpu"):
    logger.info("Evaluating model on test set...")
    model.eval()
    results = []
    for example in tqdm(test_dataset):
        input_conv = [example["messages"][0]]
        inputs = tokenizer.apply_chat_template(
            input_conv,
            return_tensors="pt",
            truncation=True,
            max_length=args.max_length - max_new_tokens,
            add_generation_prompt=True,
        ).to(device)
        with torch.no_grad():
            output_tokens = model.generate(inputs, max_new_tokens=max_new_tokens)
        prediction = tokenizer.decode(
            output_tokens[0][inputs.size(1) :], skip_special_tokens=True
        )
        results.append(parse_response(prediction) >= 8)

    # preds, refs = zip(*results)
    # with open("predictions.txt", "w") as f:
    #     for pred in preds:
    #         f.write(pred)
    #         f.write("\n")
    # rouge_scores = rouge.compute(predictions=preds, references=refs)
    report = classification_report(
        test_dataset.to_pandas()["value"].astype(int),
        results,
    )
    logger.info(f"Classification on test set: \n{report}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-length", type=int, default=4096)
    parser.add_argument("--max-new-tokens", type=int, default=512)
    parser.add_argument("--learning-rate", type=float, default=5e-5)
    parser.add_argument("--train-batch-size", type=int, default=8)
    parser.add_argument("--eval-batch-size", type=int, default=8)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--checkpoint", type=str, default="kurakurai/Luth-LFM2-350M")
    parser.add_argument("--wandb", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    if args.wandb:
        logger.info("reporting to wandb")
        wandb.login(key=os.getenv("WANDB_KEY"))

    rouge = evaluate.load("rouge")
    OUTPUT_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "climateguard_train"
    )
    prompt = """You are an assistant helping editors to moderate TV and radio content.
You will be provided with a transcript delimited by triple backticks.
Bear in mind that the transcript may be missing punctuation and may be of very low quality, with incorrect vocabulary, cuts in the wrong places, or may include some phonetic transcription.

Does the text promote climate change misinformation that undermines well-established scientific consensus, such as denying the existence of climate change or the factors that contribute to it ?

Answer just a number on a scale of 0 - does not promote - to 10 - certainly promotes.

text: {transcript}"""
    device = "cuda" if torch.cuda.is_available() else None
    if not device:
        device = "mps" if torch.backends.mps.is_available() else "cpu"

    logging.info(f"Using device: {device}")
    logging.info(f"bfloat available:: {torch.cuda.is_bf16_supported()}")

    tokenizer = AutoTokenizer.from_pretrained(args.checkpoint)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.chat_template = open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat_template.jinja")
    ).read()

    dataset = get_data()
    logger.info(dataset)
    logger.info(dataset["test"].to_pandas().value.unique())
    dataset = dataset.map(
        lambda example: {
            "messages": [
                {"role": "user", "content": prompt.format(transcript=example["text"])},
                {"role": "assistant", "content": str(example["value"])},
            ]
        }
    )
    train_dataset = dataset["train"].train_test_split(test_size=0.15)
    test_dataset = dataset["test"]

    logger.info(f"\n📝 Single Sample: {train_dataset['train'][0]['messages']}")

    base_model = AutoModelForCausalLM.from_pretrained(
        args.checkpoint, torch_dtype=torch.float16, device_map="auto"
    )
    logger.info("Evaluating base model...")
    test_model(
        test_dataset,
        base_model,
        tokenizer,
        max_new_tokens=args.max_new_tokens,
        device=device,
    )
    model = create_lora_model(base_model=base_model)

    training_args = SFTConfig(
        output_dir=OUTPUT_DIR,
        eval_strategy="steps",
        learning_rate=args.learning_rate,
        per_device_train_batch_size=args.train_batch_size,
        per_device_eval_batch_size=args.eval_batch_size,
        weight_decay=args.weight_decay,
        save_total_limit=3,
        max_steps=args.epochs
        * int(np.ceil(len(train_dataset["train"]) / args.train_batch_size)),
        logging_strategy="steps",
        logging_steps=10,
        eval_steps=len(train_dataset["train"]) // 10,
        optim="stable_adamw",
        lr_scheduler_type="linear",
        max_grad_norm=0.5,  # Set clipping threshold
        fp16=torch.cuda.is_available()
        and not torch.cuda.is_bf16_supported,  # Enable mixed precision training
        bf16=torch.cuda.is_available()
        and torch.cuda.is_bf16_supported(),  # Enable mixed precision training
        report_to="wandb" if args.wandb else None,  # Disable wandb if not needed
        remove_unused_columns=False,
        dataloader_num_workers=0,
        warmup_steps=10,
        gradient_checkpointing=True,  # Save memory
        gradient_checkpointing_kwargs={"use_reentrant": False},
        assistant_only_loss=True,
    )
    compute_metrics_fn = partial(compute_metrics, tokenizer=tokenizer, rouge=rouge)

    if args.wandb:
        run = wandb.init(
            entity=os.getenv("WANDB_ENTITY", "gmguarino"),
            project="classification",
            config=training_args.to_dict(),
            name=f"gmguarino/climateguard-{args.checkpoint.split('/')[1]}-classification-sft"
            + str(uuid.uuid4()).split("-")[0],
        )

    trainer = SFTTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset["train"],
        eval_dataset=train_dataset["test"],
        processing_class=tokenizer,
    )
    trainer.train()

    # --- SAVE LoRA ADAPTER ---
    model.save_pretrained(OUTPUT_DIR)
    tokenizer.save_pretrained(OUTPUT_DIR)

    # --- MERGE LoRA + BASE MODEL ---
    logger.info("🔄 Merging LoRA adapter with base model...")
    model_merged = model.merge_and_unload()

    merged_dir = os.path.join(OUTPUT_DIR, "merged")
    os.makedirs(merged_dir, exist_ok=True)
    model_merged.save_pretrained(merged_dir)
    tokenizer.save_pretrained(merged_dir)
    logger.info(f"✅ Merged model saved to {merged_dir}")

    logger.info("🚀 Pushing merged model to Hugging Face Hub...")
    model_merged.push_to_hub(
        f"gmguarino/climateguard-{args.checkpoint.split('/')[1]}-classification-sft"
    )
    tokenizer.push_to_hub(
        f"gmguarino/climateguard-{args.checkpoint.split('/')[1]}-classification-sft"
    )

    test_model(
        test_dataset,
        model_merged,
        tokenizer,
        max_new_tokens=args.max_new_tokens,
        device=device,
    )
