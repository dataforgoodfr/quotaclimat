import argparse
import logging
import os
import uuid
from datetime import datetime
from functools import partial

import evaluate
import numpy as np
import torch
import wandb
from datasets import Dataset, DatasetDict, load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from peft import LoraConfig, PeftModel, TaskType, get_peft_model
from tqdm import tqdm
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    DataCollatorForLanguageModeling,
    Trainer,
    TrainingArguments,
)
from trl import DPOConfig, DPOTrainer


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

    dataset = dataset.filter(lambda example: example["claims"] != [])
    dataset = dataset.map(
        lambda example: {
            "text": example["plaintext"],
            "summary": ". ".join(example["claims"]),
        }
    )
    dataset = dataset.select_columns(["id", "text", "summary"])
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


def predict_conversation(input_conv, model, tokenizer, max_new_tokens, device):
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
    return prediction


def generate_negative_example(example, model, tokenizer, max_new_tokens, device):
    input_conv = [
        {"role": "user", "content": prompt.format(transcript=example["text"])}
    ]
    prediction = predict_conversation(
        input_conv, model, tokenizer, max_new_tokens, device
    )
    return prediction


def test_model(test_dataset, model, tokenizer, max_new_tokens, device="cpu"):
    logger.info("Evaluating model on test set...")
    model.eval()
    results = []
    for example in tqdm(test_dataset):
        input_conv = [example["chosen"][0]]
        prediction = predict_conversation(
            input_conv, model, tokenizer, max_new_tokens, device
        )
        results.append((prediction, example["summary"]))

    preds, refs = zip(*results)
    with open("predictions.txt", "w") as f:
        for pred in preds:
            f.write(pred)
            f.write("\n")
    rouge_scores = rouge.compute(predictions=preds, references=refs)
    logger.info(f"ROUGE scores on test set: {rouge_scores}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-length", type=int, default=4096)
    parser.add_argument("--max-new-tokens", type=int, default=512)
    parser.add_argument("--learning-rate", type=float, default=1e-6)
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
        os.path.dirname(os.path.abspath(__file__)), "climateguard_claim_extraction_dpo"
    )
    prompt = """
À partir d'une transcription d'une émission médiatique, 
identifiez les affirmations qui ont été présentées comme des faits. 
Gardez à l'esprit que le texte peut être désordonné et manquer de ponctuation. 

Voici la transcription :
{transcript}
"""
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

    base_model = AutoModelForCausalLM.from_pretrained(
        args.checkpoint, torch_dtype=torch.float16, device_map="auto"
    )

    os.makedirs(
        os.path.join(
            OUTPUT_DIR,
            "cache",
        ),
        exist_ok=True,
    )
    if os.path.exists(
        os.path.join(OUTPUT_DIR, "cache", "dpo_dataset_train.json")
    ) and os.path.exists(os.path.join(OUTPUT_DIR, "cache", "dpo_dataset_test.json")):
        dataset = DatasetDict(
            {
                "train": Dataset.from_json(
                    os.path.join(OUTPUT_DIR, "cache", "dpo_dataset_train.json")
                ),
                "test": Dataset.from_json(
                    os.path.join(OUTPUT_DIR, "cache", "dpo_dataset_test.json")
                ),
            }
        )
    else:
        dataset = get_data()
        dataset = dataset.map(
            lambda example: {
                "prompt": prompt.format(transcript=example["text"]),
                "chosen": [
                    {
                        "role": "user",
                        "content": prompt.format(transcript=example["text"]),
                    },
                    {"role": "assistant", "content": example["summary"]},
                ],
                "rejected": [
                    {
                        "role": "user",
                        "content": prompt.format(transcript=example["text"]),
                    },
                    {
                        "role": "assistant",
                        "content": generate_negative_example(
                            example,
                            base_model,
                            tokenizer,
                            max_new_tokens=args.max_new_tokens,
                            device=device,
                        ),
                    },
                ],
            }
        )
        dataset["train"].to_json(
            os.path.join(OUTPUT_DIR, "cache", "dpo_dataset_train.json")
        )
        dataset["test"].to_json(
            os.path.join(OUTPUT_DIR, "cache", "dpo_dataset_test.json")
        )
    train_dataset = dataset["train"].train_test_split(
        test_size=0.15, shuffle=True, seed=976
    )
    test_dataset = dataset["test"]

    logger.info(f"\n📝 Single Sample:")
    logger.info(f"prompt: {train_dataset['train'][0]['prompt']}")
    logger.info(f"chosen: {train_dataset['train'][0]['chosen'][-1]}")
    logger.info(f"rejected: {train_dataset['train'][0]['rejected'][-1]}")

    logger.info("Evaluating base model...")
    test_model(
        test_dataset,
        base_model,
        tokenizer,
        max_new_tokens=args.max_new_tokens,
        device=device,
    )
    model = create_lora_model(base_model=base_model)

    training_args = DPOConfig(
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
    )
    compute_metrics_fn = partial(compute_metrics, tokenizer=tokenizer, rouge=rouge)

    if args.wandb:
        run = wandb.init(
            entity=os.getenv("WANDB_ENTITY", "gmguarino"),
            project="claim_extraction",
            config=training_args.to_dict(),
            name=f"gmguarino/climateguard-{args.checkpoint.split('/')[1]}-claim-extraction-dpo"
            + str(uuid.uuid4()).split("-")[0],
        )

    trainer = DPOTrainer(
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
    model.push_to_hub(
        f"gmguarino/climateguard-{args.checkpoint.split('/')[1]}-claim-extraction-dpo"
    )
    tokenizer.push_to_hub(
        f"gmguarino/climateguard-{args.checkpoint.split('/')[1]}-claim-extraction-dpo"
    )

    test_model(
        test_dataset,
        model,
        tokenizer,
        max_new_tokens=args.max_new_tokens,
        device=device,
    )
