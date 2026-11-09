import argparse
import json
import logging
import os
import random
from datetime import datetime
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import wandb
from datasets import load_dataset
from datasets.arrow_dataset import Dataset

# from default_training_args import DEFAULT_TRAINING_ARGS
from dotenv import load_dotenv
from huggingface_hub import HfFolder
from llama_index.core.node_parser import SentenceSplitter
from setfit import SetFitModel, Trainer, TrainingArguments, sample_dataset
from sklearn.metrics import (
    f1_score,
    precision_score,
    recall_score,
    classification_report,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def compute_metrics(eval_pred):
    predictions, labels = eval_pred
    predictions = predictions.argmax(1)
    f1 = f1_score(labels, predictions, pos_label=1)
    recall = recall_score(labels, predictions, pos_label=1)
    precision = precision_score(labels, predictions, pos_label=1)
    return {
        "f1": float(f1) if f1 == 1 else f1,
        "recall": float(recall) if recall == 1 else recall,
        "precision": float(precision) if precision == 1 else precision,
    }


def sample_dataset_claims(
    args: argparse.Namespace, dataset: Union[List[Dict[str, Any]], Dataset]
):
    splitter = SentenceSplitter(
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
    )
    records = []
    n_true = 0
    n_false = 0
    n_max = 200
    for record in dataset:
        if (
            record["misinformation"]
            and record["misinformation_claims"]
            and n_true < n_max
        ):
            for claim in record["misinformation_claims"]:
                for label in claim["labels"]:
                    claim_text = claim["text"]
                    plaintext = (
                        record["plaintext_whisper"]
                        .lower()
                        .replace(".", "")
                        .replace(",", "")
                        .replace("?", "")
                    )
                    start_claim = record["plaintext_whisper"].find(claim_text)
                    # start_idx = max(start_claim - 100, 0)
                    start_idx = start_claim
                    end_idx = start_idx + 512
                    text = " ".join(plaintext[start_idx:end_idx].split(" ")[:-1])
                    # chunk = splitter.split_text(text)[0]
                    records.append({"text": text, "label": 1})
                    n_true += 1
        elif not record["misinformation"]:
            if args.chunk and n_false < n_max:
                chunks = splitter.split_text(
                    record["plaintext_whisper"]
                    .lower()
                    .replace(".", "")
                    .replace(",", "")
                )
                records.append(
                    {
                        "text": random.choice(chunks),
                        "label": int(record["misinformation"]),
                    }
                )
                n_false += 1
            elif n_false < n_max:
                chunks = splitter.split_text(
                    record["plaintext_whisper"]
                    .lower()
                    .replace(".", "")
                    .replace(",", "")
                )
                records.append(
                    {
                        "text": (
                            record["plaintext_whisper"]
                            .lower()
                            .replace(".", "")
                            .replace(",", "")
                        ),
                        "label": int(record["misinformation"]),
                    }
                )
                n_false += 1
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=str,
        default="DataForGood/climateguard",
        help="The HF id of the dataset used for training.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="BAAI/bge-base-en-v1.5",
        help="The HF id of the base model to finetune. Should be a sentence transformer.",
    )
    parser.add_argument(
        "--split",
        type=str,
        default="train",
        help="Data split to be trained on (default: 'train')",
    )
    parser.add_argument(
        "--train-val-split",
        type=float,
        default=0.1,
        help="Percentage of train set dedicated to validation",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=512,
        help="Max segments length to train the model on (default: 512).",
    )
    parser.add_argument(
        "--chunk",
        action="store_true",
        help="Split texts longer than max tokens into chunks, if flag is present",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=512,
        help="Chunk size if chunk=True (default: 512).",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=0,
        help="Overlap between chunks if chunk=True (default: 128).",
    )
    parser.add_argument(
        "--training-type",
        type=str,
        choices=["full", "out", "linear"],
        default="out",
        help=(
            "Type of finetuning. `full` corresponds to a full finetuning, "
            "`out` corresponds to training the classifier out layer only and "
            "`linear` trains both the classification linear layer as well as the linear pooler."
        ),
    )
    parser.add_argument(
        "--logging-type",
        type=str,
        choices=["None", "wandb"],
        default="wandb",
        help="Where to log to. Either `None` or `wandb` (for weights and biases)",
    )
    parser.add_argument(
        "--max-samples-per-class",
        type=int,
        default=100,
        help="Number of training samples to use for each class (default: 200).",
    )
    parser.add_argument(
        "--push-to-hub",
        action="store_true",
        help="Push training checkpoints to hub if active",
    )
    parser.add_argument(
        "--training-args",
        type=str,
        help=(
            "JSON encoded string containing all the training arguments for the HF Trainer. "
            "Defaults defined in the default_training_args.py file. Can also be partial "
            "as the arguments will be updated key by key. "
            "See https://huggingface.co/docs/transformers/v4.52.3/main_classes/trainer "
            "for more info"
        ),
    )

    args = parser.parse_args()
    arg_names = "\n".join([f"{key}={value}" for key, value in vars(args).items()])
    logging.info(f"Starting training with the following arguments: \n{arg_names}")

    load_dotenv()
    if args.logging_type:
        wandb.login()
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    # Load raw dataset
    train_dataset = load_dataset(args.dataset, split=args.split)
    logging.info(
        "Found the following distribution of labels in dataset: \n"
        f"{train_dataset.to_pandas().misinformation.value_counts()}"
    )
    records = sample_dataset_claims(args, train_dataset.shuffle())
    claims_dataset = Dataset.from_pandas(pd.DataFrame.from_records(records))
    claims_dataset = claims_dataset.class_encode_column("label")
    split_dataset = claims_dataset.train_test_split(
        test_size=args.train_val_split, stratify_by_column="label"
    )
    split_dataset["train"] = sample_dataset(
        split_dataset["train"],
        label_column="label",
        num_samples=args.max_samples_per_class,
    )
    logging.info(
        "Sampled the following distribution of examples from dataset: \n"
        f"{split_dataset["train"].to_pandas().label.value_counts()}"
    )

    # Download the model from huggingface.co/models
    model = SetFitModel.from_pretrained(
        args.model,
        use_differentiable_head=True,
        head_params={"out_features": 2},
        labels=[0, 1],
    )

    # # set all layers to not trainable
    # if args.training_type != "full":
    #     for parameter in model.parameters():
    #         parameter.requires_grad = False

    # if args.training_type == "out":
    #     # Set linear classifier to trainable
    #     for parameter in model.classifier.parameters():
    #         parameter.requires_grad = True

    # if args.training_type == "linear":
    #     for parameter in model.pooler.dense.parameters():
    #         parameter.requires_grad = True

    # requires_grad_params = 0
    # total_params = 0
    # for parameter in model.parameters():
    #     total_params += parameter.nelement()
    #     if parameter.requires_grad:
    #         requires_grad_params += parameter.nelement()
    # logging.info(
    #     f"Training {round(requires_grad_params/1e6)}M parameters out of {round(total_params/1e6)}M"
    # )

    # training_args = DEFAULT_TRAINING_ARGS
    # if args.push_to_hub:
    #     hub_params = dict(
    #         push_to_hub=args.push_to_hub,
    #         hub_strategy="every_save",
    #         hub_token=HfFolder.get_token(),
    #     )
    #     training_args.update(hub_params)
    # if args.training_args:
    #     try:
    #         training_args.update(json.loads(args.training_args))
    #     except json.JSONDecodeError as e:
    #         logging.fatal(f"Could not load training arguments from string {args.training_args}")
    #         raise e
    # else:
    #     logging.warning(
    #         "No training arguments have been passed. Defaults will be applied. "
    #         "See `default_training_args.py` for more info. To pass customised training "
    #         "arguments, use the `--training-args` flag."
    #     )

    # Metric helper method
    # Define training args
    training_args = TrainingArguments(
        output_dir="runs/"
        + args.model.split("/")[-1]
        + "-"
        + args.dataset.split("/")[-1],
        run_name=args.model.split("/")[-1]
        + "-"
        + args.dataset.split("/")[-1]
        + datetime.now().strftime("%d-%m-%Y_%H-%M-%S"),
        batch_size=4,
        num_epochs=1,
        logging_strategy="steps",
        logging_steps=50,
        eval_strategy="steps",
        eval_steps=50,
        save_strategy="steps",
        save_steps=100,
        load_best_model_at_end=True,
        report_to=args.logging_type,
    )
    # Create a Trainer instance
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=split_dataset["train"],
        eval_dataset=split_dataset["test"],
        metric="f1",
    )
    trainer.train()
    print(trainer.evaluate(split_dataset["test"]))
    preds = model(split_dataset["test"]["text"])

    print(classification_report(split_dataset["test"]["label"], preds))
    model.save_pretrained(
        "models/" + args.model.split("/")[-1] + "-" + args.dataset.split("/")[-1]
    )

    model.push_to_hub(args.model.split("/")[-1] + "-" + args.dataset.split("/")[-1])
