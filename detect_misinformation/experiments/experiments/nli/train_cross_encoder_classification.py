import argparse
import json
import logging
import os
import uuid
from datetime import datetime

import torch
from datasets import ClassLabel, load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from sentence_transformers import (
    CrossEncoder,
    CrossEncoderTrainer,
    CrossEncoderTrainingArguments,
)
from sentence_transformers.cross_encoder.evaluation import (
    CrossEncoderClassificationEvaluator,
)
from sentence_transformers.cross_encoder.losses import CrossEntropyLoss
from sklearn.metrics import classification_report
from torch.utils.data import DataLoader
from tqdm import tqdm

import wandb
import psutil


def setup_logging():
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Configure logging
    # log_filename = f"logs/training_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )

    return logging.getLogger(__name__)


logger = setup_logging()

WANDB = False
EPOCHS = 1


def get_data():
    # Load an example training dataset that works with our loss function:
    dataset = load_dataset("rabuahmad/climatecheck", split="train")
    dataset = dataset.map(lambda row: {"label": row["annotation"]})
    features = dataset.features.copy()
    features["label"] = ClassLabel(
        names=["Refutes", "Supports", "Not Enough Information"], num_classes=3
    )
    dataset = dataset.map(features=features)
    dataset = dataset.select_columns(["abstract", "claim", "label"])

    split_data = dataset.train_test_split(test_size=0.1, stratify_by_column="label")
    train_dataset, val_dataset = split_data["train"], split_data["test"]
    split_data = train_dataset.train_test_split(
        test_size=0.1, stratify_by_column="label"
    )
    train_dataset, test_dataset = split_data["train"], split_data["test"]
    return train_dataset, val_dataset, test_dataset


def collate_fn(batch):
    inputs = [(sample["abstract"], sample["claim"]) for sample in batch]
    return (
        model.tokenizer(
            inputs,
            padding=True,
            truncation=True,
            return_tensors="pt",
            max_length=args.max_length,
        ),
        torch.tensor([sample["label"] for sample in batch]),
    )


def get_scheduler(
    optimizer,
    args,
):
    if args.lr_scheduler == "decay":
        scheduler = torch.optim.lr_scheduler.ExponentialLR(
            optimizer, gamma=1 - args.lr_weight_decay
        )
    elif args.lr_scheduler == "cosine":
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=args.epochs,
            eta_min=args.lr_min_cosine,
        )
    if args.lr_warmup:
        warmup = torch.optim.lr_scheduler.LinearLR(
            optimizer,
            start_factor=0.1,
            end_factor=1,
            total_iters=args.lr_warmup_steps,
        )
        composite_scheduler = torch.optim.lr_scheduler.SequentialLR(
            optimizer,
            schedulers=[warmup, scheduler],
            milestones=[args.lr_warmup_steps],
        )
        return composite_scheduler
    return scheduler


def get_grad_norm(model):
    with torch.no_grad():
        total_norm = 0
        for p in model.parameters():
            param_norm = p.grad.data.norm(2)
            total_norm += param_norm.item() ** 2
        total_norm = total_norm ** (1.0 / 2)
    return total_norm


def train_batch(
    model,
    batch,
    label,
    loss_fn,
    optimizer,
    scheduler,
    running_loss=0.0,
    gradient_accumulation_steps=4,
    gradient_clipping=True,
    max_grad_norm=1.0,
    logging_steps=10,
    epoch_idx=None,
    run=None,
):
    batch = batch.to(device)
    label = label.to(device)
    out = model(**batch)
    loss = loss_fn(out.logits, label)
    loss.backward()
    running_loss += loss.item() / gradient_accumulation_steps
    del batch, label, out
    if (batch_idx + 1) % logging_steps == 0:
        tqdm.write(
            (
                f"Epoch: {epoch_idx}; "
                f"Train Loss: {loss}; "
                f"LR: {scheduler.get_lr()[0]}; "
                f"Grad Norm: {get_grad_norm(model)}; "
                f"Memory usage: {psutil.virtual_memory().percent}%"
            )
        )
        if run:
            run.log(
                {
                    "train/epoch": epoch_idx,
                    "train/loss": loss,
                    "train/lr": scheduler.get_lr()[0],
                    "train/grad_norm": get_grad_norm(model),
                }
            )

    if ((batch_idx + 1) % gradient_accumulation_steps == 0) or (
        batch_idx + 1 == len(train_dataloader)
    ):
        if gradient_clipping:
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_grad_norm)
        optimizer.step()
        optimizer.zero_grad()
        running_loss = 0.0
        scheduler.step()
        torch.mps.empty_cache()
    return running_loss


def test_model(model, dataloader, loss_fn, phase="eval"):
    total_loss = 0
    predictions = []
    testing_labels = []
    with torch.no_grad():
        for batch, label in tqdm(dataloader):
            testing_labels.extend(label.detach().tolist())
            batch = batch.to(device)
            label = label.to(device)
            out = model(**batch).logits
            _pred = out.argmax(dim=1).detach().cpu().tolist()
            predictions.extend(_pred)
            loss = loss_fn(out, label)
            total_loss += loss.item()
            del batch, label, out
        total_loss /= len(dataloader)
        stats = {
            f"{phase}_loss": total_loss,
            "memory_usage": f"{psutil.virtual_memory().percent}%",
        }
        stats.update(
            classification_report(testing_labels, predictions, output_dict=True)[
                "weighted avg"
            ]
        )
        tqdm.write(str(stats))
        torch.mps.empty_cache()
        return stats


def log_eval_stats(run, stats, epoch_idx):
    run.log(
        {
            "eval/epoch": epoch_idx,
            "eval/loss": stats["eval_loss"],
            "eval/precision": stats["precision"],
            "eval/recall": stats["recall"],
            "eval/f1": stats["f1-score"],
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-length", type=int, default=512)
    parser.add_argument("--train-batch-size", type=int, default=4)
    parser.add_argument("--eval-batch-size", type=int, default=4)
    parser.add_argument("--test-batch-size", type=int, default=4)
    parser.add_argument("--lr", type=float, default=1e-5)
    parser.add_argument("--lr-scheduler", choices=["decay", "cosine"], default="decay")
    parser.add_argument("--lr-weight-decay", type=float, default=0.01)
    parser.add_argument("--lr-min-cosine", type=float, default=1e-6)
    parser.add_argument("--lr-warmup", action=argparse.BooleanOptionalAction)
    parser.add_argument("--lr-warmup-steps", type=int, default=5)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--gradient-accumulation-steps", type=int, default=4)
    parser.add_argument("--validation-steps", type=int, default=10)
    parser.add_argument("--logging-steps", type=int, default=5)
    parser.add_argument("--no-grad-norm-clip", action=argparse.BooleanOptionalAction)
    parser.add_argument("--max-grad-norm", type=float, default=1.0)

    parser.add_argument(
        "--checkpoint", type=str, default="cross-encoder/nli-deberta-v3-base"
    )
    parser.add_argument("--wandb", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    logger.info(
        f"Starting training with the following arguments:\n {json.dumps(args.__dict__, indent=2)}"
    )

    # Check for accelerator backends
    device = "cuda" if torch.cuda.is_available() else "cpu"
    device = "mps" if torch.backends.mps.is_available() else device

    if device == "cpu":
        logger.info("No accelerator found, using CPU only")
    else:
        logger.info(f"Found following accelerator for training {device}")

    load_dotenv()
    login(token=os.getenv("HF_TOKEN"))
    if args.wandb:
        print("reporting to wandb")
        wandb.login(key=os.getenv("WANDB_KEY"))
        run = wandb.init(
            entity=os.getenv("WANDB_ENTITY", "gmguarino"),
            project="climatecheck",
            config=args.__dict__,
            name=f"gmguarino/{args.checkpoint.split('/')[1]}-climatecheck-"
            + str(uuid.uuid4()).split("-")[0],
        )
    else:
        run = None

    train_dataset, val_dataset, test_dataset = get_data()

    model_name = "cross-encoder/nli-deberta-v3-base"
    model = CrossEncoder(
        args.checkpoint,
        num_labels=3,
        activation_fn=torch.nn.functional.gelu,
        device=device,
        backend="torch",
    )  # num_labels=1 is for rerankers

    train_dataloader = DataLoader(
        train_dataset,
        batch_size=args.train_batch_size,
        collate_fn=collate_fn,
        shuffle=True,
    )
    val_dataloader = DataLoader(
        val_dataset,
        batch_size=args.eval_batch_size,
        collate_fn=collate_fn,
        shuffle=True,
    )
    test_dataloader = DataLoader(
        test_dataset,
        batch_size=args.test_batch_size,
        collate_fn=collate_fn,
        shuffle=True,
    )

    loss_fn = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr)
    scheduler = get_scheduler(
        optimizer=optimizer,
        args=args,
    )

    running_loss = 0.0
    pbar = tqdm(total=args.epochs * len(train_dataloader))
    for epoch in range(args.epochs):
        for batch_idx, (batch, label) in enumerate(train_dataloader):
            epoch_idx = epoch + batch_idx / len(train_dataloader)
            running_loss = train_batch(
                model,
                batch,
                label,
                loss_fn,
                optimizer,
                scheduler,
                running_loss=running_loss,
                gradient_accumulation_steps=args.gradient_accumulation_steps,
                gradient_clipping=not args.no_grad_norm_clip,
                max_grad_norm=args.max_grad_norm,
                logging_steps=args.logging_steps,
                epoch_idx=epoch_idx,
                run=run,
            )
            if (batch_idx + 1) % args.validation_steps == 0:
                val_stats = test_model(model, val_dataloader, loss_fn, phase="eval")
                if run:
                    log_eval_stats(run, val_stats, epoch_idx)
            pbar.update()

    test_stats = test_model(model, test_dataloader, loss_fn, phase="test")
    logging.info(
        f"Post training metrics on test set:\n{json.dumps(test_stats, indent=2)}"
    )

    run.finish()
