import evaluate
import numpy as np
import torch
from datasets import load_dataset
from huggingface_hub import notebook_login
from transformers import (
    AutoModelForSeq2SeqLM,
    AutoTokenizer,
    DataCollatorForSeq2Seq,
    Seq2SeqTrainer,
    Seq2SeqTrainingArguments,
)

notebook_login()


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


def preprocess_function(examples, tokenizer, prefix=""):
    inputs = [prefix + doc for doc in examples["text"]]
    model_inputs = tokenizer(inputs, max_length=1024, truncation=True)

    labels = tokenizer(
        text_target=examples["summary"],
        max_length=256,
        truncation=True,
        padding="max_length",
        return_tensors="pt",
    )

    model_inputs["labels"] = labels["input_ids"]
    return model_inputs


def compute_metrics(eval_pred, tokenizer):
    predictions, labels = eval_pred
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


if __name__ == "__main__":
    checkpoint = "google/flan-t5-small"
    prefix = "extraire les affirmations: "

    tokenizer = AutoTokenizer.from_pretrained(checkpoint)
    data_collator = DataCollatorForSeq2Seq(tokenizer=tokenizer, model=checkpoint)

    dataset = get_data()
    tokenized_dataset = dataset.map(
        lambda examples: preprocess_function(examples, tokenizer, prefix), batched=True
    )

    rouge = evaluate.load("rouge")

    model = AutoModelForSeq2SeqLM.from_pretrained(checkpoint)

    training_args = Seq2SeqTrainingArguments(
        output_dir="climateguard_claim_extraction",
        eval_strategy="steps",
        learning_rate=2e-5,
        per_device_train_batch_size=16,
        per_device_eval_batch_size=16,
        weight_decay=0.01,
        save_total_limit=3,
        max_steps=5 * len(tokenized_dataset["train"]),
        logging_strategy="steps",
        logging_steps=2,
        eval_steps=50,
        predict_with_generate=True,
        fp16=False,  # change to bf16=True for XPU
        max_grad_norm=0.5,  # Set clipping threshold
    )

    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset["train"],
        eval_dataset=tokenized_dataset["test"],
        processing_class=tokenizer,
        data_collator=data_collator,
        compute_metrics=lambda out: compute_metrics(out, tokenizer),
    )
    trainer.train()

    trainer.push_to_hub(
        f"gmguarino/climateguard-{checkpoint.split('/')[1]}-claim-extraction"
    )
