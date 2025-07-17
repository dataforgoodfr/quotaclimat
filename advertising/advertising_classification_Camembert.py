#!/usr/bin/env python3
"""
CamemBERT Training Script for Advertising Detection
"""

# TODO: this script is at its very begining, it's not working yet. Note that this process comes from Climate Safeguards project.

import pandas as pd
import numpy as np
from datasets import Dataset
from transformers import (
    AutoTokenizer, 
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    EarlyStoppingCallback
)
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, classification_report
from sklearn.model_selection import train_test_split
import torch
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_and_prepare_data():
    """Load and prepare the dataset"""
    logger.info("Loading dataset...")
    
    # Load your dataset
    file_path = 'data/intermediate/labeled_ads_dataset.csv'
    df = pd.read_csv(file_path)
    
    df = df.sample(frac=0.25)
    
    # Convert boolean labels to integers
    df.ads_ground_truth = df.ads_ground_truth.replace(True, 1)
    df.ads_ground_truth = df.ads_ground_truth.replace(False, 0)
    
    # Split the data as specified
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
    train_df, eval_df = train_test_split(train_df, test_size=0.25, random_state=42)
    
    # Extract texts and labels
    X_train = train_df['startplaintext'].tolist()
    y_train = train_df['ads_ground_truth'].tolist()
    
    X_val = eval_df['startplaintext'].tolist()
    y_val = eval_df['ads_ground_truth'].tolist()
    
    X_test = test_df['startplaintext'].tolist()
    y_test = test_df['ads_ground_truth'].tolist()
    
    # Handle NaN values
    X_train = [str(text) if pd.notna(text) else "" for text in X_train]
    X_val = [str(text) if pd.notna(text) else "" for text in X_val]
    X_test = [str(text) if pd.notna(text) else "" for text in X_test]
    
    # Log dataset information
    logger.info(f"Original dataset shape: {df.shape}")
    logger.info(f"Training samples: {len(X_train)}")
    logger.info(f"Validation samples: {len(X_val)}")
    logger.info(f"Test samples: {len(X_test)}")
    logger.info(f"Label distribution in training set: {np.bincount(y_train)}")
    logger.info(f"Label distribution in validation set: {np.bincount(y_val)}")
    logger.info(f"Label distribution in test set: {np.bincount(y_test)}")
    
    return X_train, X_val, X_test, y_train, y_val, y_test

def initialize_model_and_tokenizer():
    """Initialize CamemBERT model and tokenizer"""
    logger.info("Initializing CamemBERT model and tokenizer...")
    
    model_name = "almanach/camembert-base"  # CamemBERT v2
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(
        model_name, 
        num_labels=2,
        id2label={0: "Not Advertisement", 1: "Advertisement"},
        label2id={"Not Advertisement": 0, "Advertisement": 1}
    )
    
    return model, tokenizer

def prepare_datasets(X_train, X_val, X_test, y_train, y_val, y_test, tokenizer):
    """Prepare and tokenize datasets"""
    logger.info("Preparing and tokenizing datasets...")
    
    def tokenize_function(examples):
        return tokenizer(
            examples["text"], 
            truncation=True, 
            padding="max_length", 
            max_length=512
        )
    
    # Create datasets
    train_dataset = Dataset.from_dict({"text": X_train, "labels": y_train})
    val_dataset = Dataset.from_dict({"text": X_val, "labels": y_val})
    test_dataset = Dataset.from_dict({"text": X_test, "labels": y_test})
    
    # Tokenize datasets
    train_dataset = train_dataset.map(tokenize_function, batched=True)
    val_dataset = val_dataset.map(tokenize_function, batched=True)
    test_dataset = test_dataset.map(tokenize_function, batched=True)
    
    return train_dataset, val_dataset, test_dataset

def compute_metrics(eval_pred):
    """Compute metrics for evaluation"""
    predictions, labels = eval_pred
    predictions = np.argmax(predictions, axis=1)
    
    precision, recall, f1, _ = precision_recall_fscore_support(labels, predictions, average='weighted')
    accuracy = accuracy_score(labels, predictions)
    
    return {
        'accuracy': accuracy,
        'f1': f1,
        'precision': precision,
        'recall': recall
    }

def setup_training_arguments():
    """Setup training arguments"""
    return TrainingArguments(
        output_dir="./camembert-ad-classifier",
        num_train_epochs=3,
        per_device_train_batch_size=16,
        per_device_eval_batch_size=16,
        warmup_steps=500,
        weight_decay=0.01,
        logging_dir="./logs",
        logging_steps=100,
        eval_strategy="steps",
        eval_steps=500,
        save_strategy="steps",
        save_steps=500,
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        greater_is_better=True,
        report_to="none",
        learning_rate=2e-5,
        lr_scheduler_type="linear",
        save_total_limit=2,
        dataloader_pin_memory=False,
        disable_tqdm=False,
    )

def train_model(model, tokenizer, train_dataset, val_dataset, training_args):
    """Train the CamemBERT model"""
    logger.info("Starting CamemBERT fine-tuning...")
    logger.info(f"Training on {len(train_dataset)} samples")
    logger.info(f"Validating on {len(val_dataset)} samples")
    
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        tokenizer=tokenizer,
        compute_metrics=compute_metrics,
        callbacks=[EarlyStoppingCallback(early_stopping_patience=3)],
    )
    
    trainer.train()
    
    logger.info("Training completed!")
    
    return trainer

def evaluate_model(trainer, test_dataset):
    """Evaluate the trained model"""
    logger.info("Evaluating on test set...")
    
    test_results = trainer.evaluate(test_dataset)
    logger.info(f"Test Results: {test_results}")
    
    # Detailed evaluation
    predictions = trainer.predict(test_dataset)
    y_pred = np.argmax(predictions.predictions, axis=1)
    y_true = test_dataset["labels"]
    
    # Classification report
    report = classification_report(
        y_true, y_pred, 
        target_names=["Not Advertisement", "Advertisement"],
        digits=4
    )
    
    print("\nDetailed Classification Report:")
    print(report)
    
    return test_results, y_pred, y_true

def save_results(test_results, training_samples, test_samples):
    """Save results for comparison"""
    results = {
        'model_name': 'CamemBERT',
        'test_accuracy': test_results['eval_accuracy'],
        'test_f1': test_results['eval_f1'],
        'test_precision': test_results['eval_precision'],
        'test_recall': test_results['eval_recall'],
        'training_samples': training_samples,
        'test_samples': test_samples
    }
    
    print("=" * 60)
    print("CAMEMBERT TRAINING COMPLETED")
    print("=" * 60)
    print(f"Test Accuracy: {results['test_accuracy']:.4f}")
    print(f"Test F1-Score: {results['test_f1']:.4f}")
    print(f"Test Precision: {results['test_precision']:.4f}")
    print(f"Test Recall: {results['test_recall']:.4f}")
    print(f"Training Samples: {results['training_samples']}")
    print(f"Test Samples: {results['test_samples']}")
    print("=" * 60)
    
    # Save results to file
    with open('camembert_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Results saved to 'camembert_results.json'")
    
    return results

def show_sample_predictions(model, tokenizer, X_test, y_test, num_samples=5):
    """Show sample predictions"""
    sample_texts = X_test[:num_samples]
    sample_labels = y_test[:num_samples]
    
    print("\nSample Predictions from Test Set:")
    
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    model.eval()
    
    for i, (text, actual_label) in enumerate(zip(sample_texts, sample_labels)):
        inputs = tokenizer(
            text, 
            return_tensors="pt", 
            truncation=True, 
            padding=True, 
            max_length=512
        ).to(device)
        
        with torch.no_grad():
            outputs = model(**inputs)
            prediction = torch.nn.functional.softmax(outputs.logits, dim=-1)
            predicted_class = torch.argmax(prediction, dim=-1).item()
            confidence = prediction[0][predicted_class].item()
        
        pred_label = "Advertisement" if predicted_class == 1 else "Not Advertisement"
        actual_label_str = "Advertisement" if actual_label == 1 else "Not Advertisement"
        text_display = text[:100] + "..." if len(text) > 100 else text
        
        print(f"Text: {text_display}")
        print(f"Prediction: {pred_label} (Confidence: {confidence:.4f})")
        print(f"Actual: {actual_label_str}")
        print("-" * 50)

def main():
    """Main training function"""
    # Load and prepare data
    X_train, X_val, X_test, y_train, y_val, y_test = load_and_prepare_data()
    
    # Initialize model and tokenizer
    model, tokenizer = initialize_model_and_tokenizer()
    
    # Prepare datasets
    train_dataset, val_dataset, test_dataset = prepare_datasets(
        X_train, X_val, X_test, y_train, y_val, y_test, tokenizer
    )
    
    # Setup training arguments
    training_args = setup_training_arguments()
    
    # Train model
    trainer = train_model(model, tokenizer, train_dataset, val_dataset, training_args)
    
    # Evaluate model
    test_results, y_pred, y_true = evaluate_model(trainer, test_dataset)
    
    # Save results
    save_results(test_results, len(train_dataset), len(test_dataset))
    
    # Show sample predictions
    show_sample_predictions(model, tokenizer, X_test, y_test)
    
    # Save model
    model.save_pretrained("./camembert-ad-classifier")
    tokenizer.save_pretrained("./camembert-ad-classifier")
    logger.info("Model and tokenizer saved to './camembert-ad-classifier'")

if __name__ == "__main__":
    main()