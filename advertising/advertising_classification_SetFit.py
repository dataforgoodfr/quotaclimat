# %%

import pandas as pd
from datasets import Dataset
from setfit import SetFitModel, SetFitTrainer
from sentence_transformers.losses import CosineSimilarityLoss
from sklearn.metrics import accuracy_score, classification_report
import torch
import transformers
from sklearn.model_selection import train_test_split
import os
import logging

os.environ['TRANSFORMERS_VERBOSITY'] = 'info'

file_path = 'data/intermediate/labeled_ads_dataset.csv' # Labeled ads comes from create_datasets.py

df = pd.read_csv(file_path)

df.ads_ground_truth = df.ads_ground_truth.replace(True, 1)
df.ads_ground_truth = df.ads_ground_truth.replace(False, 0)

train_df, test_df = train_test_split(df, test_size=0.2)

train_size = 250

test_df = test_df.sample(5000)
train_df = pd.concat([train_df.iloc[:train_size//2], train_df.iloc[-(train_size - train_size//2):]])

def prepare_dataset(texts, labels):
    """Convert your data to HuggingFace Dataset format"""
    data = {"text": texts, "label": labels}
    return Dataset.from_dict(data)

train_dataset = prepare_dataset(train_df.plaintext, train_df.ads_ground_truth)

target_labels = ['news', 'advertising']

 # %%

mode = 'online'

# model_id = 'sentence-transformers/all-MiniLM-L6-v2'
# model_id = "sentence-transformers/paraphrase-mpnet-base-v2"
# model_id = 'megablaster2600/ads-or-not-model'
# model_id = 'Kaleemullah/paraphrase-mpnet-base-v2-ads-nonads-classifier'
model_id = 'IslemTouati/french_model'
# model_id = 'IslemTouati/setfit_french'

if mode == 'local_raw':
    model = SetFitModel._from_pretrained('models')

elif mode == 'best':
    model = SetFitModel._from_pretrained('best_model')

else:
    # Load a SetFit model from Hub
    model = SetFitModel.from_pretrained(
        model_id,
        labels=target_labels,
    )

# %%

# Training phase

training = True

if training:

    trainer = SetFitTrainer(
        model=model,
        train_dataset=train_dataset,
        loss_class=CosineSimilarityLoss,
        metric="accuracy",
        batch_size=16,
        num_iterations=20,  # Number of text pairs to generate for contrastive learning
        num_epochs=1,       # Epochs for classification head training
        column_mapping={"text": "text", "label": "label"}
    )

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    transformers.logging.set_verbosity_info()

    logger.info("Starting SetFit training...")
    logger.info("Phase 1: Contrastive learning (sentence embeddings)")
    logger.info("Phase 2: Classification head training")

    trainer.train()

    trainer.model.save_pretrained("trained_models")

# %%

# Testing phase

from tqdm import tqdm

def evaluate_model(model, texts, labels, model_category=None):
    """Evaluate the trained model"""
    if model_category == 'zero-shot':
        predictions = []
        for text in tqdm(test_df.plaintext.to_list()):
            predictions += [pipe(text)['labels'][0]]

    else:
        predictions = model(texts)
    
    accuracy = accuracy_score(labels, predictions)
    report = classification_report(labels, predictions)
    
    print(f"Accuracy: {accuracy:.4f}")
    print("\nClassification Report:")
    print(report)
    
    return predictions

# Run test for few shot

test_df.ads_ground_truth = test_df.ads_ground_truth.replace(0, target_labels[0])
test_df.ads_ground_truth = test_df.ads_ground_truth.replace(1, target_labels[1])

evaluate_model(model, test_df.plaintext.to_list(), test_df.ads_ground_truth.to_list())

# %%

# Compare with zero shot

from transformers import pipeline
import pickle 

mode_bis = 'online'

if mode_bis == 'local':
    with open('zeroshot_pipe.pkl', 'rb') as f:
        pipe = pickle.load(f)

else:
    pipe = pipeline("zero-shot-classification", batch_size=16, candidate_labels=['news', 'advertising']) 
    with open('zeroshot_pipe.pkl', 'wb') as f:
        pickle.dump(pipe, f)

# Run Zero Shot predictions

predictions = evaluate_model(pipe, test_df.plaintext.to_list(), test_df.ads_ground_truth.to_list(), model_category='zero-shot')
