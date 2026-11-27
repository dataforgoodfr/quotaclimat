from datasets import load_dataset
from sentence_transformers import CrossEncoder
from sklearn.metrics import classification_report
from tqdm import tqdm

BATCH_SIZE = 4

model = CrossEncoder("cross-encoder/nli-deberta-v3-base")

# Convert scores to labels
label_mapping = ["Refutes", "Supports", "Not Enough Information"]
dataset = load_dataset("rabuahmad/climatecheck", split="train")

dataset = dataset.map(
    lambda row: {
        "label": label_mapping.index(row["annotation"]),
        "n_tokens": len(model.tokenizer.tokenize(row["abstract"])),
    }
)

predictions = []
predicted_labels = []
add_on = int(len(dataset) % BATCH_SIZE > 0)
print(add_on)
for batch_idx in tqdm(range(len(dataset) // BATCH_SIZE + add_on)):
    batch = dataset.select(
        range(batch_idx * BATCH_SIZE, min(len(dataset), (batch_idx + 1) * BATCH_SIZE))
    )
    scores = model.predict(list(zip(batch["abstract"], batch["claim"])))
    _predictions = [score_max for score_max in scores.argmax(axis=1)]
    _labels = [label_mapping[score_max] for score_max in scores.argmax(axis=1)]
    predictions.extend(_predictions)
    predicted_labels.extend(_labels)


print("Annotations")
print(dataset["annotation"][:10])
print("Predictions")
print(predicted_labels[:10])

print(classification_report(dataset["label"], predictions, target_names=label_mapping))
