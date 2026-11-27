import json
import os
import uuid
from datetime import datetime

from datasets import load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from mistralai import Mistral

load_dotenv()
login(token=os.getenv("HF_TOKEN"))

prompt = """
You are an assistant helping editors to moderate TV and radio content.
You will be provided with a transcript delimited by triple backticks.
Bare in mind that the transcript may be missing punctuation and may be of very low quality, with incorrect vocabulary, cuts in the wrong places, or may include some phonetic transcription.

Does the text promote climate change misinformation that undermines well-established scientific consensus, such as denying the existence of climate change or the factors that contribute to it ?

Answer just a number between 0 - does not promote - and 1 - certainly promotes.

text: {transcript}
"""

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
            "messages": [
                {"role": "user", "content": prompt.format(transcript=example["plaintext"])},
                {"role": "assistant", "content": str(10 * int(example["misinformation"]))},
            ]
        }
    )
    dataset = dataset.select_columns(["messages"])
    return dataset

def data_to_jsonl(dataset_split):
    record_data = []
    for record in dataset_split:
        record_data.append(json.dumps(record))
    jsonl_data = "\n".join(record_data)
    return jsonl_data


def get_file_path(split):
    if split not in ("training", "validation"):
        raise ValueError("split must be either training or valuation")
    return os.path.join(
        DATA_DIR,
        f"{split}_file_completions_{today}.jsonl"
    )

if __name__ == "__main__":
    DATA_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data"
    )
    dataset = get_data()
    today = datetime.now().strftime("%Y_%m_%d")

    api_key = os.getenv("MISTRAL_API_KEY")

    client = Mistral(api_key=api_key)

    train_data = data_to_jsonl(dataset["train"])
    test_data = data_to_jsonl(dataset["test"])



    with open(get_file_path("training"), "w") as f:
        f.write(train_data)
    with open(get_file_path("validation"), "w") as f:
        f.write(test_data)

    training_data = client.files.upload(
        file={
            "file_name": get_file_path("training"),
            "content": open(get_file_path("training"), "rb"),
        }
    )

    validation_data = client.files.upload(
        file={
            "file_name": get_file_path("validation"),
            "content": open(get_file_path("validation"), "rb"),
        }
    )

    # create a fine-tuning job
    created_jobs = client.fine_tuning.jobs.create(
        model="mistral-small-latest",
        training_files=[{"file_id": training_data.id, "weight": 1}],
        validation_files=[validation_data.id],
        hyperparameters={
            "epochs": 1,
            "learning_rate":0.0001
        },
        auto_start=True,
        integrations=[
            dict(
                entity=os.getenv("WANDB_ENTITY", "gmguarino"),
                project="mistral-completions",
                name=f"mistral-completions-climateguard"
                + str(uuid.uuid4()).split("-")[0],
                api_key=os.getenv("WANDB_KEY")
            )
        ]
    )


