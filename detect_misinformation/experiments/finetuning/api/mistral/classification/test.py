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

def get_data():
    dataset = load_dataset("DataForGood/climateguard", split="test")

    dataset = dataset.filter(lambda example: example["comments"] == [])
    dataset = dataset.filter(
        lambda example: isinstance(example["misinformation"], bool)
    )
    dataset = dataset.filter(
        lambda example: not isinstance(example["misinformation"], str)
    )
    dataset = dataset.map(
        lambda example: {
            "input": prompt.format(transcript=example["plaintext"]),
            "output": 10 * int(example["misinformation"]),
        }
    )
    dataset = dataset.select_columns(["input", "output"])
    return dataset

if __name__ == "__main__":
    dataset = get_data()
    api_key = os.getenv("MISTRAL_API_KEY")
    model = "ft:mistral-small-latest:fed0b662:20251204:eb3d9c06"

    client = Mistral(api_key=api_key)

    for record in dataset:
        chat_response = client.chat.complete(
            model = model,
            messages = [
                {
                    "role": "user",
                    "content": record["input"],
                },
            ]
        )
        print(chat_response)
        break
