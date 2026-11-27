import asyncio
import os
import re
import uuid
from datetime import datetime

import pandas as pd
from datasets import load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from openai import AsyncOpenAI
from sklearn.metrics import classification_report
from tqdm.asyncio import tqdm

MAX_CONCURRENCY = 20


load_dotenv()
login(token=os.getenv("HF_TOKEN"))

prompt = """
You are an assistant helping editors to moderate TV and radio content.
You will be provided with a transcript delimited by triple backticks.
Bear in mind that the transcript may be missing punctuation and may be of very low quality, with incorrect vocabulary, cuts in the wrong places, or may include some phonetic transcription.

Does the text promote climate change misinformation that undermines well-established scientific consensus, such as denying the existence of climate change or the factors that contribute to it ?

Answer just a number on a scale of 0 - does not promote - to 10 - certainly promotes.

text: ```{transcript}```
"""


def get_data(split: str):
    dataset = load_dataset("DataForGood/climateguard", split=split)

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
            "output": int(example["misinformation"]),
        }
    )
    dataset = dataset.select_columns(["input", "output"])
    return dataset


# Matches jobs/label_misinformation/app/pipeline.py's parse_response: the
# model is asked for a bare number, no JSON wrapper.
SCORE_RE = re.compile(r"^[^\d]*(\d+)")


async def _complete(client: AsyncOpenAI, model: str, record: dict, temperature: float):
    chat_response = await client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": record["input"],
            },
        ],
        temperature=temperature,
        max_completion_tokens=512,
        presence_penalty=0.5,
        frequency_penalty=0.5,
        stop=["\t\t\t"],
    )
    return chat_response.choices[0].message.content


def _extract(content: str):
    """Return (score, ok) where ok is False if parsing/extraction failed."""
    match = SCORE_RE.match(content.strip())
    if match:
        return int(match.group(1)), True
    return 0, False


async def classify(client: AsyncOpenAI, model: str, semaphore: asyncio.Semaphore, record: dict):
    async with semaphore:
        try:
            content = await _complete(client, model, record, temperature=0.05)
        except Exception as e:
            print(f"Error calling the API, defaulting to 0: {e}")
            return 0, str(e)

        prediction, ok = _extract(content)
        if ok:
            return prediction, content

        # Likely a degenerate repetition loop (possibly cut off by `stop`) that
        # left no leading digit. A single retry at a higher temperature rarely
        # re-enters the same loop.
        try:
            retry_content = await _complete(client, model, record, temperature=0.3)
        except Exception as e:
            print(f"Retry failed, defaulting to 0: {e}")
            return 0, content

        retry_prediction, retry_ok = _extract(retry_content)
        if retry_ok:
            return retry_prediction, retry_content

        print(f"Could not parse response after retry, defaulting to 0: {retry_content!r}")
        return 0, retry_content


async def run(client: AsyncOpenAI, model: str, dataset):
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    tasks = [classify(client, model, semaphore, record) for record in dataset]
    return await tqdm.gather(*tasks)


async def main(model: str):
    api_key = os.getenv("OPENAI_KEY")
    client = AsyncOpenAI(api_key=api_key)
    # Matches jobs/label_misinformation/app/main.py: scores >= this threshold
    # are flagged as misinformation.
    min_misinformation_score = int(os.getenv("MIN_MISINFORMATION_SCORE", 10))

    for split in ["train", "test"]:
        dataset = get_data(split)

        results = await run(client, model, dataset)
        predictions = [prediction for prediction, _ in results]
        raw_results = [raw_result for _, raw_result in results]
        binarized_predictions = [
            int(prediction >= min_misinformation_score) for prediction in predictions
        ]

        df_results = pd.DataFrame(
            {
                "predictions": predictions,
                "binarized_predictions": binarized_predictions,
                "raw_results": raw_results,
                "labels": dataset.to_pandas()["output"].to_list(),
            }
        )
        df_results.to_csv(f"raw_results_{split}.csv")
        print(f"=== {split} ===")
        print(
            classification_report(
                dataset.to_pandas()["output"].to_list(), binarized_predictions
            )
        )


if __name__ == "__main__":
    asyncio.run(main("ft:gpt-4o-mini-2024-07-18:personal::B1xWiJRm"))
