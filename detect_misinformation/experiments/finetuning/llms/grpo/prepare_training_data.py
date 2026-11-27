import argparse
import asyncio
import os
import sys
from typing import Any, Dict, Union

import jsonlines
from datasets import Dataset, DatasetDict, load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from numpy.random import randint
from openai import AsyncOpenAI, OpenAI
from tqdm.asyncio import tqdm

sys.path.append("./..")
from prompts import (
    prompt_chat,
    prompt_synthetic_information,
    prompt_synthetic_misinformation,
)

load_dotenv()
login(token=os.getenv("HF_TOKEN"))


async def synth_gen_model_reason(
    client: AsyncOpenAI, record: Dict[str, Any], model: str = "gpt-4o-mini"
):
    prompt_synthetic = (
        prompt_synthetic_misinformation
        if record["misinformation"]
        else prompt_synthetic_information
    )
    response = await client.responses.create(
        model=model, input=prompt_synthetic + record["plaintext"]
    )
    return response.output[0].content[0].text


async def generate_conversation(
    args: argparse.Namespace,
    client: AsyncOpenAI,
    record: Dict[str, str],
    semaphore: asyncio.Semaphore,
):
    messages = {"prompt": prompt_chat + record["plaintext"]}
    async with semaphore:
        reason = await synth_gen_model_reason(client=client, record=record)
        str_start = f"<think>\n{reason}\n</think>\n\n" if args.thinking else ""
        messages.update(
            {
                "answer": str_start
                + f"<misinformation>\n{record['misinformation']}\n</misinformation>"
            }
        )
        return messages


def generate_conversations(
    args: argparse.Namespace,
    client: AsyncOpenAI,
    dataset: Union[DatasetDict, Dataset],
    semaphore: asyncio.Semaphore,
):
    if isinstance(dataset, DatasetDict):
        dataset = dataset["train"]
    tasks = [
        generate_conversation(args, client, record, semaphore) for record in dataset
    ]

    conversations = asyncio.run(tqdm.gather(*tasks))

    return conversations


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dataset", type=str, default="DataForGood/climateguard")
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("-t", "--train", type=str, default="train.jsonl")
    parser.add_argument("-v", "--val", type=str, default="valid.jsonl")
    parser.add_argument("-s", "--split", type=float, default=0.1)
    parser.add_argument("--thinking", type=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--semaphore", type=int, default=10)
    args = parser.parse_args()

    train_dataset = load_dataset(
        args.dataset, download_mode="force_redownload", split="train"
    )

    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(args.semaphore)
    conversations = generate_conversations(
        args=args, client=client, dataset=train_dataset, semaphore=semaphore
    )

    n_validation = int((len(conversations) * args.split) // 1)
    validation_idxs = randint(0, len(conversations), n_validation)
    val_conversations = [conversations[idx] for idx in validation_idxs]
    train_conversations = [
        conversations[idx]
        for idx in range(len(conversations))
        if idx not in validation_idxs
    ]

    if not os.path.exists(args.data_dir):
        os.mkdir(args.data_dir)

    with jsonlines.open(os.path.join(args.data_dir, args.train), "w") as writer:
        writer.write_all(train_conversations)

    with jsonlines.open(os.path.join(args.data_dir, args.val), "w") as writer:
        writer.write_all(train_conversations)
