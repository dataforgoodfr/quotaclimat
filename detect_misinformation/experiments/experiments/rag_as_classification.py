import asyncio
import re
from typing import Dict, List

import torch
from datasets import load_dataset
from dotenv import load_dotenv
from openai import AsyncOpenAI
from sentence_transformers import CrossEncoder
from sklearn.metrics import classification_report
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

load_dotenv()

# Initialize client
client = AsyncOpenAI()


def call_causal_model(prompt: str, model_name: str):
    """
    Call a causal language model to generate text based on the provided prompt.

    Parameters:
    - prompt (str): The input text to be used as the starting point for generation.
    - model_name (str): The identifier of the pre-trained causal language model to use.

    Returns:
    - str: The generated text by the model, following the given prompt.
    """
    # load the tokenizer and the model
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(
        model_name, dtype="auto", device_map="auto"
    )
    messages = [{"role": "user", "content": prompt}]
    text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )
    model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

    with torch.no_grad():
        generated_ids = model.generate(**model_inputs, max_new_tokens=512)
    output_ids = generated_ids[0][len(model_inputs.input_ids[0]) :].tolist()
    content = tokenizer.decode(output_ids, skip_special_tokens=True).strip("\n")
    return content


def call_classifier(example: Dict[str, str], model: CrossEncoder):
    model = CrossEncoder("cross-encoder/nli-deberta-v3-base")
    scores = model.predict(
        [
            (example["abstract"], example["claim"]),
        ]
    )

    # Convert scores to labels
    label_mapping = ["Refutes", "Supports", "Not Enough Information"]
    map_labels = {"Supports": 0, "Refutes": 1, "Not Enough Information": 2}
    labels = [label_mapping[score_max] for score_max in scores.argmax(axis=1)]


def parse_re(text: str, pattern: str, verbose=False):
    """
    Parse the input text using the provided regular expression pattern.

    Parameters:
    - text (str): The input text to be parsed.
    - pattern (str): The regular expression pattern used for searching and matching in the text.
    - verbose (bool, optional): If True, print the entire text if no match is found. Default is False.

    Returns:
    - str: The matched pattern from the text if found; otherwise, an empty string.
    """

    match = re.search(pattern, text, re.DOTALL)
    if match:
        return pattern
    if verbose:
        print(text)
    return ""


def get_result(text: str) -> str:
    """
    Determine the verdict of an input text using predefined patterns.

    Parameters:
    - text (str): The input text to be analyzed.

    Returns:
    - str: The detected verdict, either 'supports', 'refutes', or 'na'.
    """
    for pattern in ("supports", "refutes", "na"):
        result = parse_re(text.lower(), pattern, verbose=pattern == "na")
        if result:
            return result
    return "na"


async def fetch_response(semaphore, prompt_id, prompt):
    """Fetch a response for a single prompt with semaphore limiting concurrency."""
    async with semaphore:
        response = await client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
        )
        return {
            "id": prompt_id,
            "prompt": prompt,
            "response": get_result(response.choices[0].message.content),
        }


async def get_responses(prompts, max_concurrent=5):
    """
    Get responses for a list of prompts concurrently but with limited concurrency.

    Args:
        prompts (list[str]): List of prompts to send to GPT.
        max_concurrent (int): Maximum number of concurrent requests.

    Returns:
        list[dict]: List of dicts with id, prompt, and response, ordered by prompt id.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [fetch_response(semaphore, i, prompt) for i, prompt in enumerate(prompts)]

    results = await atqdm.gather(*tasks)

    # Sort by ID to preserve original order
    results.sort(key=lambda x: x["id"])
    return results


engine = "transformers"
engine = "sentence-transformers"
# engine = "openai"

prompt = """
You are a fact checking assistant. Given a claim and a document, check if the document
refutes the claim, supports the claim, or does not have enough information on the claim.
Reply only with 'supports', 'refutes' or 'na'. Analyze the claim based only on the information
contained in the document.
claim: {claim}
document: {document}
"""
dataset = load_dataset("rabuahmad/climatecheck", split="train")
predictions = []
labels = []
map_labels = {"Supports": 0, "Refutes": 1, "Not Enough Information": 2}
map_predictions = {"supports": 0, "refutes": 1, "na": 2}

if engine == "transformers_llm":
    model_id = "LiquidAI/LFM2-1.2B"

    for example in tqdm(dataset):
        output = call_causal_model(
            model_name=model_id,
            prompt=prompt.format(claim=example["claim"], document=example["abstract"]),
        )
        result = get_result(output)
        predictions.append(map_predictions.get(result))
        labels.append(map_labels.get(example["annotation"]))

if engine == "transformers":
    model_id = "LiquidAI/LFM2-1.2B"

    for example in tqdm(dataset):
        output = call_causal_model(
            model_name=model_id,
            prompt=prompt.format(claim=example["claim"], document=example["abstract"]),
        )
        result = get_result(output)
        predictions.append(map_predictions.get(result))
        labels.append(map_labels.get(example["annotation"]))

elif engine == "openai":
    prompts = []
    for example in tqdm(dataset):
        prompts.append(
            prompt.format(claim=example["claim"], document=example["abstract"])
        )
        labels.append(map_labels.get(example["annotation"]))

    responses = asyncio.run(get_responses(prompts, max_concurrent=30))
    predictions = [map_predictions.get(response["response"]) for response in responses]

print(classification_report(labels, predictions))
