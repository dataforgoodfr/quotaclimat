import asyncio
import re

import evaluate
import pandas as pd
import torch
from datasets import load_dataset
from dotenv import load_dotenv
from ollama import AsyncClient, Client
from openai import AsyncOpenAI
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

load_dotenv()

# Initialize client
client_ollama = AsyncClient()
ollama_model_id = "hf.co/kurakurai/Luth-LFM2-350M-GGUF:latest"


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
    tokenizer = AutoTokenizer.from_pretrained(
        model_name, torch_dtype=torch.float16, device_map="auto"
    )
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


async def fetch_response_openai(semaphore, prompt_id, prompt):
    """Fetch a response for a single prompt with semaphore limiting concurrency."""
    async with semaphore:
        response = await client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
        )
        return {
            "id": prompt_id,
            "prompt": prompt,
            "response": response.choices[0].message.content,
        }


async def fetch_response_ollama(semaphore, prompt_id, prompt):
    """Fetch a response for a single prompt with semaphore limiting concurrency."""
    async with semaphore:
        response = await client_ollama.generate(
            model=ollama_model_id,
            prompt=prompt,
        )

        return {
            "id": prompt_id,
            "prompt": prompt,
            "response": response.response,
        }


async def get_responses(prompts, max_concurrent=5, function=fetch_response_openai):
    """
    Get responses for a list of prompts concurrently but with limited concurrency.

    Args:
        prompts (list[str]): List of prompts to send to GPT.
        max_concurrent (int): Maximum number of concurrent requests.

    Returns:
        list[dict]: List of dicts with id, prompt, and response, ordered by prompt id.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [function(semaphore, i, prompt) for i, prompt in enumerate(prompts)]

    results = await atqdm.gather(*tasks)

    # Sort by ID to preserve original order
    results.sort(key=lambda x: x["id"])
    return results


engine = "transformers"
# engine = "ollama"
# engine = "openai"

prompt = """
À partir d'une transcription d'une émission médiatique, vous devez extraire l'argument principal du texte,
dans le but d'identifier les affirmations qui ont été présentées comme des faits. 
Gardez à l'esprit que le texte peut être désordonné et manquer de ponctuation. 
Votre tâche consiste à comprendre le message principal qui est véhiculé.
N'oubliez pas de mentionner les personnes ou entités qui ont été mentionnées dans l'affirmation.
Soyez précis et concis. 
Répondez par une seule phrase courte par transcription, 
rédigez l'affirmation comme si vous la formuliez vous-même de façon très synthètique.

Voici la transcription :
{transcript}
"""

prompt_v2 = """
À partir d'une transcription, votre travail consiste à extraire toute affirmation qui pourrait être mentionnée dans le texte.
Une affirmation est un fait qui a été déclaré par un locuteur. Par exemple, une affirmation pourrait être :
« Les véhicules électriques sont l'avenir de la mobilité »
ou
« Le CO2 n'entraîne pas de réchauffement climatique ».
Gardez à l'esprit que la transcription peut être de mauvaise qualité et ne pas respecter la ponctuation appropriée.
Extrayez l'affirmation de manière concise. S'il y a deux affirmations dans le texte, écrivez-les sous forme de deux phrases distinctes
« Les humains ne sont pas responsables du changement climatique. Le CO2 n'induit pas de réchauffement climatique ».
Répondez par une ou deux seules affirmations par transcription.

Voici la transcription:
{transcript}
"""
dataset = load_dataset("DataForGood/climateguard", split="test")
dataset = dataset.filter(lambda example: example["claims"] != [])
print(dataset)
predictions = []
reference = []

if engine == "transformers":
    model_id = "gmguarino/climateguard-Luth-LFM2-350M-claim-extraction-dpo"

    for example in tqdm(dataset):
        output = call_causal_model(
            model_name=model_id,
            prompt=prompt.format(transcript=example["plaintext"]),
        )
        predictions.append(output)
        reference.append(". ".join(example["claims"]))

elif engine == "openai":
    client = AsyncOpenAI()
    prompts = []
    for example in tqdm(dataset):
        prompts.append(prompt.format(transcript=example["plaintext"]))
        reference.append(". ".join(example["claims"]))

    responses = asyncio.run(get_responses(prompts, max_concurrent=30))
    predictions = [response["response"] for response in responses]

elif engine == "ollama":
    prompts = []
    for example in tqdm(dataset):
        prompts.append(prompt_v2.format(transcript=example["plaintext"]))
        reference.append(". ".join(example["claims"]))

    responses = asyncio.run(get_responses(prompts, max_concurrent=10))
    predictions = [response["response"].replace("\n", "") for response in responses]

with open("predictions.txt", "w") as f:
    for pred in predictions:
        f.write(pred)
        f.write("\n")

with open("targets.txt", "w") as f:
    for ref in reference:
        f.write(ref)
        f.write("\n")


rouge = evaluate.load("rouge")
rouge_scores = rouge.compute(predictions=predictions, references=reference)
print(f"ROUGE scores on test set: {rouge_scores}")
