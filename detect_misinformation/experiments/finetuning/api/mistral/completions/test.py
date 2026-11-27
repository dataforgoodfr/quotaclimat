import asyncio
import json
import os
import re
import uuid
from datetime import datetime

import pandas as pd
from datasets import load_dataset
from dotenv import load_dotenv
from huggingface_hub import login
from mistralai.client import Mistral
from mistralai.extra.utils.response_format import response_format_from_pydantic_model
from pydantic import BaseModel, Field
from sklearn.metrics import classification_report
from tqdm.asyncio import tqdm

MAX_CONCURRENCY = 20


load_dotenv()
login(token=os.getenv("HF_TOKEN"))

prompt = """
Tu es un assistant qui aide des éditeurs à modérer des contenus TV et radio.
Tu vas recevoir une transcription délimitée par des triples guillemets.
Attention, cette transcription peut être mal ponctuée et de très mauvaise qualité, avec un vocabulaire incorrect, des coupures mal placées, ou une transcription phonétique approximative.

La désinformation climatique est tout contenu qui contredit le consensus scientifique établi ou propage des narratifs trompeurs sur le changement climatique, selon trois axes :
- Science climatique : nier ou minimiser les causes humaines du réchauffement, contester l'existence ou la gravité de la crise climatique, ou déformer les projections du GIEC.
- Action climatique : discréditer les politiques climatiques (Accord de Paris, lois climat), présenter l'inaction comme légitime, ou instrumentaliser de fausses données pour bloquer la régulation.
- Solutions d'atténuation et d'adaptation : tromper sur l'efficacité, le coût ou la faisabilité des solutions reconnues par le GIEC (renouvelables, efficacité énergétique, capture de carbone, etc.).

Sont également concernés les chiffres falsifiés ou sortis de contexte, les corrélations abusives, les théories du complot sur les acteurs de la transition, et les amalgames entre action climatique et agendas idéologiques sans base factuelle.

Le texte ci-dessous promeut-il de la désinformation climatique telle que définie ci-dessus ?
En cas de doute, considère que le texte promeut de la désinformation : nous cherchons à maximiser le rappel, il vaut mieux un faux positif qu'un faux négatif.

Réfléchis d'abord brièvement (2 à 3 phrases maximum) en donnant seulement ta propre analyse, sans jamais citer ni reformuler entre guillemets un extrait du texte, puis donne ta réponse finale.
N'utilise aucun markdown (pas de gras, italique, listes, titres ou guillemets typographiques) : réponds uniquement en texte brut.
N'ouvre jamais de guillemets ("), ils provoquent des boucles infinies de génération.

texte: ```{transcript}```
"""


class MisinformationClassification(BaseModel):
    reasoning: str = Field(
        description="Brève analyse du texte expliquant s'il promeut ou non de la désinformation climatique, avant de donner la réponse finale."
    )
    misinformation: bool = Field(
        description="Whether the text promotes climate change misinformation that undermines well-established scientific consensus, as defined above."
    )

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


RESPONSE_FORMAT = response_format_from_pydantic_model(MisinformationClassification)
MISINFORMATION_FIELD_RE = re.compile(r'"misinformation"\s*:\s*(true|false)', re.IGNORECASE)


async def _complete(client: Mistral, model: str, record: dict, temperature: float):
    chat_response = await client.chat.complete_async(
        model=model,
        messages=[
            {
                "role": "user",
                "content": record["input"],
            },
        ],
        response_format=RESPONSE_FORMAT,
        temperature=temperature,
        max_tokens=512,
        presence_penalty=0.5,
        frequency_penalty=0.5,
        stop=["\t\t\t"],
    )
    return chat_response.choices[0].message.content


def _extract(content: str):
    """Return (misinformation, ok) where ok is False if parsing/extraction failed."""
    try:
        result = MisinformationClassification.model_validate_json(content)
        return int(result.misinformation), True
    except Exception:
        # The model can emit invalid JSON (e.g. an unescaped quote inside the
        # free-text `reasoning` field, or a truncated response after a repetition
        # loop got cut off by the `stop` sequence). Fall back to extracting just
        # the `misinformation` boolean, which is short and rarely malformed.
        match = MISINFORMATION_FIELD_RE.search(content)
        if match:
            return int(match.group(1).lower() == "true"), True
        return 0, False


async def classify(client: Mistral, model: str, semaphore: asyncio.Semaphore, record: dict):
    async with semaphore:
        try:
            content = await _complete(client, model, record, temperature=0.05)
        except Exception as e:
            print(f"Error calling the API, defaulting to 0: {e}")
            return 0, str(e)

        prediction, ok = _extract(content)
        if ok:
            return prediction, content

        # Likely a degenerate repetition loop cut off by `stop`. A single retry
        # at a higher temperature rarely re-enters the same loop.
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


async def run(client: Mistral, model: str, dataset):
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    tasks = [classify(client, model, semaphore, record) for record in dataset]
    return await tqdm.gather(*tasks)


async def main(model: str):
    api_key = os.getenv("MISTRAL_API_KEY")
    client = Mistral(api_key=api_key)

    for split in ["train", "test"]:
        dataset = get_data(split)

        results = await run(client, model, dataset)
        predictions = [prediction for prediction, _ in results]
        raw_results = [raw_result for _, raw_result in results]

        df_results = pd.DataFrame(
            {
                "predictions": predictions,
                "raw_results": raw_results,
                "labels": dataset.to_pandas()["output"].to_list(),
            }
        )
        df_results.to_csv(f"raw_results_{split}.csv")
        print(f"=== {split} ===")
        print(classification_report(dataset.to_pandas()["output"].to_list(), predictions))


if __name__ == "__main__":
    asyncio.run(main("mistral-small-2603"))
