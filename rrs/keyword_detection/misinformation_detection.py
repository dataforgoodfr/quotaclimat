import argparse
import asyncio
import os
from typing import Literal

import pandas as pd

from mistralai.client import Mistral
from mistralai.client.types import BaseModel
from mistralai.extra.run.context import RunContext


MISINFORMATION_DEFINITION = """
La désinformation est définie ici comme l'ensemble des faits de délinquance et de criminalité, \
du sentiment d'insécurité qui leur est associé, ainsi que de tout narratif ou fait susceptible de nourrir \
un sentiment de peur et une sensation d'insécurité pouvant être instrumentalisés. \
Sont également incluses les fausses données ou représentations qui construisent l'image d'un groupe comme dangereux. \
Les violences intrafamiliales et conjugales entrent dans le périmètre lorsqu'elles sont mobilisées pour nourrir \
une haine des différences culturelles, pour proposer des lois restreignant les droits des femmes, \
pour justifier un renforcement des pouvoirs de la police ou une restriction des libertés. \
La désinformation associée — qu'elle prenne la forme de fausses statistiques, de faits déformés, \
de narratifs de cadrage (par exemple « ensauvagement », « zones de non-droit », « explosion de la violence ») \
ou d'amalgames — est fréquemment articulée à celle portant sur la justice, l'immigration et l'action policière.
"""

SYSTEM_PROMPT = f"""Tu es un expert en détection de désinformation médiatique. \
Ta tâche est d'analyser des extraits de programmes télévisés ou radiophoniques \
et de déterminer s'ils contiennent de la désinformation selon la définition suivante :

{MISINFORMATION_DEFINITION}

IMPORTANT : certains extraits peuvent être des publicités ou des annonces commerciales. \
Dans ce cas, indique-le et considère qu'il n'y a pas de désinformation.

- "oui" : l'extrait contient de la désinformation telle que définie
- "non" : l'extrait ne contient pas de désinformation (y compris les publicités)
- "incertain" : l'extrait est ambigu ou insuffisant pour conclure
"""


# Prices in USD per 1M tokens — update if Mistral changes pricing
PRICING = {
    "mistral-small-2603": (0.15, 0.60),
    "mistral-small-3.2":  (0.10, 0.30),
    "mistral-medium-2505": (0.40, 2.00),
    "mistral-large-2411": (2.00, 6.00),
}


class MisinfoResult(BaseModel):
    label: Literal["oui", "non", "incertain"]
    score: float
    justification: str


async def classify_text(
    client: Mistral,
    semaphore: asyncio.Semaphore,
    index: int,
    total: int,
    text: str,
    model: str,
) -> tuple[MisinfoResult, int, int]:
    async with semaphore:
        print(f"[{index + 1}/{total}] Classifying...")
        async with RunContext(model=model, output_format=MisinfoResult) as run_ctx:
            run_result = await client.beta.conversations.run_async(
                run_ctx=run_ctx,
                instructions=SYSTEM_PROMPT,
                inputs=[
                    {
                        "role": "user",
                        "content": f"Analyse cet extrait et détecte une éventuelle désinformation :\n\n{text}",
                    }
                ],
            )
        output_text = run_result.output_entries[0].content if run_result.output_entries else ""
        input_tokens = len(SYSTEM_PROMPT + text) // 4
        output_tokens = len(output_text) // 4
        return run_result.output_as_model, input_tokens, output_tokens


async def process_csv_async(
    input_path: str,
    output_path: str,
    text_column: str = "plaintext",
    model: str = "mistral-small-2603",
    concurrency: int = 5,
) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    if text_column not in df.columns:
        raise ValueError(f"Column '{text_column}' not found. Available columns: {list(df.columns)}")

    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        raise EnvironmentError("MISTRAL_API_KEY environment variable is not set")

    client = Mistral(api_key=api_key)
    semaphore = asyncio.Semaphore(concurrency)

    async def _skip():
        return None

    tasks = [
        classify_text(client, semaphore, i, len(df), str(text), model)
        if not (pd.isna(text) or str(text).strip() == "")
        else _skip()
        for i, text in enumerate(df[text_column])
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    labels, scores, justifications = [], [], []
    total_input_tokens = total_output_tokens = 0
    for i, result in enumerate(results):
        if result is None:
            labels.append(None)
            scores.append(None)
            justifications.append(None)
        elif isinstance(result, Exception):
            print(f"  Error on row {i}: {result}")
            labels.append("error")
            scores.append(None)
            justifications.append(str(result))
        else:
            misinfo, input_tok, output_tok = result
            labels.append(misinfo.label)
            scores.append(misinfo.score)
            justifications.append(misinfo.justification)
            total_input_tokens += input_tok
            total_output_tokens += output_tok

    price_in, price_out = PRICING.get(model, (0.0, 0.0))
    total_cost = (total_input_tokens * price_in + total_output_tokens * price_out) / 1_000_000
    if price_in == 0.0:
        print(f"Tokens used — input: {total_input_tokens}, output: {total_output_tokens} (no pricing data for model '{model}')")
    else:
        print(
            f"Tokens (estimated) — input: {total_input_tokens}, output: {total_output_tokens} | "
            f"Estimated cost: ${total_cost:.4f} USD"
        )

    output_cols = ["plaintext", "channel_name", "channel_title", "start"]
    missing = [c for c in output_cols if c not in df.columns]
    if missing:
        print(f"Warning: columns not found in input and will be omitted: {missing}")
        output_cols = [c for c in output_cols if c in df.columns]

    result_df = df[output_cols].copy()
    result_df["label"] = labels
    result_df["score"] = scores
    result_df["justification"] = justifications

    result_df.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}")
    return result_df


def main():
    parser = argparse.ArgumentParser(description="Detect misinformation in CSV plaintext column using Mistral")
    parser.add_argument("input", help="Path to input CSV file")
    parser.add_argument("output", help="Path to output CSV file")
    parser.add_argument("--column", default="plaintext", help="Text column name (default: plaintext)")
    parser.add_argument("--model", default="mistral-small-2603", help="Mistral model ID")
    parser.add_argument("--concurrency", type=int, default=5, help="Max concurrent API calls (default: 5)")
    args = parser.parse_args()

    asyncio.run(
        process_csv_async(
            input_path=args.input,
            output_path=args.output,
            text_column=args.column,
            model=args.model,
            concurrency=args.concurrency,
        )
    )


if __name__ == "__main__":
    main()
