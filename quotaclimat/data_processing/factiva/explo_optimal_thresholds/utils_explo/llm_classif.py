import asyncio
import json
import os
from typing import Any, Dict, List

import nest_asyncio
import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import AzureChatOpenAI
from pydantic import BaseModel, Field, ValidationError
from tqdm import tqdm

from quotaclimat.data_processing.factiva.explo_optimal_thresholds.utils_explo.prompts.prompt_crises import (
    PROMPT_CRISES,
)

nest_asyncio.apply()

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print("Project root:", project_root)

def find_project_root(current_path, marker_file=".env"):
    # Start from the directory of the file
    current_path = os.path.dirname(current_path)
    while current_path != os.path.dirname(current_path):
        if marker_file in os.listdir(current_path):
            return current_path
        current_path = os.path.dirname(current_path)
    return None


def instantiate_openai_llm(prefix):
    env_prefix = prefix.upper()
    try:
        endpoint = os.getenv(f"{env_prefix}_ENDPOINT")
        deployment = os.getenv(f"{env_prefix}_DEPLOYMENT")
        api_version = os.getenv(f"{env_prefix}_API_VERSION")
        api_key = os.getenv(f"{env_prefix}_KEY")
        if not all([endpoint, deployment, api_version, api_key]):
            raise ValueError(
                f"Missing one or more environment variables for prefix '{env_prefix}'"
            )
        return AzureChatOpenAI(
            azure_endpoint=endpoint,
            azure_deployment=deployment,
            api_version=api_version,
            api_key=api_key,
        )
    except Exception as e:
        print(
            f"Error while instantiating AzureChatOpenAI model for prefix '{env_prefix}': {e}"
        )
        return None


# Get the project root path
project_root = find_project_root(os.path.abspath(__file__))
print("Project root:", project_root)


# Load environment variables from the .env file located at the project root
if project_root is not None:
    env_path = os.path.join(project_root, ".env")
    load_dotenv(env_path)
    print(f"Environment variables loaded from: {env_path}")
else:
    print("Unable to find the project root to load the .env file")

llm_gpt_4o_mini = instantiate_openai_llm("gpt4o_mini")
llm_gpt_4_1 = instantiate_openai_llm("gpt4_1")

# 1. System prompt: defines the LLM's task, label definitions, rules, and expected JSON output.
# Pydantic schema & parser for structured output
class Evidence(BaseModel):
    crise: str
    maillon: str
    texte: str


class ClassificationOutput(BaseModel):
    crises: List[str] = Field(default_factory=list)
    maillons_par_crise: Dict[str, List[str]] = Field(default_factory=dict)
    evidences: List[Evidence] = Field(default_factory=list)


classification_parser = PydanticOutputParser(
    pydantic_object=ClassificationOutput
)


def _extract_json_from_content(content: str) -> str:
    start = content.find("{")
    end = content.rfind("}")
    if start >= 0 and end > start:
        return content[start : end + 1]
    return content


def _normalize_langchain_content(content: Any) -> str:
    if isinstance(content, list):
        content = "".join(
            block.get("text", "") if isinstance(block, dict) else str(block)
            for block in content
        )
    if not isinstance(content, str):
        content = str(content)
    return content


def _parse_with_pydantic(content: str) -> Dict[str, Any]:
    clean_content = _extract_json_from_content(content)
    try:
        parsed = classification_parser.parse(clean_content)
        return parsed.model_dump()
    except (ValidationError, json.JSONDecodeError) as e:
        raise ValueError(f"Unable to parse LLM output → {e}")


# 2. Calls the LLM with article text and parses the returned JSON
def classify_article_text(text: str, llm, max_retries: int = 2, system_prompt: str = PROMPT_CRISES) -> Dict[str, Any]:
    """
    Sends article text to the LLM and parses the response as JSON.
    Returns a Python dict with the expected structure.
    If parsing fails, returns an empty dict.
    """
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=text),
    ]
    last_err = None
    for _ in range(max_retries + 1):
        try:
            resp = llm.invoke(messages)
            content = getattr(resp, "content", resp)

            content = _normalize_langchain_content(content)
            return _parse_with_pydantic(content)
        except Exception as e:
            last_err = e

    print(f"[WARN] Failed to parse model response as JSON: {last_err}")
    return {}


# 3. Utility: convert LLM result into CSV-friendly columns
def extract_cols_from_result(res: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts columns for CSV output:
    - 'crises': a flat comma-separated string of detected crises
    - 'maillons_par_crise': a flat comma-separated list of <crise>_<maillon>
    - 'raw_json': full model output as JSON string (for audit/debug)
    - 'confiance': optional confidence score
    """
    VALID_CRISES = {"climat", "biodiversite", "ressources"}
    VALID_MAILLONS = {"cause", "constat", "consequence", "solution"}

    crises = []
    maillons_per_crise = []

    # Extract and validate crises
    for c in res.get("crises", []):
        if c in VALID_CRISES:
            crises.append(c)

    # Extract and validate maillons per crisis
    mpc = res.get("maillons_par_crise", {}) or {}
    for c, maillons in mpc.items():
        if c not in VALID_CRISES:
            continue
        for m in maillons:
            if m in VALID_MAILLONS:
                maillons_per_crise.append(f"{c}_{m}")

    # Build output strings
    crises_str = ", ".join(sorted(crises))
    maillons_par_crise_str = ", ".join(sorted(maillons_per_crise))

    return {
        "crises": crises_str,
        "maillons_par_crise": maillons_par_crise_str,
        "raw_json": json.dumps(res, ensure_ascii=False),
    }


# 4. Apply classification to all rows of a DataFrame
async def classify_dataframe_async(
    df: pd.DataFrame, text_col: str, llm, system_prompt: str = PROMPT_CRISES, batch_size: int = 5
) -> pd.DataFrame:
    outputs: List[Dict[str, Any]] = []

    texts = df[text_col].astype(str).tolist()

    # Préparer les messages
    all_messages: List[List[BaseMessage]] = [
        [SystemMessage(content=system_prompt), HumanMessage(content=text)]
        for text in texts
    ]

    batches = [
        all_messages[i : i + batch_size]
        for i in range(0, len(all_messages), batch_size)
    ]
    print(f"Nombre total de textes : {len(texts)}")
    print(f"Nombre de batchs : {len(batches)}")

    for batch_idx, batch in enumerate(
        tqdm(batches, desc="Classification async par batch")
    ):
        try:
            responses = await llm.abatch(batch)
        except Exception as e:
            print(f"[ERROR] Échec du batch {batch_idx + 1} → {e}")
            responses = [None] * len(batch)

        for i, resp in enumerate(responses):
            index = batch_idx * batch_size + i
            if resp is None:
                outputs.append(extract_cols_from_result({}))
                continue

            try:
                content = getattr(resp, "content", resp)
                normalized = _normalize_langchain_content(content)
                parsed = _parse_with_pydantic(normalized)
                out = extract_cols_from_result(parsed)
            except Exception as e:
                print(f"[ERROR] Parsing ligne {index} → {e}")
                out = extract_cols_from_result({})
            outputs.append(out)

    out_df = df.copy()
    out_df = pd.concat([out_df, pd.DataFrame(outputs)], axis=1)
    return out_df


# 5. CSV file wrapper: loads, classifies, writes result
def classify_csv(
    in_path: str, out_path: str, *, text_col: str = "text", llm=None, sep: str = ",", system_prompt: str = PROMPT_CRISES
):
    if llm is None:
        raise ValueError("llm must be provided.")
    df = pd.read_csv(in_path, sep=sep)

    async def run_async():
        return await classify_dataframe_async(df, text_col=text_col, llm=llm, system_prompt=system_prompt)

    try:
        out_df = asyncio.run(run_async())
    except RuntimeError:
        # If we are in a notebook or interactive script, fallback
        import nest_asyncio

        nest_asyncio.apply()
        out_df = asyncio.get_event_loop().run_until_complete(run_async())

    out_df.to_csv(out_path, index=False)
    return out_df