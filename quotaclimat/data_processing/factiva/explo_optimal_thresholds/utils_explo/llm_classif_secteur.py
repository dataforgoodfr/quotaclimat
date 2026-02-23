"""
Sector classifier for environmental press articles.
Uses an LLM (Azure OpenAI) and Pydantic for structured output.
"""
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
from pydantic import BaseModel, Field, ValidationError, field_validator
from tqdm import tqdm

from quotaclimat.data_processing.factiva.explo_optimal_thresholds.utils_explo.prompts.prompt_secteurs import (
    PROMPT_SECTEURS,
)

nest_asyncio.apply()

# Allowed sectors (aligned with prompt)
SECTEURS_VALIDES = {
    "Agriculture & alimentation",
    "Bâtiment & aménagement",
    "Économie circulaire",
    "Economie circulaire",
    "Mobilité",
    "Eau",
    "Ecosystème",
    "Energie",
    "Industrie",
    "Transversal",
    "Autre",
}


def find_project_root(current_path: str, marker_file: str = ".env") -> str | None:
    current_path = os.path.dirname(current_path)
    while current_path != os.path.dirname(current_path):
        try:
            if marker_file in os.listdir(current_path):
                return current_path
        except OSError:
            pass
        current_path = os.path.dirname(current_path)
    return None


def instantiate_openai_llm(prefix: str) -> AzureChatOpenAI | None:
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


project_root = find_project_root(os.path.abspath(__file__))
if project_root is not None:
    env_path = os.path.join(project_root, ".env")
    load_dotenv(env_path)
else:
    load_dotenv()

llm_gpt_4o_mini = instantiate_openai_llm("gpt4o_mini")
llm_gpt_4_1 = instantiate_openai_llm("gpt4_1")


# Pydantic models for sector output (enforce LLM response shape)

class ClassificationSecteurOutput(BaseModel):
    secteurs: List[str] = Field(default_factory=list)
    evidences: List[Dict[str, str]] = Field(default_factory=list)

    @field_validator("secteurs")
    @classmethod
    def secteurs_in_list(cls, v: List[str]) -> List[str]:
        for s in v:
            if s not in SECTEURS_VALIDES:
                raise ValueError(f"Invalid sector: {s!r}. Allowed: {SECTEURS_VALIDES}")
        return v


classification_secteur_parser = PydanticOutputParser(
    pydantic_object=ClassificationSecteurOutput
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


def _parse_secteur_with_pydantic(content: str) -> Dict[str, Any]:
    clean_content = _extract_json_from_content(content)
    try:
        parsed = classification_secteur_parser.parse(clean_content)
        return parsed.model_dump()
    except (ValidationError, json.JSONDecodeError) as e:
        raise ValueError(f"Unable to parse LLM output → {e}")


def classify_article_text_secteur(
    text: str,
    llm: Any,
    max_retries: int = 2,
    system_prompt: str = PROMPT_SECTEURS,
) -> Dict[str, Any]:
    """Send article text to the LLM and parse response as sector JSON. Returns dict with 'secteurs' and 'evidences'."""
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
            return _parse_secteur_with_pydantic(content)
        except Exception as e:
            last_err = e

    print(f"[WARN] Failed to parse model response as JSON secteurs: {last_err}")
    return {"secteurs": [], "evidences": []}


def extract_cols_from_result_secteur(res: Dict[str, Any]) -> Dict[str, Any]:
    """Build CSV/DataFrame columns: llm_secteurs (comma-separated), llm_evidences (raw JSON for audit)."""
    secteurs = [
        s for s in res.get("secteurs", [])
        if s in SECTEURS_VALIDES
    ]
    return {
        "llm_secteurs": ", ".join(sorted(secteurs)),
        "llm_secteur_evidences": json.dumps(res, ensure_ascii=False),
    }


async def classify_dataframe_secteur_async(
    df: pd.DataFrame,
    text_col: str,
    llm: Any,
    system_prompt: str = PROMPT_SECTEURS,
    batch_size: int = 5,
) -> pd.DataFrame:
    """Run sector classification on every row of the DataFrame."""
    outputs: List[Dict[str, Any]] = []
    texts = df[text_col].astype(str).tolist()

    all_messages: List[List[BaseMessage]] = [
        [SystemMessage(content=system_prompt), HumanMessage(content=text)]
        for text in texts
    ]

    batches = [
        all_messages[i : i + batch_size]
        for i in range(0, len(all_messages), batch_size)
    ]
    print(f"Total texts: {len(texts)}, batches: {len(batches)}")

    for batch_idx, batch in enumerate(
        tqdm(batches, desc="Sector classification async by batch")
    ):
        try:
            responses = await llm.abatch(batch)
        except Exception as e:
            print(f"[ERROR] Batch {batch_idx + 1} failed: {e}")
            responses = [None] * len(batch)

        for i, resp in enumerate(responses):
            index = batch_idx * batch_size + i
            if resp is None:
                outputs.append(extract_cols_from_result_secteur({"secteurs": [], "evidences": []}))
                continue

            try:
                content = getattr(resp, "content", resp)
                normalized = _normalize_langchain_content(content)
                parsed = _parse_secteur_with_pydantic(normalized)
                out = extract_cols_from_result_secteur(parsed)
            except Exception as e:
                print(f"[ERROR] Parse row {index}: {e}")
                out = extract_cols_from_result_secteur({"secteurs": [], "evidences": []})
            outputs.append(out)

    out_df = df.copy()
    out_df = pd.concat([out_df, pd.DataFrame(outputs)], axis=1)
    return out_df


def classify_csv_secteur(
    in_path: str,
    out_path: str,
    *,
    text_col: str = "text",
    llm: Any = None,
    sep: str = ",",
    system_prompt: str = PROMPT_SECTEURS,
) -> pd.DataFrame:
    if llm is None:
        raise ValueError("llm must be provided.")
    df = pd.read_csv(in_path, sep=sep)

    async def run_async():
        return await classify_dataframe_secteur_async(
            df, text_col=text_col, llm=llm, system_prompt=system_prompt
        )

    try:
        out_df = asyncio.run(run_async())
    except RuntimeError:
        nest_asyncio.apply()
        out_df = asyncio.get_event_loop().run_until_complete(run_async())

    out_df.to_csv(out_path, index=False)
    return out_df
