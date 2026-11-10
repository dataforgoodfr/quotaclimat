import importlib
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache
from typing import List, Optional, Tuple

import asyncio
import pandas as pd
import openai
from mistralai.client import Mistral
from mistralai.extra.utils.response_format import response_format_from_pydantic_model
from prompts import DisinformationPrompt, PROMPTS
from pydantic import BaseModel, Field, create_model
from secret_utils import get_secret_docker

# Non prod modules
if importlib.util.find_spec("llama_index"):
    from llama_index.core.node_parser import SentenceSplitter
if importlib.util.find_spec("transformers"):
    from transformers import AutoModelForSequenceClassification, AutoTokenizer
if importlib.util.find_spec("torch"):
    from torch import nn

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@dataclass
class PipelineInput:
    transcript: str

    # suggestion of other metadata that could be added
    # path to audio file for whisperization...
    audio_file: Optional[str] = None
    # source of the transcript ex: "sud-radio"
    source: Optional[str] = None
    # date and time of the original emission
    date: Optional[str] = None
    # Keywords ID
    id: Optional[str] = None


@dataclass
class PipelineOutput:
    # disinformation score (0: not disinformation - 10: disinformation)
    score: int
    # a brief reason about the score
    reason: str = ""
    # suggestion of other metadata that could be added
    cards_category: Optional[str] = None
    # probability of generation
    probability: Optional[float] = None
    # Keywords ID
    id: Optional[str] = None


class Pipeline(ABC):
    @abstractmethod
    def process(self, input_data: PipelineInput) -> PipelineOutput:
        """Process input data and return an integer classification."""
        pass

    @abstractmethod
    def batch_process(self, input_data: List[PipelineInput]) -> List[PipelineOutput]:
        """Process input data and return an integer classification for each input in batch."""
        pass

    @abstractmethod
    def describe(self) -> List[str]:
        """Describe pipeline steps - log / return"""
        pass


def parse_response_reason(response: str) -> PipelineOutput:
    """Parse llm output containing a score and a reason."""
    # "Score: 0, Reason: score too low"
    match = re.match(r"Score: *(\d+), *Reason: *(.+)", response)
    if match:
        score = int(match.group(1))  # Extract score as an integer
        reason = match.group(2)  # Extract reason
    else:
        logging.warning(f"Could not parse {response}")
        score = 0
        reason = "too low"
    logging.info(f"Parsed score: {score}, reason: {reason}")
    return PipelineOutput(score=score, reason=reason)


def parse_response(response: str, id: Optional[str] = None) -> PipelineOutput:
    """Parse response containing only a score."""
    match = re.match(r"^[^\d]*(\d+)", response)
    if match:
        score = int(match.group(1))  # Extract score as an integer
    else:
        logging.warning(f"Could not parse {response}")
        score = 0
    logging.info(f"Parsed score: {score}")
    return PipelineOutput(score=score, id=id)


class MisinformationClassification(BaseModel):
    reasoning: str
    misinformation: bool


# Field descriptions are sent to the model as part of the JSON schema, so they
# must be in the language of the prompt or the reasoning may come back in the
# wrong language.
SCHEMA_DESCRIPTIONS = {
    "french": {
        "reasoning": "Brève analyse du texte expliquant s'il promeut ou non de la désinformation climatique, avant de donner la réponse finale.",
        "misinformation": "Le texte promeut-il de la désinformation climatique qui contredit le consensus scientifique établi, telle que définie ci-dessus ?",
    },
    "dutch": {
        "reasoning": "Korte analyse van de tekst die uitlegt of hij klimaatdesinformatie verspreidt of niet, voordat het eindantwoord wordt gegeven.",
        "misinformation": "Verspreidt de tekst klimaatdesinformatie die ingaat tegen de vastgestelde wetenschappelijke consensus, zoals hierboven gedefinieerd?",
    },
    "portuguese": {
        "reasoning": "Breve análise do texto explicando se ele promove ou não desinformação climática, antes de dar a resposta final.",
        "misinformation": "O texto promove desinformação climática que contradiz o consenso científico estabelecido, conforme definida acima?",
    },
    "german": {
        "reasoning": "Kurze Analyse des Textes, die erklärt, ob er Klimadesinformation verbreitet oder nicht, bevor die endgültige Antwort gegeben wird.",
        "misinformation": "Verbreitet der Text Klimadesinformation, die dem etablierten wissenschaftlichen Konsens widerspricht, wie oben definiert?",
    },
    "spanish": {
        "reasoning": "Breve análisis del texto que explica si promueve o no desinformación climática, antes de dar la respuesta final.",
        "misinformation": "¿Promueve el texto desinformación climática que contradice el consenso científico establecido, tal como se define arriba?",
    },
    "polish": {
        "reasoning": "Krótka analiza tekstu wyjaśniająca, czy promuje on dezinformację klimatyczną, przed podaniem ostatecznej odpowiedzi.",
        "misinformation": "Czy tekst promuje dezinformację klimatyczną przeczącą ugruntowanemu konsensusowi naukowemu, zgodnie z powyższą definicją?",
    },
}
DEFAULT_SCHEMA_LANGUAGE = "french"


@lru_cache(maxsize=None)
def build_response_format(language: str = DEFAULT_SCHEMA_LANGUAGE):
    """Mistral response_format for MisinformationClassification, described in `language`."""
    descriptions = SCHEMA_DESCRIPTIONS.get(
        language.lower(), SCHEMA_DESCRIPTIONS[DEFAULT_SCHEMA_LANGUAGE]
    )
    model = create_model(
        "MisinformationClassification",
        reasoning=(str, Field(description=descriptions["reasoning"])),
        misinformation=(bool, Field(description=descriptions["misinformation"])),
    )
    return response_format_from_pydantic_model(model)
MISINFORMATION_FIELD_RE = re.compile(r'"misinformation"\s*:\s*(true|false)', re.IGNORECASE)


def parse_structured_response(
    content: str, id: Optional[str] = None
) -> Tuple[PipelineOutput, bool]:
    """Parse a structured (reasoning + misinformation bool) Mistral response.

    Returns (output, ok) where ok is False if neither strict JSON parsing nor
    the regex fallback could recover a misinformation value (score defaults to 0).
    """
    try:
        result = MisinformationClassification.model_validate_json(content)
        return PipelineOutput(
            score=10 if result.misinformation else 0, reason=result.reasoning, id=id
        ), True
    except Exception:
        # The model can emit invalid JSON (e.g. an unescaped quote inside the
        # free-text `reasoning` field, or a truncated response after a repetition
        # loop got cut off by the `stop` sequence). Fall back to extracting just
        # the `misinformation` boolean, which is short and rarely malformed.
        match = MISINFORMATION_FIELD_RE.search(content)
        if match:
            misinformation = match.group(1).lower() == "true"
            return PipelineOutput(
                score=10 if misinformation else 0, reason=content, id=id
            ), True
        logging.warning(f"Could not parse structured response {content}")
        return PipelineOutput(score=0, reason="", id=id), False


class MistralSinglePromptPipeline(Pipeline):
    def __init__(
        self,
        model_name: str,
        api_key: Optional[str] = None,
        prompt: Optional[DisinformationPrompt] = None,
        prompt_version: Optional[str] = None,
        use_async: bool = False,
        semaphore_limit: int = 5,
        language: str = DEFAULT_SCHEMA_LANGUAGE,
    ) -> None:
        if not prompt:
            if not prompt_version:
                raise ValueError(
                    "Must define either prompt or prompt_version to retrieve prompt from versions."
                )
            prompt = self._get_prompt_from_version(prompt_version)

        self._api_key = api_key if api_key else get_secret_docker("MISTRAL_API_KEY")
        self._model = model_name
        self._response_format = build_response_format(language)
        self.use_async = use_async
        if self.use_async:
            self.semaphore_limit = semaphore_limit

        self._system_prompt = prompt.prompt
        self.prompt_version = prompt.version
        self.binary = prompt.binary
        self.version = f"{model_name}/{prompt.version}"
        self._steps = [
            f"Mistral structured prompt with {self._model} - prompt version: {prompt.version} - prompt text: {self._system_prompt}"
        ]

    def _get_prompt_from_version(self, version_string: str):
        return PROMPTS[version_string]

    @property
    def _client(self) -> Mistral:
        # Lazily created, and excluded from pickling (see __getstate__): the
        # underlying httpx client holds a thread lock that Ray/Modin cannot
        # serialize when this pipeline is captured by a df.apply() closure
        # (see main.py's extract_model_result). Each worker process recreates
        # its own client once, on first use, after unpickling.
        if self.__dict__.get("_client_instance") is None:
            self._client_instance = Mistral(api_key=self._api_key)
        return self._client_instance

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_client_instance", None)
        return state

    def _build_messages(self, transcript: str):
        content = self._system_prompt.format(transcript=transcript)
        return [{"role": "user", "content": content}]

    def _complete(self, messages, temperature: float) -> str:
        chat_response = self._client.chat.complete(
            model=self._model,
            messages=messages,
            response_format=self._response_format,
            temperature=temperature,
            max_tokens=512,
            presence_penalty=0.5,
            frequency_penalty=0.5,
            stop=["\t\t\t"],
        )
        return chat_response.choices[0].message.content

    async def _async_complete(self, messages, temperature: float) -> str:
        chat_response = await self._client.chat.complete_async(
            model=self._model,
            messages=messages,
            response_format=self._response_format,
            temperature=temperature,
            max_tokens=512,
            presence_penalty=0.5,
            frequency_penalty=0.5,
            stop=["\t\t\t"],
        )
        return chat_response.choices[0].message.content

    def process(self, input_data: PipelineInput) -> PipelineOutput:
        messages = self._build_messages(input_data.transcript)
        try:
            content = self._complete(messages, temperature=0.05)
            output, ok = parse_structured_response(content, id=input_data.id)
            if ok:
                return output

            # Likely a degenerate repetition loop cut off by `stop`. A single
            # retry at a higher temperature rarely re-enters the same loop.
            content = self._complete(messages, temperature=0.3)
            output, ok = parse_structured_response(content, id=input_data.id)
            return output
        except Exception as e:
            logging.error(f"Error calling Mistral: {e}")
            raise Exception

    async def _async_process(
        self,
        input_data: PipelineInput,
        semaphore: asyncio.Semaphore = asyncio.Semaphore(1),
    ):
        messages = self._build_messages(input_data.transcript)
        async with semaphore:
            content = await self._async_complete(messages, temperature=0.05)
        output, ok = parse_structured_response(content, id=input_data.id)
        if not ok:
            # Likely a degenerate repetition loop cut off by `stop`. A single
            # retry at a higher temperature rarely re-enters the same loop.
            async with semaphore:
                content = await self._async_complete(messages, temperature=0.3)
            output, ok = parse_structured_response(content, id=input_data.id)
        return input_data.id, output

    async def _async_batch_process(self, input_data: List[PipelineInput]):
        semaphore = asyncio.Semaphore(self.semaphore_limit)
        return await asyncio.gather(
            *[
                self._async_process(
                    PipelineInput(
                        transcript=input.transcript, id=input.id if input.id else idx
                    ),
                    semaphore,
                )
                for idx, input in enumerate(input_data)
            ]
        )

    def batch_process(self, input_data: List[PipelineInput]):
        if self.use_async:
            unordered_responses = asyncio.run(self._async_batch_process(input_data))
            response_dict = {idx: output for idx, output in unordered_responses}
            responses = []
            for idx, input in enumerate(input_data):
                _id = input.id if input.id else idx
                responses.append(response_dict[_id])
        else:
            responses = []
            for data in input_data:
                responses.append(self.process(data))
        return responses

    def describe(self) -> None:
        for step in self._steps:
            logging.info(step)
        return self._steps


class SinglePromptPipeline(Pipeline):
    def __init__(
        self,
        model_name: str,
        api_key: Optional[str] = None,
        prompt: Optional[DisinformationPrompt] = None,
        prompt_version: Optional[str] = None,
        use_async: bool = False,
        semaphore_limit: int = 5,
    ) -> None:
        if not prompt:
            if not prompt_version:
                raise ValueError(
                    "Must define either prompt or prompt_version to retrieve prompt from versions."
                )
            prompt = self._get_prompt_from_version(prompt_version)

        openai_key = api_key if api_key else get_secret_docker("OPENAI_API_KEY")
        openai.api_key = openai_key
        os.environ["OPENAI_API_KEY"] = openai_key
        self._model = model_name
        self.use_async = use_async
        if self.use_async:
            self.semaphore_limit = semaphore_limit

        self._system_prompt = prompt.prompt
        self.prompt_version = prompt.version
        self.binary = prompt.binary
        self.version = f"{model_name}/{prompt.version}"
        self._steps = [
            f"Single Open AI prompt with {self._model} - prompt version: {prompt.version} - prompt text: {self._system_prompt}"
        ]

    def _get_prompt_from_version(self, version_string: str):
        return PROMPTS[version_string]

    async def _async_process(
        self,
        input_data: PipelineInput,
        async_client: openai.AsyncOpenAI,
        semaphore: asyncio.Semaphore = asyncio.Semaphore(1),
    ):
        prompt = self._system_prompt + f" '''{input_data.transcript}'''"
        messages = [{"role": "user", "content": prompt}]
        async with semaphore:
            response = await async_client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=0,
            )
        result = response.choices[0].message.content.strip()
        return input_data.id, parse_response(result, input_data.id)

    def process(self, input_data: PipelineInput) -> int:
        prompt = self._system_prompt + f" '''{input_data.transcript}'''"
        messages = [{"role": "user", "content": prompt}]
        logging.debug(f"Send {messages}")

        try:
            openai_key = get_secret_docker("OPENAI_API_KEY")
            openai.api_key = openai_key
            response = openai.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=0,
            )
            logging.debug(f"Response API: {response}")
            result = response.choices[0].message.content.strip()

            return parse_response(result)
        except Exception as e:
            logging.error(f"Error : {e}")
            raise Exception

    async def _async_batch_process(self, input_data: List[PipelineInput]):
        semaphore = asyncio.Semaphore(self.semaphore_limit)
        async_client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        return await asyncio.gather(
            *[
                self._async_process(
                    PipelineInput(
                        transcript=input.transcript, id=input.id if input.id else idx
                    ),
                    async_client,
                    semaphore,
                )
                for idx, input in enumerate(input_data)
            ]
        )

    def batch_process(self, input_data: List[PipelineInput]):
        if self.use_async:
            unordered_responses = asyncio.run(self._async_batch_process(input_data))
            response_dict = {idx: output for idx, output in unordered_responses}
            responses = []
            for idx, input in enumerate(input_data):
                _id = input.id if input.id else idx
                responses.append(response_dict[_id])
        else:
            responses = []
            for data in input_data:
                responses.append(self.process(data))
        return responses

    def describe(self) -> None:
        for step in self._steps:
            logging.info(step)
        return self._steps


class BertPipeline(Pipeline):
    def __init__(
        self,
        model_name: str,
        tokenizer_name: Optional[str] = None,
        chunk_size: int = 512,
        chunk_overlap: int = 256,
        batch_size: int = 32,
        min_probability: float = 0.5,
        verbose: bool = False,
    ):
        self._model = model_name
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        for parameter in self.model.parameters():
            parameter.requires_grad = False
        if tokenizer_name is None:
            tokenizer_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
        self.version = f"{model_name}/{chunk_size}_{chunk_overlap}"
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.min_probability = min_probability
        self.verbose = verbose

        self.splitter = SentenceSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )

        self._steps = [
            (
                f"BERT Pipeline using model: {self._model} - ",
                f"chunk size: {chunk_size} - prompt text: {chunk_overlap}",
            )
        ]

    def process(self, input_data: PipelineInput) -> int:
        texts = []
        chunks = self.splitter.split_text(input_data.transcript)
        for chunk in chunks:
            texts.append(chunk)
        inputs = self.tokenizer(
            texts,
            padding="max_length",
            truncation=True,
            max_length=self.chunk_size,
            return_tensors="pt",
        )
        logging.info(
            f"Processing text of {len(input_data.transcript)}, split into {len(chunks)} chunks after chunking."
        )
        outputs = self.model(
            input_ids=inputs["input_ids"],
            attention_mask=inputs["attention_mask"],
            seq_len=self.chunk_size,
            batch_size=self.batch_size,
            show_progress=True,
        )
        probability = (nn.functional.softmax(outputs.logits, dim=-1)[:, 1]).max().item()
        prediction = probability >= self.min_probability
        return PipelineOutput(score=prediction, probability=probability)

    def describe(self) -> None:
        for step in self._steps:
            logging.info(step)
        return self._steps

    def batch_process(self, input_data: List[PipelineInput]):
        ids = []
        texts = []
        for idx, data_record in enumerate(input_data):
            chunks = self.splitter.split_text(data_record.transcript)
            for chunk in chunks:
                _id = data_record.id if data_record.id else idx
                ids.append(_id)
                texts.append(chunk)
        inputs = self.tokenizer(
            texts,
            padding="max_length",
            truncation=True,
            max_length=self.chunk_size,
            return_tensors="pt",
        )
        predictions = []
        probabilities = []
        logging.info(
            f"Processing {len(inputs['input_ids'])} texts after chunking,"
            f"with batch size {self.batch_size}: {len(inputs['input_ids']) // self.batch_size + 1} iterations"
        )
        for window in range(len(inputs["input_ids"]) // self.batch_size + 1):
            if self.verbose:
                logging.info(f"Processing Batch number : {window+1}")
            outputs = self.model(
                input_ids=inputs["input_ids"][
                    self.batch_size * window : self.batch_size * (window + 1)
                ],
                attention_mask=inputs["attention_mask"][
                    self.batch_size * window : self.batch_size * (window + 1)
                ],
                # seq_len=self.chunk_size,
                # batch_size=self.batch_size,
                # show_progress=True,
            )
            if self.min_probability:
                _predictions = (
                    (
                        nn.functional.softmax(outputs.logits, dim=-1)[:, 1]
                        > self.min_probability
                    )
                    .int()
                    .tolist()
                )
            else:
                _predictions = outputs.logits.numpy().argmax(1).tolist()
            predictions.extend(_predictions)
            probabilities.extend(
                (nn.functional.softmax(outputs.logits, dim=-1)[:, 1]).tolist()
            )
        results_df = pd.DataFrame(
            {
                "id": ids,
                "prediction": predictions,
                "probability": probabilities,
            }
        )
        results_df = results_df.groupby(["id"]).agg("max").reset_index()
        logging.info(f"Elaborated {len(predictions)} texts")
        logging.info(
            f"Misinformation detection results: {results_df.prediction.value_counts()}"
        )
        logging.info(
            f"Misinformation probabilities results: {results_df.probability.describe()}"
        )
        return [
            PipelineOutput(
                id=row["id"], score=row["prediction"], probability=row["probability"]
            )
            for idx, row in results_df.iterrows()
        ]


def get_pipeline_from_name(name: str):
    mapping = {
        "bert": BertPipeline,
        "simple_prompt": SinglePromptPipeline,
        "mistral_prompt": MistralSinglePromptPipeline,
    }
    if name in mapping:
        return mapping[name]
    else:
        logging.error(
            f"Cannot retrive pipeline {name}. Available pipelines are: {list(mapping.keys())}"
        )
