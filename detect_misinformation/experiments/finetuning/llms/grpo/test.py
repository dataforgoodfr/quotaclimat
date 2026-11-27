import argparse
import json
import re

import pandas as pd
from datasets import load_dataset
from mlx_lm import generate, load
from mlx_lm.sample_utils import make_logits_processors, make_sampler
from prompts import prompt_chat
from sklearn.metrics import classification_report
from tqdm import tqdm


def remove_thinking(text, thinking_token):
    if thinking_token:
        # Remove thinking section
        start_thought = text.find(f"<{thinking_token}>")
        end_thought = text.find(f"</{thinking_token}>")
        if end_thought > start_thought:
            text = text[end_thought + len(f"</{thinking_token}>") :]
    return text


def parse_xml(text, thinking_token=None):
    pattern = r"misinformation>(.*?)</misinformation>"
    text = remove_thinking(text, thinking_token)
    match = re.search(pattern, text, re.DOTALL)
    if match:
        misinformation = match.group(1)
        return {"misinformation": misinformation.strip()}
    print("No XML match found")
    print(text)
    return {"misinformation": False}


def parse_json(text, thinking_token=None):
    pattern = r"\{.*?\}"  # Matches JSON-like objects

    text = remove_thinking(text, thinking_token)

    match = re.search(pattern, text, re.DOTALL)
    if match:
        json_str = match.group(0)
        try:
            json_obj = json.loads(json_str)  # Properly parse the JSON
        except json.JSONDecodeError:
            json_str = json_str.removeprefix("{").removesuffix("}")
            json_obj = {}
            segments = json_str.split(":")
            json_obj[segments[0].strip().replace('"', "").replace("'", "")] = (
                segments[1].strip().replace('"', "").replace("'", "")
            )
        finally:
            if list(json_obj.keys())[0] == "misinformation":
                return json_obj
            else:
                print("Cannot parse JSON")
                return {"misinformation": False}
    print("No JSON match found")


def parse_bool(response):
    if isinstance(response, bool) or isinstance(response, int):
        return int(response)
    elif isinstance(response, str):
        return int(response.lower() == "true")
    print("Cannot parse bool")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dataset", type=str, default="DataForGood/climateguard")
    parser.add_argument("-s", "--data-split", type=str, default="test")
    parser.add_argument(
        "-m", "--model", type=str, default="mlx-community/Qwen3-0.6B-bf16"
    )
    parser.add_argument("-a", "--adapters", type=str, default="adapters")
    parser.add_argument("-t", "--temp", type=float, default=0.1)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--repetition-penalty", type=float, default=0)
    parser.add_argument("--thinking-token", type=str, default=None)
    parser.add_argument(
        "--return-type", type=str, choices=["json", "xml"], default="json"
    )
    args = parser.parse_args()
    print("Found following args")
    print(str(args))

    parse_function = {
        "json": parse_json,
        "xml": parse_xml,
    }

    load_args = dict(path_or_hf_repo=args.model)
    if args.adapters != "None":
        load_args.update(dict(adapter_path=args.adapters))
    model_lora, tokenizer_lora = load(**load_args)

    dataset = load_dataset(args.dataset, split=args.data_split)

    labels = []
    predictions = []
    for record in tqdm(dataset):
        messages = [
            {"role": "user", "content": prompt_chat + f"```{record['plaintext']}```"}
        ]

        prompt = tokenizer_lora.apply_chat_template(
            messages,
            add_generation_prompt=True,
            enable_thinking=args.thinking_token is not None,
        )
        sampler = make_sampler(temp=args.temp)
        logits_processors = make_logits_processors(
            repetition_penalty=args.repetition_penalty
        )
        response = generate(
            model_lora,
            tokenizer_lora,
            prompt=prompt,
            max_tokens=args.max_tokens,
            verbose=False,
            sampler=sampler,
            # logits_processors=logits_processors,
        )
        prediction = parse_function[args.return_type](
            response, thinking_token=args.thinking_token
        )
        predictions.append(
            parse_bool(prediction["misinformation"]) if prediction else 0
        )
        labels.append(int(record["misinformation"]))

    print(classification_report(labels, predictions))
    print(pd.Series(predictions).value_counts())
