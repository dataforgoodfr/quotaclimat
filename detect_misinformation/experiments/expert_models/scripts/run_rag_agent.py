"""Fact-check TV/radio transcripts with a NON fine-tuned model acting as a RAG agent.

Same I/O as run_inference.py (input CSV with id, channel_name, datetime,
plaintext, url -> XLSX with extra columns), but instead of a fine-tuned
checkpoint the base chat model (default Qwen3) gets a `search_reference` tool
that retrieves snippets from the index built by build_rag_index.py.

Per row the agent loops: reason (Qwen3 thinking mode) about which claims touch
the theme, whether retrieval is needed at all and which query to send -> call
the tool -> reflect on the evidence (sufficient? refine the query?) -> ...
-> final answer. The answer must start with [FLAGGED] or [NOT_FLAGGED]
(binary), followed by the debunk of each problematic phrase. Flagged =
falsehood / manipulation of the truth about the theme (environmental health),
or discourse of inaction ("il est trop tard", "on n'y peut rien").

Output columns: input columns + verdict (FLAGGED | NOT_FLAGGED | UNPARSEABLE),
debunk, queries (tool queries issued), evaluations (PERTINENT/NON_PERTINENT per search),
evidence (grounded | ungrounded | no_search), sources (chunk_ids retrieved).

Usage:
    python run_rag_agent.py biodiversity [--model-name Qwen/Qwen3-8B]
        [--quant 4bit|none] [--index-dir PATH] [--limit N] [--trace FILE]
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import torch
from dotenv import load_dotenv
from tqdm import tqdm

SCRIPTS_DIR = Path(__file__).resolve().parent
EXPERT_MODELS_DIR = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from rag_utils import Embedder, load_index, search  # noqa: E402
from run_inference import REQUIRED_COLUMNS, RELEVANCE_DEFINITIONS, strip_tag  # noqa: E402
from train_lora import TOPIC_DESCRIPTIONS, stop_token_ids  # noqa: E402

VERDICT_RE = re.compile(r"\[?\s*(NOT_FLAGGED|FLAGGED)\s*\]?", re.IGNORECASE)
THINK_RE = re.compile(r"<think>(.*?)</think>", re.DOTALL)
TOOL_CALL_RE = re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.DOTALL)

TOOL_NAME = "search_reference"
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": TOOL_NAME,
            "description": (
                "Recherche dans les documents de référence (ouvrages d'experts sur la santé environnementale) "
                "les extraits les plus pertinents pour vérifier une affirmation précise. "
                "Renvoie quelques extraits numérotés [S1], [S2]... avec leur source."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Requête en français, courte et précise, portant sur UNE affirmation à vérifier "
                            "(ex. : « effets de l'acétamipride sur les abeilles »)."
                        ),
                    }
                },
                "required": ["query"],
            },
        },
    }
]

SYSTEM_PROMPT_TEMPLATE = """\
Tu es un assistant de fact-checking développé par Data For Good et Quotaclimat. \
Tu analyses des extraits de transcription de télévision ou de radio au regard de la thématique suivante :

{definition}

Les transcriptions sont automatiques et bruitées : pas de ponctuation fiable, \
interlocuteurs qui se coupent la parole, mots mal reconnus, passages sans rapport avec le sujet. \
Concentre-toi sur le sens et ignore le bruit.

Ta mission est de classer l'extrait de façon BINAIRE :
- [FLAGGED] si l'extrait contient au moins un passage relevant de la thématique qui est : \
(a) une affirmation fausse ou inexacte ; \
(b) une manipulation de la vérité (information tronquée ou sélective, cadrage trompeur, fausse équivalence, \
exagération ou minimisation, doute injustifié sur un consensus scientifique) ; \
(c) un discours d'inaction ou de fatalisme, c'est-à-dire qui affirme qu'agir est inutile ou impossible \
(« il est trop tard », « on ne peut plus rien faire », « ça ne sert à rien », « les efforts sont inutiles ») ; \
une simple difficulté ou un constat réaliste sur l'ampleur du problème n'en est pas un.
- [NOT_FLAGGED] dans tous les autres cas : propos exacts, propos sans rapport avec la thématique, \
ou simples opinions qui ne déforment pas les faits.

Tu disposes de l'outil `search_reference` qui interroge des documents de référence fiables. Méthode :
1. Réfléchis d'abord : quels passages touchent à la thématique ? Une vérification documentaire est-elle \
vraiment nécessaire ? Si l'extrait est hors sujet ou manifestement sans problème, n'appelle pas l'outil.
2. L'outil ne sert qu'à vérifier les affirmations factuelles (catégories a et b). Un discours d'inaction \
(catégorie c) se repère à sa formulation seule : signale-le directement, sans le chercher dans les documents. \
Si l'extrait ne contient que du discours d'inaction, n'appelle pas l'outil. \
Toute affirmation factuelle liée à la thématique (effets d'un produit ou d'une substance, chiffres, \
lois, consensus scientifique, déclarations d'experts, y compris celles qui te semblent évidemment vraies \
ou fausses) DOIT être vérifiée avec l'outil : ne te fie pas à ta seule mémoire et ne la qualifie pas \
d'« opinion » pour éviter la recherche. Formule une requête précise (une par affirmation), en français, \
puis appelle l'outil.
3. Après chaque recherche, on te demandera d'évaluer explicitement si les extraits renvoyés sont pertinents \
pour l'affirmation vérifiée (réponse [PERTINENT] ou [NON_PERTINENT]). Les extraits peuvent être hors sujet : \
ne les utilise que s'ils répondent vraiment à l'affirmation. S'ils ne sont pas pertinents, tu pourras \
reformuler ta requête (autres mots-clés, autre angle) et relancer la recherche.
4. Quand tu as assez d'éléments, ou si la recherche reste vaine, donne ta réponse finale.

Format de la réponse finale (en français) : commence EXACTEMENT par la balise « [FLAGGED] » ou « [NOT_FLAGGED] » \
(rien avant). Si [FLAGGED], liste ensuite une puce par passage problématique : \
« phrase citée de la transcription » — catégorie (fausseté / manipulation / inaction) — \
explication concise et factuelle qui corrige ou remet en contexte, en citant les sources [S#] utilisées. \
Pour un discours d'inaction, la catégorie est « inaction » et l'explication reste brève ; \
ne cite de source [S#] que si une recherche en a réellement fourni une pertinente. \
Si [NOT_FLAGGED], justifie en une phrase. N'invente aucun fait qui ne figure pas dans les sources \
retournées ou qui n'est pas solidement établi.\
"""

USER_TEMPLATE = "Transcription à analyser :\n\n{text}"
NUDGE_FINAL = (
    "Tu as maintenant assez d'éléments. Donne ta réponse finale sans appeler d'outil, "
    "en commençant exactement par [FLAGGED] ou [NOT_FLAGGED]."
)
NO_RESULT = "Aucun extrait pertinent trouvé."

# Explicit evaluation step run after every tool result (tools disabled for this turn).
EVAL_PROMPT = (
    "Évalue maintenant les extraits renvoyés par l'outil : répondent-ils réellement à l'affirmation "
    "que tu cherchais à vérifier ? Réponds en commençant EXACTEMENT par [PERTINENT] ou [NON_PERTINENT], "
    "puis justifie en une ou deux phrases (indique les [S#] utiles le cas échéant)."
)
RELEVANCE_EVAL_RE = re.compile(r"\[?\s*(NON_PERTINENT|PERTINENT)\s*\]?", re.IGNORECASE)
CONTINUE_AFTER_RELEVANT = (
    "Poursuis : vérifie une autre affirmation avec l'outil si nécessaire, sinon donne ta réponse finale."
)
RETRY_AFTER_IRRELEVANT = (
    "Les extraits ne sont pas pertinents. Reformule ta requête (autres mots-clés, autre angle, terme plus "
    "général ou plus précis) et appelle de nouveau `search_reference`. Si tu estimes qu'aucune source "
    "ne peut répondre, passe directement à ta réponse finale."
)
FINAL_AFTER_IRRELEVANT = (
    "Les extraits ne sont pas pertinents et tu ne peux plus faire de recherche. Donne ta réponse finale sans "
    "citer ces extraits, en commençant exactement par [FLAGGED] ou [NOT_FLAGGED] ; "
    "base-toi uniquement sur des faits solidement établis et reste prudent."
)


def build_system_prompt(model: str) -> str:
    definition = RELEVANCE_DEFINITIONS.get(model) or TOPIC_DESCRIPTIONS.get(model, model.replace("_", " "))
    return SYSTEM_PROMPT_TEMPLATE.format(definition=definition)


def evidence_status(queries: list[str], evaluations: list[str]) -> str:
    """grounded = at least one search was judged relevant; ungrounded = searched but nothing relevant
    (the verdict then rests on the model's own knowledge); no_search = retrieval was not used."""
    if not queries:
        return "no_search"
    return "grounded" if "PERTINENT" in evaluations else "ungrounded"


def extract_verdict(text: str) -> str:
    match = VERDICT_RE.match(text.strip())
    return match.group(1).upper() if match else "UNPARSEABLE"


def load_model_and_tokenizer(model_name: str, quant: str):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    token = os.getenv("HF_TOKEN")
    model_kwargs: dict[str, Any] = {"token": token}
    if quant == "4bit":
        from transformers import BitsAndBytesConfig

        if not torch.cuda.is_available():
            raise SystemExit("--quant 4bit requires a CUDA GPU. Use --quant none to run on CPU/MPS.")
        model_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16,
            bnb_4bit_use_double_quant=True,
        )
        model_kwargs["device_map"] = "auto"
    elif torch.cuda.is_available():
        model_kwargs["torch_dtype"] = torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
        model_kwargs["device_map"] = "auto"
    else:
        model_kwargs["torch_dtype"] = torch.float32

    print(f"Loading {model_name} ...")
    model = AutoModelForCausalLM.from_pretrained(model_name, **model_kwargs)
    tokenizer = AutoTokenizer.from_pretrained(model_name, token=token)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    if not torch.cuda.is_available():
        # generate() hits a fatal Metal backend assertion on Apple Silicon (see run_inference.py): stay on CPU.
        model = model.to("cpu")
    model.eval()
    return model, tokenizer


class Retriever:
    def __init__(self, index_dir: Path, top_k: int, max_chars: int, embedder_name: str | None):
        self.embeddings, self.chunks, self.meta = load_index(index_dir)
        self.embedder = Embedder(embedder_name or self.meta["embedder"])
        if self.embedder.name != self.meta["embedder"]:
            raise SystemExit(f"Index was built with {self.meta['embedder']!r}, not {self.embedder.name!r}.")
        self.top_k = top_k
        self.max_chars = max_chars

    def __call__(self, query: str, seen: dict[str, int]) -> tuple[str, list[str]]:
        """Run a query. `seen` maps chunk_id -> source number for this row, so [S#] labels stay unique."""
        q = self.embedder.embed_queries([query])[0]
        parts, ids, used = [], [], 0
        for idx, _score in search(q, self.embeddings, self.top_k):
            chunk = self.chunks[idx]
            label = seen.setdefault(chunk["chunk_id"], len(seen) + 1)
            title = " - ".join(t for t in (chunk["chapter_title"], chunk["subsection_title"]) if t)
            part = f"[S{label}] ({title}, p. {chunk['start_page']}-{chunk['end_page']})\n{chunk['text']}"
            if parts and used + len(part) > self.max_chars:
                break
            parts.append(part[: self.max_chars])
            used += len(part)
            ids.append(chunk["chunk_id"])
        return ("\n\n".join(parts) if parts else NO_RESULT), ids


def generate(model, tokenizer, prompt: str, args: argparse.Namespace, max_new_tokens: int | None = None) -> str:
    device = next(model.parameters()).device
    inputs = tokenizer(prompt, return_tensors="pt").to(device)
    with torch.no_grad():
        out = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens or args.max_new_tokens,
            do_sample=True,
            temperature=0.6,
            top_p=0.95,
            top_k=20,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=stop_token_ids(tokenizer),
        )
    return tokenizer.decode(out[0][inputs["input_ids"].shape[1] :], skip_special_tokens=True).strip()


def parse_tool_calls(text: str) -> list[dict]:
    calls = []
    for raw in TOOL_CALL_RE.findall(text):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        arguments = data.get("arguments", {})
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {}
        if data.get("name") == TOOL_NAME and isinstance(arguments, dict) and arguments.get("query"):
            calls.append({"name": TOOL_NAME, "arguments": {"query": str(arguments["query"])}})
    return calls


def run_agent(model, tokenizer, retriever: Retriever, system_prompt: str, text: str, args: argparse.Namespace) -> dict:
    ids = tokenizer(text, add_special_tokens=False)["input_ids"]
    if len(ids) > args.max_transcript_tokens:
        text = tokenizer.decode(ids[: args.max_transcript_tokens])

    messages: list[dict] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": USER_TEMPLATE.format(text=text)},
    ]
    seen: dict[str, int] = {}
    queries: list[str] = []
    source_ids: list[str] = []
    trace: list[dict] = []
    evaluations: list[str] = []
    rounds = 0
    nudged = False
    final = ""

    while True:
        # Tools stay available until the round or context budget is exhausted or a final answer was requested.
        tools = TOOLS if rounds < args.max_tool_rounds and not nudged else None
        prompt = tokenizer.apply_chat_template(
            messages,
            tools=tools,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=not args.no_thinking,
        )
        if tools and len(tokenizer(prompt)["input_ids"]) + args.max_new_tokens > args.max_length:
            nudged = True
            messages.append({"role": "user", "content": NUDGE_FINAL})
            continue

        raw = generate(model, tokenizer, prompt, args)
        think = "\n".join(m.strip() for m in THINK_RE.findall(raw))
        content = THINK_RE.sub("", raw).replace("</think>", "").strip()
        calls = parse_tool_calls(content) if tools else []
        trace.append({"think": think, "content": content, "tool_calls": calls})

        if calls:
            rounds += 1
            assistant: dict[str, Any] = {
                "role": "assistant",
                "content": TOOL_CALL_RE.sub("", content).strip(),
                "tool_calls": [{"type": "function", "function": c} for c in calls],
            }
            if think:
                assistant["reasoning_content"] = think
            messages.append(assistant)
            for call in calls:
                query = call["arguments"]["query"]
                result, chunk_ids = retriever(query, seen)
                queries.append(query)
                source_ids.extend(i for i in chunk_ids if i not in source_ids)
                messages.append({"role": "tool", "content": result})
                trace.append({"query": query, "result": result})

            # Explicit evaluation step: is the retrieved evidence relevant? If not, loop back with a new query.
            messages.append({"role": "user", "content": EVAL_PROMPT})
            eval_prompt = tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
            )
            if len(tokenizer(eval_prompt)["input_ids"]) + args.max_eval_tokens > args.max_length:
                messages.pop()
                nudged = True
                messages.append({"role": "user", "content": NUDGE_FINAL})
                continue
            eval_text = generate(model, tokenizer, eval_prompt, args, max_new_tokens=args.max_eval_tokens)
            eval_text = THINK_RE.sub("", eval_text).replace("</think>", "").strip()
            match = RELEVANCE_EVAL_RE.match(eval_text)
            # Unparseable evaluation fails open as relevant, so a formatting slip doesn't discard evidence.
            relevant = not match or match.group(1).upper() == "PERTINENT"
            evaluations.append("PERTINENT" if relevant else "NON_PERTINENT")
            trace.append({"evaluation": eval_text, "relevant": relevant})
            messages.append({"role": "assistant", "content": eval_text})
            if relevant:
                follow_up = CONTINUE_AFTER_RELEVANT
            elif rounds < args.max_tool_rounds:
                follow_up = RETRY_AFTER_IRRELEVANT
            else:
                follow_up = FINAL_AFTER_IRRELEVANT
                nudged = True
            messages.append({"role": "user", "content": follow_up})
            continue

        final = TOOL_CALL_RE.sub("", content).strip()
        if extract_verdict(final) != "UNPARSEABLE" or nudged:
            break
        # Final answer without the required tag: ask once more for the proper format.
        nudged = True
        messages.append({"role": "assistant", "content": final})
        messages.append({"role": "user", "content": NUDGE_FINAL})

    return {"final": final, "queries": queries, "evaluations": evaluations, "sources": source_ids, "trace": trace}


def main(args: argparse.Namespace) -> None:
    if not args.input.exists():
        raise SystemExit(f"Input CSV not found: {args.input}")
    df = pd.read_csv(args.input, dtype=str, keep_default_na=False)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"Input CSV is missing required columns: {missing}")
    if args.limit:
        df = df.head(args.limit)
    print(f"Loaded {len(df)} rows from {args.input}")

    system_prompt = args.system_prompt or build_system_prompt(args.model)
    print(f"\nSystem prompt:\n{system_prompt}\n")

    retriever = Retriever(args.index_dir, args.top_k, args.max_tool_chars, args.embedder)
    print(f"Index: {len(retriever.chunks)} chunks, embedder {retriever.meta['embedding_model']}")
    model, tokenizer = load_model_and_tokenizer(args.model_name, args.quant)
    torch.manual_seed(args.seed)

    verdicts, debunks, queries, evals, evidence, sources = [], [], [], [], [], []
    trace_file = open(args.trace, "w", encoding="utf-8") if args.trace else None
    try:
        for row_id, text in tqdm(zip(df["id"], df["plaintext"]), total=len(df), desc="agent"):
            try:
                result = run_agent(model, tokenizer, retriever, system_prompt, text, args)
            except RuntimeError as exc:
                print(f"[warn] Agent failed for row {row_id}: {exc!r}")
                result = {"final": "", "queries": [], "evaluations": [], "sources": [], "trace": []}
            verdicts.append(extract_verdict(result["final"]))
            debunks.append(strip_tag(result["final"], VERDICT_RE))
            queries.append(" | ".join(result["queries"]))
            evals.append(" | ".join(result["evaluations"]))
            evidence.append(evidence_status(result["queries"], result["evaluations"]))
            sources.append(", ".join(result["sources"]))
            if trace_file:
                trace_file.write(json.dumps({"id": row_id, **result}, ensure_ascii=False) + "\n")
                trace_file.flush()
    finally:
        if trace_file:
            trace_file.close()

    df["verdict"] = verdicts
    df["debunk"] = debunks
    df["queries"] = queries
    df["evaluations"] = evals
    df["evidence"] = evidence
    df["sources"] = sources
    print(f"\nVerdict counts: {df['verdict'].value_counts().to_dict()}")
    print(f"Evidence: {df['evidence'].value_counts().to_dict()}")
    print(f"Rows with at least one search: {(df['queries'] != '').sum()}/{len(df)}")

    if args.only_flagged:
        df = df[df["verdict"] == "FLAGGED"]
        print(f"Keeping only FLAGGED rows: {len(df)}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(args.output, index=False, engine="openpyxl")
    print(f"Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("model", help="Expert model topic, e.g. 'biodiversity' (default paths and theme definition)")
    parser.add_argument("--model-name", type=str, default="Qwen/Qwen3-8B", help="Base (non fine-tuned) chat model with tool-calling support")
    parser.add_argument("--quant", choices=["4bit", "none"], default="4bit", help="4bit needs CUDA; none works on CPU/MPS")
    parser.add_argument("--index-dir", type=Path, default=None, help="Default: <model>/data/rag/")
    parser.add_argument("--embedder", default=None, help="Sanity check only; the embedder is read from the index's meta.json")
    parser.add_argument("--top-k", type=int, default=3, help="Snippets returned per tool call")
    parser.add_argument("--max-tool-chars", type=int, default=2400, help="Hard cap on a tool result's length")
    parser.add_argument("--max-tool-rounds", type=int, default=3, help="Max tool-calling rounds before forcing the final answer")
    parser.add_argument("--max-length", type=int, default=8192, help="Prompt token budget; tools are disabled when the context would exceed it")
    parser.add_argument("--max-transcript-tokens", type=int, default=3000, help="Truncate longer transcripts")
    parser.add_argument("--max-new-tokens", type=int, default=1500, help="Per generation step (includes reasoning)")
    parser.add_argument("--max-eval-tokens", type=int, default=250, help="Max tokens for the post-retrieval relevance evaluation step")
    parser.add_argument("--no-thinking", action="store_true", help="Disable Qwen3 thinking mode (no reflection step)")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--system-prompt", type=str, default=None, help="Override the French system prompt")
    parser.add_argument("--input", type=Path, default=None, help="Default: <model>/data/inference/input.csv")
    parser.add_argument("--output", type=Path, default=None, help="Default: <model>/data/inference/output_rag.xlsx")
    parser.add_argument("--trace", type=Path, default=None, help="Write the full agent turns (reasoning, tool calls, results) to this JSONL")
    parser.add_argument(
        "--only-flagged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write only FLAGGED rows to the output (--no-only-flagged keeps every row; the --trace file always has all rows)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    parser.add_argument("--env-file", type=str, default=".env")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    load_dotenv(env_path if env_path.exists() else None)

    model_dir = EXPERT_MODELS_DIR / args.model
    args.index_dir = args.index_dir or model_dir / "data" / "rag"
    args.input = args.input or model_dir / "data" / "inference" / "input.csv"
    args.output = args.output or model_dir / "data" / "inference" / "output_rag.xlsx"
    main(args)
