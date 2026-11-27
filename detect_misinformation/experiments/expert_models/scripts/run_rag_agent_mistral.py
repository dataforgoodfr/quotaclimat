"""Same RAG fact-checking agent as run_rag_agent.py, but the LLM is served by the Mistral API.

Identical workflow, prompts (French), index, I/O and output columns as
run_rag_agent.py (see its docstring); only the generation backend differs:
`mistral-small-2603` (default) with native function calling instead of a local
Qwen3. Needs MISTRAL_API_KEY. The query embedder is read from the index's
meta.json (build the index with `--embedder mistral` if you want mistral-embed
for retrieval too).

Differences due to the API backend:
- No thinking-mode tags: the system prompt's reflection instructions are
  followed in the message `content`, which is logged to the trace as the
  reflection. Optionally pass --reasoning-effort for native reasoning.
- The post-retrieval relevance evaluation request is appended to the last tool
  message (the API rejects a user turn right after a tool turn), and run with
  tool_choice="none".
- Rows are processed concurrently (--concurrency, default 10); local query embedding is serialised.
- No token budget (the API handles the context); rounds are limited by
  --max-tool-rounds.

Usage:
    python run_rag_agent_mistral.py biodiversity [--model-name mistral-small-2603]
        [--limit N] [--trace FILE]
"""

import argparse
import asyncio
import json
import os
import sys
import threading
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

SCRIPTS_DIR = Path(__file__).resolve().parent
EXPERT_MODELS_DIR = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from run_rag_agent import (  # noqa: E402
    CONTINUE_AFTER_RELEVANT,
    EVAL_PROMPT,
    FINAL_AFTER_IRRELEVANT,
    NUDGE_FINAL,
    RELEVANCE_EVAL_RE,
    REQUIRED_COLUMNS,
    RETRY_AFTER_IRRELEVANT,
    TOOL_NAME,
    TOOLS,
    USER_TEMPLATE,
    VERDICT_RE,
    Retriever,
    build_system_prompt,
    evidence_status,
    extract_verdict,
    strip_tag,
)

DEFAULT_MODEL = "mistral-small-2603"

REFLECTION_HINT = (
    "\n\nAvant chaque appel d'outil, écris dans ta réponse une courte réflexion (2-3 phrases) : "
    "quelles affirmations touchent à la thématique, une vérification est-elle nécessaire, "
    "et pourquoi cette requête précise."
)


def flatten_content(content: Any) -> tuple[str, str]:
    """Mistral message content is a string or a list of chunks (text / thinking). Returns (thinking, text)."""
    if content is None:
        return "", ""
    if isinstance(content, str):
        return "", content.strip()
    think, text = [], []
    for chunk in content:
        kind = getattr(chunk, "type", None)
        if kind == "thinking":
            for part in getattr(chunk, "thinking", None) or []:
                if getattr(part, "text", None):
                    think.append(part.text)
        elif getattr(chunk, "text", None):
            text.append(chunk.text)
    return "\n".join(think).strip(), "".join(text).strip()


async def chat(
    client, args: argparse.Namespace, messages: list[dict], tools: list[dict] | None, max_tokens: int, tool_choice: str = "auto"
):
    kwargs: dict[str, Any] = {
        "model": args.model_name,
        "messages": messages,
        "temperature": args.temperature,
        "max_tokens": max_tokens,
        "random_seed": args.seed,
    }
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = tool_choice
    if args.reasoning_effort:
        kwargs["reasoning_effort"] = args.reasoning_effort
    delay = 5
    for attempt in range(6):
        try:
            return (await client.chat.complete_async(**kwargs)).choices[0].message
        except Exception as exc:  # noqa: BLE001 - retry rate limits / transient server errors only
            status = getattr(exc, "status_code", None)
            if attempt == 5 or status not in (429, 500, 502, 503, 504):
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


def locked_search(retriever: Retriever, lock: threading.Lock, query: str, seen: dict[str, int]):
    """Embedding the query is local, CPU/GPU-bound work: run it in a thread, one query at a time."""
    with lock:
        return retriever(query, seen)


def parse_tool_calls(message) -> list[dict]:
    calls = []
    for tc in message.tool_calls or []:
        arguments = tc.function.arguments
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {}
        if tc.function.name == TOOL_NAME and isinstance(arguments, dict) and arguments.get("query"):
            calls.append({"id": tc.id, "query": str(arguments["query"])})
    return calls


async def run_agent(
    client, retriever: Retriever, retrieval_lock: threading.Lock, system_prompt: str, text: str, args: argparse.Namespace
) -> dict:
    messages: list[dict] = [
        {"role": "system", "content": system_prompt + REFLECTION_HINT},
        {"role": "user", "content": USER_TEMPLATE.format(text=text[: args.max_transcript_chars])},
    ]
    seen: dict[str, int] = {}
    queries: list[str] = []
    evaluations: list[str] = []
    source_ids: list[str] = []
    trace: list[dict] = []
    rounds = 0
    nudged = False
    final = ""

    while True:
        tools = TOOLS if rounds < args.max_tool_rounds and not nudged else None
        message = await chat(client, args, messages, tools, args.max_new_tokens)
        think, content = flatten_content(message.content)
        calls = parse_tool_calls(message) if tools else []
        trace.append({"think": think, "content": content, "tool_calls": [c["query"] for c in calls]})

        if calls:
            rounds += 1
            messages.append(
                {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": [
                        {
                            "id": c["id"],
                            "type": "function",
                            "function": {"name": TOOL_NAME, "arguments": json.dumps({"query": c["query"]}, ensure_ascii=False)},
                        }
                        for c in calls
                    ],
                }
            )
            for call in calls:
                result, chunk_ids = await asyncio.to_thread(locked_search, retriever, retrieval_lock, call["query"], seen)
                queries.append(call["query"])
                source_ids.extend(i for i in chunk_ids if i not in source_ids)
                messages.append({"role": "tool", "name": TOOL_NAME, "tool_call_id": call["id"], "content": result})
                trace.append({"query": call["query"], "result": result})

            # Explicit evaluation step: is the retrieved evidence relevant? If not, loop back with a new query.
            messages[-1]["content"] += "\n\n---\n" + EVAL_PROMPT
            # tool_choice="none" so the evaluation can't turn into another search
            eval_msg = await chat(client, args, messages, TOOLS, args.max_eval_tokens, tool_choice="none")
            _, eval_text = flatten_content(eval_msg.content)
            match = RELEVANCE_EVAL_RE.match(eval_text)
            # Unparseable evaluation fails open as relevant, so a formatting slip doesn't discard evidence.
            relevant = not match or match.group(1).upper() == "PERTINENT"
            evaluations.append("PERTINENT" if relevant else "NON_PERTINENT")
            trace.append({"evaluation": eval_text, "relevant": relevant})
            messages.append({"role": "assistant", "content": eval_text or "[PERTINENT]"})
            if relevant:
                follow_up = CONTINUE_AFTER_RELEVANT
            elif rounds < args.max_tool_rounds:
                follow_up = RETRY_AFTER_IRRELEVANT
            else:
                follow_up = FINAL_AFTER_IRRELEVANT
                nudged = True
            messages.append({"role": "user", "content": follow_up})
            continue

        final = content
        if extract_verdict(final) != "UNPARSEABLE" or nudged:
            break
        # Final answer without the required tag: ask once more for the proper format.
        nudged = True
        messages.append({"role": "assistant", "content": final or "…"})
        messages.append({"role": "user", "content": NUDGE_FINAL})

    return {"final": final, "queries": queries, "evaluations": evaluations, "sources": source_ids, "trace": trace}


async def main(args: argparse.Namespace) -> None:
    from mistralai.client import Mistral

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

    retriever = Retriever(args.index_dir, args.top_k, args.max_tool_chars, None)
    print(f"Index: {len(retriever.chunks)} chunks, embedder {retriever.meta['embedding_model']}")
    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])

    retrieval_lock = threading.Lock()
    semaphore = asyncio.Semaphore(args.concurrency)
    results: list[dict] = [{}] * len(df)
    progress = tqdm(total=len(df), desc="agent")
    trace_file = open(args.trace, "w", encoding="utf-8") if args.trace else None

    async def process(i: int, row_id: str, text: str) -> None:
        async with semaphore:
            try:
                result = await run_agent(client, retriever, retrieval_lock, system_prompt, text, args)
            except Exception as exc:  # noqa: BLE001 - keep going, mark the row UNPARSEABLE
                print(f"[warn] Agent failed for row {row_id}: {exc!r}")
                result = {"final": "", "queries": [], "evaluations": [], "sources": [], "trace": []}
        results[i] = result
        if trace_file:
            trace_file.write(json.dumps({"id": row_id, **result}, ensure_ascii=False) + "\n")
            trace_file.flush()
        progress.update(1)

    try:
        await asyncio.gather(*(process(i, rid, txt) for i, (rid, txt) in enumerate(zip(df["id"], df["plaintext"]))))
    finally:
        progress.close()
        if trace_file:
            trace_file.close()

    verdicts = [extract_verdict(r["final"]) for r in results]
    debunks = [strip_tag(r["final"], VERDICT_RE) for r in results]
    queries = [" | ".join(r["queries"]) for r in results]
    evals = [" | ".join(r["evaluations"]) for r in results]
    evidence = [evidence_status(r["queries"], r["evaluations"]) for r in results]
    sources = [", ".join(r["sources"]) for r in results]

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
    parser.add_argument("--model-name", type=str, default=DEFAULT_MODEL, help="Mistral API model id")
    parser.add_argument("--index-dir", type=Path, default=None, help="Default: <model>/data/rag/")
    parser.add_argument("--top-k", type=int, default=3, help="Snippets returned per tool call")
    parser.add_argument("--max-tool-chars", type=int, default=2400, help="Hard cap on a tool result's length")
    parser.add_argument("--max-tool-rounds", type=int, default=3, help="Max tool-calling rounds before forcing the final answer")
    parser.add_argument("--max-transcript-chars", type=int, default=12000, help="Truncate longer transcripts")
    parser.add_argument("--max-new-tokens", type=int, default=1500, help="Per generation step")
    parser.add_argument("--max-eval-tokens", type=int, default=250, help="Max tokens for the post-retrieval relevance evaluation step")
    parser.add_argument("--concurrency", type=int, default=10, help="Max rows processed concurrently (API calls in flight)")
    parser.add_argument("--temperature", type=float, default=0.3)
    parser.add_argument("--reasoning-effort", choices=["none", "minimal", "low", "medium", "high", "xhigh"], default=None, help="Native reasoning effort, if the model supports it (unset = API default)")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--system-prompt", type=str, default=None, help="Override the French system prompt")
    parser.add_argument("--input", type=Path, default=None, help="Default: <model>/data/inference/input.csv")
    parser.add_argument("--output", type=Path, default=None, help="Default: <model>/data/inference/output_rag_mistral.xlsx")
    parser.add_argument("--trace", type=Path, default=None, help="Write the full agent turns to this JSONL")
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
    if not os.getenv("MISTRAL_API_KEY"):
        raise SystemExit("Error: MISTRAL_API_KEY not set. Add it to your .env file or environment.")

    model_dir = EXPERT_MODELS_DIR / args.model
    args.index_dir = args.index_dir or model_dir / "data" / "rag"
    args.input = args.input or model_dir / "data" / "inference" / "input.csv"
    args.output = args.output or model_dir / "data" / "inference" / "output_rag_mistral.xlsx"
    asyncio.run(main(args))
