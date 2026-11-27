"""Split parsed reference sections into small snippets and embed them for RAG.

Reads the `<topic>/data/parsed/*.jsonl` sections produced by
parse_pdf_to_jsonl.py, cuts each section into chunks of at most
--chunk-chars characters (hard cap, so retrieved passages stay small enough
for a local model's context), embeds them, and writes to
`<topic>/data/rag/` (or --output-dir):

    chunks.jsonl     one chunk per line (chunk_id, source_id, chapter_title,
                     subsection_title, start_page, end_page, text)
    embeddings.npy   float32, L2-normalised, row i <-> line i of chunks.jsonl
    meta.json        embedder + chunking parameters (read by run_rag_agent.py)

Usage:
    python build_rag_index.py biodiversity [--embedder e5|bge-m3|mistral]
        [--chunk-chars 600] [--overlap-chars 100] [--limit N]
"""

import argparse
import json
import re
import sys
from pathlib import Path

from dotenv import load_dotenv

SCRIPTS_DIR = Path(__file__).resolve().parent
EXPERT_MODELS_DIR = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from rag_utils import DEFAULT_EMBEDDER, EMBEDDERS, Embedder, save_index  # noqa: E402

SENTENCE_RE = re.compile(r"(?<=[.!?…»])\s+")


def clean_paragraphs(text: str) -> list[str]:
    """Undo PDF line wrapping: de-hyphenate, join single newlines, keep blank-line paragraphs."""
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    paragraphs = []
    for para in re.split(r"\n\s*\n", text):
        para = re.sub(r"\s+", " ", para).strip()
        if para:
            paragraphs.append(para)
    return paragraphs


def split_units(paragraph: str, cap: int) -> list[str]:
    """Sentences, with any sentence longer than `cap` split on words."""
    units = []
    for sentence in SENTENCE_RE.split(paragraph):
        sentence = sentence.strip()
        if not sentence:
            continue
        while len(sentence) > cap:
            cut = sentence.rfind(" ", 0, cap)
            if cut <= 0:
                cut = cap
            units.append(sentence[:cut].strip())
            sentence = sentence[cut:].strip()
        if sentence:
            units.append(sentence)
    return units


def chunk_section(text: str, cap: int, min_chars: int, overlap: int) -> list[str]:
    units = [u for p in clean_paragraphs(text) for u in split_units(p, cap)]

    # Greedy packing without overlap.
    groups: list[list[str]] = []
    current: list[str] = []
    size = 0
    for unit in units:
        add = len(unit) + (1 if current else 0)
        if current and size + add > cap:
            groups.append(current)
            current, size = [], 0
            add = len(unit)
        current.append(unit)
        size += add
    if current:
        groups.append(current)

    # Merge a too-small trailing group into the previous one when it fits.
    if len(groups) > 1 and len(" ".join(groups[-1])) < min_chars:
        merged = groups[-2] + groups[-1]
        if len(" ".join(merged)) <= cap:
            groups[-2:] = [merged]

    # Render, prepending trailing sentences of the previous group as overlap when it fits under the cap.
    chunks = []
    for i, group in enumerate(groups):
        body = " ".join(group)
        if i > 0 and overlap > 0:
            tail: list[str] = []
            tail_len = 0
            for unit in reversed(groups[i - 1]):
                if tail_len + len(unit) + 1 > overlap:
                    break
                tail.insert(0, unit)
                tail_len += len(unit) + 1
            if tail and len(" ".join(tail)) + 1 + len(body) <= cap:
                body = " ".join(tail) + " " + body
        chunks.append(body)
    return chunks


def main(args: argparse.Namespace) -> None:
    parsed_dir = EXPERT_MODELS_DIR / args.model / "data" / "parsed"
    inputs = args.input or sorted(parsed_dir.glob("*.jsonl"))
    if not inputs:
        raise SystemExit(f"No parsed JSONL files found in {parsed_dir}")

    chunks: list[dict] = []
    for path in inputs:
        with open(path, encoding="utf-8") as f:
            sections = [json.loads(line) for line in f if line.strip()]
        if args.limit:
            sections = sections[: args.limit]
        for section in sections:
            pieces = chunk_section(section["text"], args.chunk_chars, args.min_chunk_chars, args.overlap_chars)
            for n, piece in enumerate(pieces):
                chunks.append(
                    {
                        "chunk_id": f"{section['id']}#{n}",
                        "source_id": section["id"],
                        "chapter_title": section.get("chapter_title", ""),
                        "subsection_title": section.get("subsection_title", ""),
                        "start_page": section.get("start_page"),
                        "end_page": section.get("end_page"),
                        "text": piece,
                    }
                )
        print(f"{path.name}: {len(sections)} sections")

    lengths = sorted(len(c["text"]) for c in chunks)
    assert lengths[-1] <= args.chunk_chars, f"chunk of {lengths[-1]} chars exceeds cap {args.chunk_chars}"
    print(
        f"{len(chunks)} chunks | chars min/median/max = {lengths[0]}/{lengths[len(lengths) // 2]}/{lengths[-1]} "
        f"(cap {args.chunk_chars})"
    )

    embedder = Embedder(args.embedder)
    # Prefix chapter/subsection titles for retrieval context; the stored text stays the raw chunk.
    texts = [" - ".join(t for t in (c["chapter_title"], c["subsection_title"]) if t) + "\n" + c["text"] for c in chunks]
    embeddings = embedder.embed_passages(texts, batch_size=args.batch_size)
    print(f"Embeddings: {embeddings.shape} ({embedder.model_id})")

    out_dir = args.output_dir or EXPERT_MODELS_DIR / args.model / "data" / "rag"
    meta = {
        "embedder": args.embedder,
        "embedding_model": embedder.model_id,
        "dim": int(embeddings.shape[1]),
        "n_chunks": len(chunks),
        "chunk_chars": args.chunk_chars,
        "min_chunk_chars": args.min_chunk_chars,
        "overlap_chars": args.overlap_chars,
        "sources": [p.name for p in inputs],
    }
    save_index(out_dir, chunks, embeddings, meta)
    print(f"Wrote index to {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("model", help="Expert model topic, e.g. 'biodiversity'")
    parser.add_argument("--input", type=Path, nargs="+", default=None, help="Parsed JSONL file(s) (default: all in <model>/data/parsed/)")
    parser.add_argument("--output-dir", type=Path, default=None, help="Default: <model>/data/rag/")
    parser.add_argument("--embedder", choices=list(EMBEDDERS), default=DEFAULT_EMBEDDER)
    parser.add_argument("--chunk-chars", type=int, default=600, help="Hard cap on chunk length in characters")
    parser.add_argument("--min-chunk-chars", type=int, default=150, help="Smaller trailing chunks are merged into the previous one if they fit")
    parser.add_argument("--overlap-chars", type=int, default=100, help="Sentence-level overlap between consecutive chunks")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N sections per file")
    parser.add_argument("--env-file", type=str, default=".env")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    load_dotenv(env_path if env_path.exists() else None)
    main(args)
