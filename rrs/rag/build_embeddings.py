"""Build a RAG index for one expert-model subject: parse source PDFs into
chapter/subsection records, chunk them, embed the chunks, and upload the
resulting index to S3.

Combines (ported from https://github.com/dataforgoodfr/climateguard,
expert_models/scripts/):
  - parse_pdf_to_jsonl.py: font-size heuristics turn each PDF into
    per-chapter, per-subsection records (chapters from heading font size,
    subsections from bold body-sized headings).
  - build_rag_index.py: splits those sections into <= --chunk-chars
    snippets (sentence-level, with overlap) and embeds them.

Source PDFs are downloaded from s3://{EMBEDDINGS_BUCKET_NAME}/{S3_PREFIX}/{subject}/raw/,
and the built index (chunks.jsonl, embeddings.npy, meta.json — see
rag_utils.save_index) is uploaded to s3://{EMBEDDINGS_BUCKET_NAME}/{S3_PREFIX}/{subject}/rag/.

Before rebuilding, the raw PDFs' ETags are hashed into a fingerprint and
compared against the "source_fingerprint" stored in the previous build's
meta.json (fetched directly from S3, no download needed) — if nothing
changed, the rebuild is skipped unless --force is passed.

Usage:
    poetry run python -m rrs.rag.build_embeddings environmental_health
    poetry run python -m rrs.rag.build_embeddings environmental_health --embedder mistral --chunk-chars 600
"""

import argparse
import hashlib
import json
import logging
import os
import re
import tempfile
from collections import Counter
from pathlib import Path

import pymupdf as fitz
from dotenv import load_dotenv

from rrs.rag.rag_utils import DEFAULT_EMBEDDER, EMBEDDERS, Embedder, META_FILE, save_index
from rrs.rag.s3_sync import EMBEDDINGS_BUCKET_NAME, download_prefix, get_object_etags, get_s3_client, read_text, upload_folder

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

DEFAULT_SUBJECT_NAME = "environmental_health"
DEFAULT_S3_PREFIX = "expert_models"

TITLE_SIZE_RATIO = 3.5  # heading spans larger than body_size * this are chapter titles
KICKER_SIZE_RATIO = 1.4  # spans larger than body_size * this (but not a title) are kickers
BOLD_FLAG = 1 << 4  # PyMuPDF span flags bit for bold text
SUBSECTION_MIN_RATIO = 0.85  # subsection headings are bold spans within this size band of body_size
SUBSECTION_MAX_RATIO = 1.35

SENTENCE_RE = re.compile(r"(?<=[.!?…»])\s+")


# --- parse_pdf_to_jsonl.py (PDF -> chapter/subsection records) -------------


def get_body_size(doc: fitz.Document) -> float:
    char_counts = Counter()
    for page in doc:
        for block in page.get_text("dict")["blocks"]:
            for line in block.get("lines", []):
                for span in line["spans"]:
                    char_counts[round(span["size"], 1)] += len(span["text"])
    return char_counts.most_common(1)[0][0]


def is_decorative(text: str) -> bool:
    """True for glyph-only fragments (quote-mark icons, bullets, control chars)
    that some PDFs render as oversized spans inline within normal body text -
    these must not be mistaken for headings just because of their font size."""
    return not re.search(r"\w", text)


def line_heading_size(line) -> float:
    """Largest size among a line's non-decorative spans, or 0 if none qualify."""
    sizes = [
        round(span["size"], 1) for span in line["spans"] if not is_decorative(span["text"])
    ]
    return max(sizes, default=0)


def page_headings(page, body_size: float) -> tuple[str, str]:
    """Return (kicker, title) heading text found on a page, in reading order."""
    title_parts, kicker_parts = [], []
    for block in page.get_text("dict")["blocks"]:
        for line in block.get("lines", []):
            line_text = "".join(span["text"] for span in line["spans"]).strip()
            if not line_text:
                continue
            max_size = line_heading_size(line)
            if max_size > body_size * TITLE_SIZE_RATIO:
                title_parts.append(line_text)
            elif max_size > body_size * KICKER_SIZE_RATIO:
                kicker_parts.append(line_text)
    return " ".join(kicker_parts).strip(), " ".join(title_parts).strip()


def is_subsection_heading_block(block, body_size: float) -> bool:
    """A block is a subsection heading iff every non-decorative span in it is
    bold and close to body-text size - distinct from bold running headers
    (smaller than body_size) and from chapter titles/kickers (much larger)."""
    spans = [
        span
        for line in block.get("lines", [])
        for span in line["spans"]
        if not is_decorative(span["text"])
    ]
    if not spans:
        return False
    return all(span["flags"] & BOLD_FLAG for span in spans) and all(
        body_size * SUBSECTION_MIN_RATIO <= round(span["size"], 1) <= body_size * SUBSECTION_MAX_RATIO
        for span in spans
    )


def classify_block(block, body_size: float) -> tuple[str, str]:
    """Classify a block as ('title'|'kicker'|'subsection'|'body'|'empty', text)."""
    lines = []
    max_size = 0
    for line in block.get("lines", []):
        line_text = "".join(span["text"] for span in line["spans"]).strip()
        if line_text:
            lines.append(line_text)
        max_size = max(max_size, line_heading_size(line))
    text = "\n".join(lines).strip()
    if not text:
        return "empty", ""

    if max_size > body_size * TITLE_SIZE_RATIO:
        return "title", " ".join(lines).strip()
    if max_size > body_size * KICKER_SIZE_RATIO:
        return "kicker", " ".join(lines).strip()
    if is_subsection_heading_block(block, body_size):
        return "subsection", " ".join(lines).strip()
    if re.fullmatch(r"\d+", text):
        return "empty", ""  # bare page number
    return "body", text


def slugify(text: str, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug or fallback


def find_chapters(doc, stop_after_keyword: str | None) -> list[tuple[int, int, str, str]]:
    """Return [(start_page, end_page, kicker, title), ...] page ranges (0-indexed,
    end exclusive) for each chapter, in document order."""
    body_size = get_body_size(doc)
    boundaries = []  # (page_index, kicker, title)
    for pno, page in enumerate(doc):
        kicker, title = page_headings(page, body_size)
        # A page with only a kicker-tier heading (no title-tier one) still
        # marks a new chapter - e.g. a part divider with no separate title
        # font of its own. Promote it so its content isn't silently dropped.
        if not title and kicker:
            kicker, title = "", kicker
        if title:
            boundaries.append((pno, kicker, title))

    chapters = []
    for idx, (start, kicker, title) in enumerate(boundaries):
        end = boundaries[idx + 1][0] if idx + 1 < len(boundaries) else len(doc)
        chapters.append((start, end, kicker, title))
        if stop_after_keyword and (
            stop_after_keyword.upper() in title.upper()
            or stop_after_keyword.upper() in kicker.upper()
        ):
            break

    return chapters


def find_subsections(doc, start: int, end: int, body_size: float) -> list[dict]:
    """Split a chapter's page range into subsections at bold, body-sized
    heading blocks. Always returns at least one subsection."""
    subsections = []
    current_title = ""
    current_start = start
    current_paragraphs: list[str] = []

    def flush(stop_page: int):
        text = "\n\n".join(current_paragraphs).strip()
        subsections.append(
            {
                "title": current_title,
                "start_page": current_start + 1,
                "end_page": stop_page,
                "char_count": len(text),
                "text": text,
            }
        )

    for pno in range(start, end):
        for block in doc[pno].get_text("dict")["blocks"]:
            kind, text = classify_block(block, body_size)
            if kind in ("title", "kicker", "empty"):
                continue
            if kind == "subsection":
                if current_title or current_paragraphs:
                    flush(pno)
                current_title = text
                current_start = pno
                current_paragraphs = []
            else:  # body
                current_paragraphs.append(text)

    flush(end)
    return subsections


def parse_pdf(pdf_path: Path, stop_after_keyword: str | None = None) -> list[dict]:
    doc = fitz.open(pdf_path)
    body_size = get_body_size(doc)

    records = []
    for chapter_idx, (start, end, _kicker, chapter_title) in enumerate(
        find_chapters(doc, stop_after_keyword)
    ):
        subsections = find_subsections(doc, start, end, body_size)
        for sub_idx, sub in enumerate(subsections):
            records.append(
                {
                    "id": f"{chapter_idx:02d}_{sub_idx:02d}_"
                    f"{slugify(sub['title'] or chapter_title, f'section_{chapter_idx}_{sub_idx}')}",
                    "chapter_title": chapter_title,
                    "subsection_title": sub["title"],
                    "char_count": sub["char_count"],
                    "start_page": sub["start_page"],
                    "end_page": sub["end_page"],
                    "text": sub["text"],
                }
            )

    return records


# --- build_rag_index.py (sections -> chunks -> embeddings) -----------------


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


# --- orchestration -----------------------------------------------------


def _fingerprint(etags: dict[str, str]) -> str:
    """Stable hash of a {key: etag} mapping, used to detect whether the raw
    source documents have changed since the last build."""
    canonical = json.dumps(dict(sorted(etags.items())), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_embeddings(
    subject_name: str,
    s3_prefix: str = DEFAULT_S3_PREFIX,
    embedder_name: str = DEFAULT_EMBEDDER,
    chunk_chars: int = 600,
    min_chunk_chars: int = 150,
    overlap_chars: int = 100,
    batch_size: int = 32,
    stop_after_keyword: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> Path:
    s3 = get_s3_client()
    raw_prefix = f"{s3_prefix}/{subject_name}/raw/"
    rag_prefix = f"{s3_prefix}/{subject_name}/rag"

    raw_etags = get_object_etags(s3, EMBEDDINGS_BUCKET_NAME, raw_prefix)
    pdf_etags = {k: v for k, v in raw_etags.items() if k.lower().endswith(".pdf")}
    if not pdf_etags:
        raise SystemExit(f"No PDF files found at s3://{EMBEDDINGS_BUCKET_NAME}/{raw_prefix}")
    source_fingerprint = _fingerprint(pdf_etags)

    if not force:
        previous_meta_raw = read_text(s3, EMBEDDINGS_BUCKET_NAME, f"{rag_prefix}/{META_FILE}")
        if previous_meta_raw is not None:
            previous_fingerprint = json.loads(previous_meta_raw).get("source_fingerprint")
            if previous_fingerprint == source_fingerprint:
                logging.info(
                    f"Source documents at s3://{EMBEDDINGS_BUCKET_NAME}/{raw_prefix} unchanged since the last "
                    f"build — skipping. Pass --force to rebuild anyway."
                )
                return rag_prefix

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        raw_dir = tmp_path / "raw"
        out_dir = tmp_path / "rag"

        logging.info(f"Downloading source PDFs from s3://{EMBEDDINGS_BUCKET_NAME}/{raw_prefix}...")
        pdf_paths = [
            p for p in download_prefix(s3, EMBEDDINGS_BUCKET_NAME, raw_prefix, raw_dir) if p.suffix.lower() == ".pdf"
        ]
        if not pdf_paths:
            raise SystemExit(f"No PDF files found at s3://{EMBEDDINGS_BUCKET_NAME}/{raw_prefix}")
        logging.info(f"  {len(pdf_paths)} PDF(s) downloaded.")

        chunks: list[dict] = []
        for pdf_path in sorted(pdf_paths):
            sections = parse_pdf(pdf_path, stop_after_keyword=stop_after_keyword)
            if limit:
                sections = sections[:limit]
            for section in sections:
                pieces = chunk_section(section["text"], chunk_chars, min_chunk_chars, overlap_chars)
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
            logging.info(f"{pdf_path.name}: {len(sections)} section(s)")

        if not chunks:
            raise SystemExit("No chunks produced from the source PDFs.")

        lengths = sorted(len(c["text"]) for c in chunks)
        assert lengths[-1] <= chunk_chars, f"chunk of {lengths[-1]} chars exceeds cap {chunk_chars}"
        logging.info(
            f"{len(chunks)} chunks | chars min/median/max = {lengths[0]}/{lengths[len(lengths) // 2]}/{lengths[-1]} "
            f"(cap {chunk_chars})"
        )

        embedder = Embedder(embedder_name)
        # Prefix chapter/subsection titles for retrieval context; the stored text stays the raw chunk.
        texts = [
            " - ".join(t for t in (c["chapter_title"], c["subsection_title"]) if t) + "\n" + c["text"]
            for c in chunks
        ]
        embeddings = embedder.embed_passages(texts, batch_size=batch_size)
        logging.info(f"Embeddings: {embeddings.shape} ({embedder.model_id})")

        meta = {
            "embedder": embedder_name,
            "embedding_model": embedder.model_id,
            "dim": int(embeddings.shape[1]),
            "n_chunks": len(chunks),
            "chunk_chars": chunk_chars,
            "min_chunk_chars": min_chunk_chars,
            "overlap_chars": overlap_chars,
            "sources": [p.name for p in pdf_paths],
            "source_fingerprint": source_fingerprint,
        }
        save_index(out_dir, chunks, embeddings, meta)

        logging.info(f"Uploading index to s3://{EMBEDDINGS_BUCKET_NAME}/{rag_prefix}/...")
        upload_folder(s3, out_dir, EMBEDDINGS_BUCKET_NAME, rag_prefix)
        logging.info("  Done.")

    return rag_prefix


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "subject",
        nargs="?",
        default=os.getenv("SUBJECT", DEFAULT_SUBJECT_NAME),
        help=f"Expert model subject, e.g. 'environmental_health' (default: {DEFAULT_SUBJECT_NAME}, env: SUBJECT)",
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.getenv("S3_PREFIX", DEFAULT_S3_PREFIX),
        help=f"S3 key prefix holding <subject>/raw and <subject>/rag (default: {DEFAULT_S3_PREFIX}, env: S3_PREFIX)",
    )
    parser.add_argument("--embedder", choices=list(EMBEDDERS), default=os.getenv("EMBEDDER", DEFAULT_EMBEDDER))
    parser.add_argument("--chunk-chars", type=int, default=int(os.getenv("CHUNK_CHARS", 600)), help="Hard cap on chunk length in characters")
    parser.add_argument("--min-chunk-chars", type=int, default=int(os.getenv("MIN_CHUNK_CHARS", 150)), help="Smaller trailing chunks are merged into the previous one if they fit")
    parser.add_argument("--overlap-chars", type=int, default=int(os.getenv("OVERLAP_CHARS", 100)), help="Sentence-level overlap between consecutive chunks")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", 32)))
    parser.add_argument("--stop-after-keyword", default=os.getenv("STOP_AFTER_KEYWORD") or None, help="Drop all section boundaries after the first one whose title/kicker contains this text")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N sections per PDF")
    parser.add_argument(
        "--force",
        action="store_true",
        default=os.getenv("FORCE_REBUILD", "").lower() in ("1", "true", "yes"),
        help="Rebuild even if the source PDFs haven't changed since the last build (env: FORCE_REBUILD)",
    )
    args = parser.parse_args()

    build_embeddings(
        subject_name=args.subject,
        s3_prefix=args.s3_prefix,
        embedder_name=args.embedder,
        chunk_chars=args.chunk_chars,
        min_chunk_chars=args.min_chunk_chars,
        overlap_chars=args.overlap_chars,
        force=args.force,
        batch_size=args.batch_size,
        stop_after_keyword=args.stop_after_keyword,
        limit=args.limit,
    )
