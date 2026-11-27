"""Parse a PDF book into per-chapter, per-subsection JSONL records using
font-based heuristics.

Shared across all expert models (climateguard/expert_models/<model>/), each of
which stores its source PDF(s) in data/raw and its parsed output in data/parsed.

Chapters are detected from heading font sizes rather than a table of contents,
since these source PDFs typically have no usable outline/bookmarks. The
largest font-size cluster on a page marks the start of a new chapter; the
next-largest cluster on that page (if present) is treated as a kicker/eyebrow
label (e.g. a "PREMIÈRE FAUSSE INFORMATION" tag above a chapter title).

Within each chapter, subsections are detected from a *different* signal: a
paragraph block set entirely in bold text at roughly body-text size (neither
the running-header size nor a chapter-heading size) is treated as a
subsection title, and the following body text as its content, until the next
such block or the end of the chapter. Blank lines ("\n\n") are not a reliable
signal on their own - ordinary paragraph breaks look identical to them - so
they are not used to detect subsections. Chapters whose PDF has no such bold,
body-sized heading convention simply yield a single untitled subsection
spanning the whole chapter.

Usage:
    python parse_pdf_to_jsonl.py <model> [--pdf PATH] [--out PATH] [--stop-after-keyword TEXT]

<model> is an expert model directory name, e.g. "biodiversity"
(climateguard/expert_models/biodiversity). Defaults to the single PDF found in
that model's data/raw, writing to data/parsed/<pdf_name>.jsonl.

--stop-after-keyword: if given, chapters are only emitted up to and including
the first one whose title/kicker contains this text (case-insensitive); later
chapters (e.g. a duplicated table of contents or publisher back matter) are
dropped. Omit it for documents with no such back matter to discard.
"""

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import fitz  # PyMuPDF

EXPERT_MODELS_DIR = Path(__file__).resolve().parents[1]

TITLE_SIZE_RATIO = 3.5  # heading spans larger than body_size * this are chapter titles
KICKER_SIZE_RATIO = 1.4  # spans larger than body_size * this (but not a title) are kickers
BOLD_FLAG = 1 << 4  # PyMuPDF span flags bit for bold text
SUBSECTION_MIN_RATIO = 0.85  # subsection headings are bold spans within this size band of body_size
SUBSECTION_MAX_RATIO = 1.35


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model", help="Expert model directory name, e.g. 'biodiversity'")
    parser.add_argument("--pdf", type=Path, help="Input PDF path (default: single PDF in data/raw)")
    parser.add_argument("--out", type=Path, help="Output JSONL path (default: data/parsed/<pdf_name>.jsonl)")
    parser.add_argument(
        "--stop-after-keyword",
        help="Drop all section boundaries after the first one whose title/kicker contains this text",
    )
    args = parser.parse_args()

    model_dir = EXPERT_MODELS_DIR / args.model
    raw_dir = model_dir / "data" / "raw"
    parsed_dir = model_dir / "data" / "parsed"

    if args.pdf:
        pdf_path = args.pdf
    else:
        pdfs = sorted(raw_dir.glob("*.pdf"))
        if not pdfs:
            raise SystemExit(f"No PDF files found in {raw_dir}")
        pdf_path = pdfs[0]

    if args.out:
        out_path = args.out
    else:
        parsed_dir.mkdir(parents=True, exist_ok=True)
        out_path = parsed_dir / f"{pdf_path.stem}.jsonl"

    sections = parse_pdf(pdf_path, stop_after_keyword=args.stop_after_keyword)

    with out_path.open("w", encoding="utf-8") as f:
        for section in sections:
            f.write(json.dumps(section, ensure_ascii=False) + "\n")

    print(f"Wrote {len(sections)} sections to {out_path}")


if __name__ == "__main__":
    main()
