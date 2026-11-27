"""Shared helpers for the RAG pipeline: embedders, index I/O and cosine search.

Used by build_rag_index.py (passages) and run_rag_agent.py (queries). All
embedders return L2-normalised float32 arrays, so cosine similarity is a plain
dot product.
"""

import json
import os
from pathlib import Path

import numpy as np
import torch

# name -> (model id, pooling, query prefix, passage prefix)
EMBEDDERS = {
    "e5": ("intfloat/multilingual-e5-small", "mean", "query: ", "passage: "),
    "bge-m3": ("BAAI/bge-m3", "cls", "", ""),
    "mistral": ("mistral-embed", None, "", ""),
}
DEFAULT_EMBEDDER = "e5"

CHUNKS_FILE = "chunks.jsonl"
EMBEDDINGS_FILE = "embeddings.npy"
META_FILE = "meta.json"


def pick_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def _normalize(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    return (x / np.clip(norms, 1e-12, None)).astype(np.float32)


class Embedder:
    def __init__(self, name: str = DEFAULT_EMBEDDER):
        if name not in EMBEDDERS:
            raise ValueError(f"Unknown embedder {name!r}; choose from {list(EMBEDDERS)}")
        self.name = name
        self.model_id, self.pooling, self.query_prefix, self.passage_prefix = EMBEDDERS[name]
        self._model = None
        self._tokenizer = None
        self._client = None
        self._device = None

    def _load(self) -> None:
        if self.name == "mistral":
            if self._client is None:
                from mistralai.client import Mistral

                api_key = os.getenv("MISTRAL_API_KEY")
                if not api_key:
                    raise SystemExit("MISTRAL_API_KEY not set (needed for --embedder mistral).")
                self._client = Mistral(api_key=api_key)
            return
        if self._model is None:
            from transformers import AutoModel, AutoTokenizer

            self._device = pick_device()
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_id)
            self._model = AutoModel.from_pretrained(self.model_id).to(self._device).eval()

    def _embed(self, texts: list[str], prefix: str, batch_size: int) -> np.ndarray:
        self._load()
        texts = [prefix + t for t in texts]
        out: list[np.ndarray] = []
        for start in range(0, len(texts), batch_size):
            batch = texts[start : start + batch_size]
            if self.name == "mistral":
                resp = self._client.embeddings.create(model=self.model_id, inputs=batch)
                out.append(np.array([d.embedding for d in resp.data], dtype=np.float32))
                continue
            enc = self._tokenizer(batch, padding=True, truncation=True, max_length=512, return_tensors="pt").to(
                self._device
            )
            with torch.no_grad():
                hidden = self._model(**enc).last_hidden_state
            if self.pooling == "cls":
                pooled = hidden[:, 0]
            else:
                mask = enc["attention_mask"].unsqueeze(-1).to(hidden.dtype)
                pooled = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
            out.append(pooled.float().cpu().numpy())
        return _normalize(np.concatenate(out)) if out else np.zeros((0, 0), dtype=np.float32)

    def embed_passages(self, texts: list[str], batch_size: int = 32) -> np.ndarray:
        return self._embed(texts, self.passage_prefix, batch_size)

    def embed_queries(self, texts: list[str], batch_size: int = 32) -> np.ndarray:
        return self._embed(texts, self.query_prefix, batch_size)


def save_index(out_dir: Path, chunks: list[dict], embeddings: np.ndarray, meta: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / CHUNKS_FILE, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
    np.save(out_dir / EMBEDDINGS_FILE, embeddings)
    (out_dir / META_FILE).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def load_index(index_dir: Path) -> tuple[np.ndarray, list[dict], dict]:
    for name in (CHUNKS_FILE, EMBEDDINGS_FILE, META_FILE):
        if not (index_dir / name).exists():
            raise SystemExit(f"Missing {index_dir / name}. Run build_rag_index.py first.")
    embeddings = np.load(index_dir / EMBEDDINGS_FILE)
    with open(index_dir / CHUNKS_FILE, encoding="utf-8") as f:
        chunks = [json.loads(line) for line in f if line.strip()]
    meta = json.loads((index_dir / META_FILE).read_text(encoding="utf-8"))
    if len(chunks) != len(embeddings):
        raise SystemExit(f"Index mismatch: {len(chunks)} chunks vs {len(embeddings)} embeddings in {index_dir}")
    return embeddings, chunks, meta


def search(query_emb: np.ndarray, embeddings: np.ndarray, top_k: int) -> list[tuple[int, float]]:
    """Return [(row index, cosine score)] for the top_k best rows."""
    scores = embeddings @ query_emb
    top_k = min(top_k, len(scores))
    idx = np.argpartition(-scores, top_k - 1)[:top_k]
    idx = idx[np.argsort(-scores[idx])]
    return [(int(i), float(scores[i])) for i in idx]
