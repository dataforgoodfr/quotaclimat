"""
Regroupement de chunks audio par similarité cosinus sur embeddings
====================================================================
Version embedding de e04_group_chunks.py.
Remplace la comparaison O(n²) sur constellation maps par une
multiplication matricielle sur embeddings normalisés.
"""

import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import List

import numpy as np

from .tools.common_objects import EmbeddingChunk, EmbeddingFingerprint
from .tools.fingerprint.pairs import make_params_hash

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingChunkGroup:
    """Duck-type compatible with ChunkGroup for reuse in e05."""

    count: int
    duration_mean: float
    duration_std: float
    occurrences: list[EmbeddingChunk]

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            count=data["count"],
            duration_mean=data["duration_mean"],
            duration_std=data["duration_std"],
            occurrences=[EmbeddingChunk.from_dict(occ) for occ in data["occurrences"]],
        )


def _cluster_embeddings(
    chunks: list[EmbeddingChunk],
    similarity_threshold: float,
    duration_tol: float,
) -> dict[int, list[int]]:
    """
    Group similar chunks via embedding cosine similarity + Union-Find.

    Much faster than constellation map comparison:
    cosine similarity matrix is computed in one matrix multiply.
    """
    n = len(chunks)
    if n == 0:
        return {}

    # Build embedding matrix (n, 2048)
    emb_matrix = np.array(
        [c.fingerprint.embedding for c in chunks], dtype=np.float32
    )
    norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True) + 1e-8
    emb_normed = emb_matrix / norms

    # Cosine similarity matrix (n, n) — single matmul
    cos_sim = emb_normed @ emb_normed.T

    # Duration vector for pre-filter
    durations = np.array([c.fingerprint.duration_sec for c in chunks])

    # Union-Find
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    matches = 0
    for i in range(n):
        for j in range(i + 1, n):
            if abs(durations[i] - durations[j]) > duration_tol:
                continue
            if cos_sim[i, j] >= similarity_threshold:
                union(i, j)
                matches += 1

    logger.debug(f"Embedding clustering: {matches} similar pairs found among {n} chunks")

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    return dict(groups)


class EmbeddingChunkGrouping:
    def __init__(
        self,
        similarity_threshold: float = 0.92,
        duration_tol: float = 1.0,
    ):
        self.similarity_threshold = similarity_threshold
        self.duration_tol = duration_tol

    def params(self) -> dict:
        return {
            "strategy": "embedding",
            "similarity_threshold": self.similarity_threshold,
            "duration_tol": self.duration_tol,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())

    def run(self, source: list[EmbeddingChunk]) -> list[EmbeddingChunkGroup]:
        chunks = [c for c in source if c.fingerprint.duration_sec >= 0.5]
        logger.debug(f"{len(chunks)} embedding chunks to group")

        groups = _cluster_embeddings(
            chunks, self.similarity_threshold, self.duration_tol
        )

        report_groups: list[EmbeddingChunkGroup] = []
        for member_idxs in groups.values():
            members = sorted(
                [chunks[i] for i in member_idxs], key=lambda c: c.start_sec
            )
            durations = [c.fingerprint.duration_sec for c in members]

            report_groups.append(
                EmbeddingChunkGroup(
                    count=len(members),
                    duration_mean=round(float(np.mean(durations)), 2),
                    duration_std=round(float(np.std(durations)), 2),
                    occurrences=members,
                )
            )

        report_groups.sort(key=lambda g: -g.count)
        return report_groups


def embedding_canonical(chunks: list[EmbeddingChunk]) -> EmbeddingChunk:
    """
    Build a canonical EmbeddingChunk from multiple occurrences.

    Uses element-wise mean of embeddings and median of acoustic features.
    """
    if len(chunks) == 1:
        return chunks[0]

    durations = [c.fingerprint.duration_sec for c in chunks]
    energies = [c.fingerprint.energy_mean for c in chunks]
    centroids = [c.fingerprint.spectral_centroid for c in chunks]
    zcrs = [c.fingerprint.zcr_mean for c in chunks]
    embeddings = np.array(
        [c.fingerprint.embedding for c in chunks], dtype=np.float32
    )

    mean_embedding = embeddings.mean(axis=0)

    # Collect semantic labels from all occurrences (most common ones)
    label_counts: dict[str, int] = {}
    for c in chunks:
        for label in c.fingerprint.semantic_labels or []:
            label_counts[label] = label_counts.get(label, 0) + 1
    top_labels = sorted(label_counts, key=label_counts.get, reverse=True)[:3]

    richest = max(chunks, key=lambda c: c.fingerprint.duration_sec)

    return EmbeddingChunk(
        start_sec=richest.start_sec,
        end_sec=richest.end_sec,
        channel=richest.channel,
        fingerprint=EmbeddingFingerprint(
            duration_sec=float(np.median(durations)),
            energy_mean=float(np.median(energies)),
            spectral_centroid=float(np.median(centroids)),
            zcr_mean=float(np.median(zcrs)),
            embedding=mean_embedding.tolist(),
            semantic_labels=top_labels if top_labels else None,
        ),
    )
