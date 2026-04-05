"""
Identification de chunks déjà connus via similarité cosinus sur embeddings
===========================================================================
Version embedding de e03_already_identified_advertising.py.
Compare les embeddings des chunks locaux avec ceux stockés en DB
au lieu d'utiliser les constellation maps.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
from sqlalchemy import select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad

from .tools.common_objects import EmbeddingChunk, EmbeddingFingerprint, Fragment

logger = logging.getLogger(__name__)

CURSOR_BATCH_SIZE = 1000


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a < 1e-8 or norm_b < 1e-8:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


@dataclass
class AdEmbeddingMatch:
    ad: Ad
    chunk_entry_index: int
    chunk_index: int
    similarity: float


async def run_embedding_chunk_identification(
    chunks: list[EmbeddingChunk],
    params_hash: str,
    similarity_threshold: float = 0.92,
    duration_tol: float = 1.0,
) -> tuple[list[Fragment], list[EmbeddingChunk]]:
    """
    Identify already-known chunks using embedding cosine similarity.

    For each local chunk, search the DB for Ads whose stored embeddings
    match (cosine similarity >= threshold). Much faster than constellation
    map comparison: one dot product per pair.

    Returns:
        (known_fragments, unknown_chunks)
    """
    matches: dict[int, list[AdEmbeddingMatch]] = defaultdict(list)
    total_db_fingerprints = 0

    # Pre-compute local embedding matrix for fast comparison
    local_embeddings = np.array(
        [c.fingerprint.embedding for c in chunks], dtype=np.float32
    )
    local_norms = np.linalg.norm(local_embeddings, axis=1, keepdims=True) + 1e-8
    local_normed = local_embeddings / local_norms
    local_durations = np.array([c.fingerprint.duration_sec for c in chunks])

    with get_db_session() as session:
        for ads in session.scalars(
            select(Ad).execution_options(yield_per=CURSOR_BATCH_SIZE)
        ).partitions():
            for ad in ads:
                for entry_idx, chunk_entry in enumerate(ad.chunks or []):
                    if chunk_entry.get("hash") != params_hash:
                        continue
                    for fp_idx, fp_dict in enumerate(
                        chunk_entry.get("fingerprints", [])
                    ):
                        db_fp = EmbeddingFingerprint.from_dict(fp_dict)
                        total_db_fingerprints += 1

                        if db_fp.embedding is None:
                            continue

                        db_emb = np.array(db_fp.embedding, dtype=np.float32)
                        db_norm = np.linalg.norm(db_emb) + 1e-8
                        db_normed = db_emb / db_norm

                        # Vectorized cosine similarity against all local chunks
                        similarities = local_normed @ db_normed

                        # Duration pre-filter + similarity threshold
                        for local_idx in range(len(chunks)):
                            if (
                                abs(local_durations[local_idx] - db_fp.duration_sec)
                                > duration_tol
                            ):
                                continue
                            if similarities[local_idx] >= similarity_threshold:
                                matches[local_idx].append(
                                    AdEmbeddingMatch(
                                        ad=ad,
                                        chunk_entry_index=entry_idx,
                                        chunk_index=fp_idx,
                                        similarity=float(similarities[local_idx]),
                                    )
                                )

    logger.info(
        f"Loaded {total_db_fingerprints} DB fingerprints (params_hash={params_hash})"
    )

    # Build final lists (same sequential matching logic as e03)
    known_fragments: list[Fragment] = []
    unknown_chunks: list[EmbeddingChunk] = []

    local_idx = 0
    while local_idx < len(chunks):
        chunk = chunks[local_idx]
        chunk_matches = matches.get(local_idx, [])

        if not chunk_matches:
            unknown_chunks.append(chunk)
            local_idx += 1
            continue

        chunk_is_start_of_segment = local_idx == 0 or (
            (chunk.start_sec - chunks[local_idx - 1].end_sec) > 1.0
        )

        # Sort by importance: advertising first, then by duration desc, chunk_index asc
        sorted_matches = _sort_matches(chunk_matches)

        matched = False
        for chunk_match in sorted_matches:
            if chunk_match.chunk_index > 0 and not chunk_is_start_of_segment:
                continue

            db_ad = chunk_match.ad
            db_entry = db_ad.chunks[chunk_match.chunk_entry_index]
            db_fingerprint_count = len(db_entry.get("fingerprints", []))

            is_match, next_index = _current_fragment_is_a_match(
                chunk_match, db_fingerprint_count, chunks, local_idx, matches
            )

            if is_match:
                if chunk_match.chunk_index == 0:
                    start_sec = chunk.start_sec
                    end_sec = chunk.start_sec + db_ad.duration_sec
                else:
                    end_sec = chunks[
                        local_idx + db_fingerprint_count - chunk_match.chunk_index - 1
                    ].end_sec
                    start_sec = end_sec - db_ad.duration_sec

                matched_local_chunks = chunks[local_idx:next_index]
                known_fragments.append(
                    Fragment(
                        start_sec=start_sec,
                        end_sec=end_sec,
                        channel=chunk.channel,
                        classification="already_known_ad",
                        group_id=db_ad.id,
                        chunks=matched_local_chunks,
                    )
                )
                local_idx = next_index
                matched = True
                break

        if not matched:
            unknown_chunks.append(chunk)
            local_idx += 1

    logger.info(
        f"Embedding chunk identification: {len(known_fragments)} fragments known, "
        f"{len(unknown_chunks)} chunks unknown (out of {len(chunks)} total)"
    )

    return known_fragments, unknown_chunks


def _current_fragment_is_a_match(
    chunk_match: AdEmbeddingMatch,
    db_fingerprint_count: int,
    local_chunks: list[EmbeddingChunk],
    current_local_idx: int,
    all_chunk_matches: dict[int, list[AdEmbeddingMatch]],
) -> tuple[bool, int | None]:
    number_of_chunks_to_match = db_fingerprint_count - chunk_match.chunk_index
    for offset in range(1, number_of_chunks_to_match):
        next_local_idx = current_local_idx + offset
        if next_local_idx >= len(local_chunks):
            return True, next_local_idx
        prev_chunk = local_chunks[next_local_idx - 1]
        next_chunk = local_chunks[next_local_idx]

        if next_chunk.start_sec - prev_chunk.end_sec > 1.0:
            return True, next_local_idx

        next_chunk_matches = all_chunk_matches.get(next_local_idx, [])
        if not any(
            m.ad.id == chunk_match.ad.id
            and m.chunk_entry_index == chunk_match.chunk_entry_index
            and m.chunk_index == chunk_match.chunk_index + offset
            for m in next_chunk_matches
        ):
            return False, None

    return True, current_local_idx + number_of_chunks_to_match


def _sort_matches(matches: list[AdEmbeddingMatch]) -> list[AdEmbeddingMatch]:
    return sorted(
        (m for m in matches if m.ad.fragment_type == "advertising"),
        key=lambda m: (m.chunk_index, -m.ad.duration_sec),
    ) + sorted(
        (m for m in matches if m.ad.fragment_type != "advertising"),
        key=lambda m: (m.chunk_index, -m.ad.duration_sec),
    )
