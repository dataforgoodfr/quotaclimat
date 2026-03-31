import logging
from collections import defaultdict
from dataclasses import dataclass, field

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad

from .tools.common_objects import Chunk
from .tools.fingerprint.hash import are_chunks_similar

logger = logging.getLogger(__name__)

CURSOR_BATCH_SIZE = 1000


@dataclass
class AdChunkMatch:
    ad: Ad
    chunk_entry_index: int  # index in ad.chunks list (the entry matching params_hash)
    chunk_index: int  # index within that entry's "chunks" list


@dataclass
class KnownChunk:
    chunk: Chunk
    matching_ads: list[AdChunkMatch] = field(default_factory=list)


async def run_chunk_identification(
    chunks: list[Chunk],
    params_hash: str,
    min_matching_hashes: int = 1,
    similarity_threshold: float = 0.05,
) -> tuple[list[KnownChunk], list[Chunk]]:
    """
    Identifie les chunks déjà connus (présents dans la DB) et les chunks inconnus.

    Pour chaque chunk local, recherche les Ad de la DB dont les chunks correspondent
    (via inverted index sur les hashes de fingerprint). Plusieurs Ad peuvent correspondre
    à un même chunk — toutes sont conservées dans KnownChunk.matching_ads, avec les indices
    exacts du chunk dans l'Ad (chunk_entry_index, chunk_index).

    Le traitement est fait page par page pour limiter la mémoire utilisée.

    Args:
        chunks: Les chunks locaux à identifier.
        params_hash: Hash des paramètres ChunkCreator, pour filtrer les chunks DB compatibles.
        min_matching_hashes: Nombre minimum de hashes communs pour considérer deux chunks similaires.
        similarity_threshold: Score minimum (cohérence temporelle) pour valider une correspondance.

    Returns:
        (known_chunks, unknown_chunks): chunks reconnus (avec leurs Ad correspondantes) et inconnus.
    """
    # matches[local_idx] accumulates AdChunkMatch across all pages
    matches: dict[int, list[AdChunkMatch]] = defaultdict(list)

    # Precompute local hash sets once (reused for every page)
    local_hash_sets: list[set[str]] = [{h for h, _ in (c.hashes or [])} for c in chunks]

    total_db_chunks = 0

    with get_db_session() as session:
        for ads in session.query(Ad).yield_per(CURSOR_BATCH_SIZE):
            # --- Build hash index for this batch only ---
            # hash_str → list of (ad, entry_idx, chunk_idx, db_chunk, db_hash_set)
            hash_index: dict[str, list[tuple[Ad, int, int, Chunk, set]]] = defaultdict(
                list
            )

            for ad in ads:
                for entry_idx, chunk_entry in enumerate(ad.chunks or []):
                    if chunk_entry.get("hash") != params_hash:
                        continue
                    for chunk_idx, chunk_dict in enumerate(
                        chunk_entry.get("chunks", [])
                    ):
                        db_chunk = Chunk.from_dict(chunk_dict)
                        db_hash_set = {h for h, _ in (db_chunk.hashes or [])}
                        for h in db_hash_set:
                            hash_index[h].append(
                                (ad, entry_idx, chunk_idx, db_chunk, db_hash_set)
                            )
                        total_db_chunks += 1

            # --- Match every local chunk against this page ---
            for local_idx, (chunk, local_hash_set) in enumerate(
                zip(chunks, local_hash_sets)
            ):
                # Candidate DB chunks sharing at least one hash
                candidates: dict[int, tuple[Ad, int, int, Chunk, set]] = {}
                for h in local_hash_set:
                    for item in hash_index.get(h, []):
                        candidates[id(item[3])] = item  # key = identity of db_chunk

                for (
                    ad,
                    entry_idx,
                    chunk_idx,
                    db_chunk,
                    db_hash_set,
                ) in candidates.values():
                    if are_chunks_similar(
                        chunk,
                        db_chunk,
                        local_hash_set,
                        db_hash_set,
                        min_matching_hashes,
                        similarity_threshold,
                    ):
                        matches[local_idx].append(
                            AdChunkMatch(
                                ad=ad,
                                chunk_entry_index=entry_idx,
                                chunk_index=chunk_idx,
                            )
                        )

    logger.info(f"Loaded {total_db_chunks} DB chunks (params_hash={params_hash})")

    # --- Build final lists ---
    known_fragments: list[KnownChunk] = []
    unknown_chunks: list[Chunk] = []

    # Chunks should be sorted by start_time
    for local_idx, chunk in enumerate(chunks):
        chunk_matches = matches.get(local_idx, [])
        if chunk_matches:
            known_fragments.append(KnownChunk(chunk=chunk, matching_ads=chunk_matches))
        else:
            unknown_chunks.append(chunk)

    logger.info(
        f"Chunks identification: {len(known_fragments)} fragments known, {len(unknown_chunks)} chunks unknown "
        f"(out of {len(chunks)} total)"
    )

    return known_fragments, unknown_chunks
