import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad

from .tools.common_objects import Chunk, Fragment
from .tools.fingerprint.hash import are_chunks_similar

logger = logging.getLogger(__name__)

CURSOR_BATCH_SIZE = 1000


@dataclass
class AdChunkMatch:
    ad: Ad
    chunk_entry_index: int  # index in ad.chunks list (the entry matching params_hash)
    chunk_index: int  # index within that entry's "chunks" list


async def run_chunk_identification(
    chunks: list[Chunk],
    params_hash: str,
    min_matching_hashes: int = 1,
    similarity_threshold: float = 0.05,
) -> tuple[list[Fragment], list[Chunk]]:
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
        for ads in session.scalars(
            select(Ad)
            .filter(Ad.first_detection_date < datetime.now() - timedelta(days=1))
            .execution_options(yield_per=CURSOR_BATCH_SIZE)
        ).partitions():
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
    known_fragments: list[Fragment] = []
    unknown_chunks: list[Chunk] = []

    # Chunks should be sorted by start_time
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

        for chunk_match in _sort_chunk_matches_by_importance(chunk_matches):
            if chunk_match.chunk_index > 0 and not chunk_is_start_of_segment:
                # we matched a chunk that is in the middle of a fragment, we cannot use it
                continue

            # Now we check if all following chunks in the same db fragment also match the next chunks in the local list
            db_ad = chunk_match.ad
            db_entry = db_ad.chunks[chunk_match.chunk_entry_index]
            db_chunks = [Chunk.from_dict(c) for c in db_entry.get("chunks", [])]

            match, next_index = _current_fragment_is_a_match(
                chunk_match, db_chunks, chunks, local_idx, matches
            )

            if match:
                # We consider this fragment as already known, we add it to the known fragments list and skip all its chunks in the local list
                if chunk_match.chunk_index == 0:
                    start_sec = chunk.start_sec
                    end_sec = chunk.start_sec + db_ad.duration_sec
                else:
                    # We compute end_sec from the last chunk that match
                    end_sec = chunks[
                        local_idx + len(db_chunks) - chunk_match.chunk_index - 1
                    ].end_sec
                    start_sec = end_sec - db_ad.duration_sec
                known_fragments.append(
                    Fragment(
                        start_sec=start_sec,
                        end_sec=end_sec,
                        channel=chunk.channel,
                        classification="already_known_ad",
                        group_id=db_ad.id,
                        chunks=db_chunks,
                    )
                )
                local_idx = (
                    next_index  # skip all chunks of this fragment in the local list
                )
                break  # stop checking other matches for this chunk, we already found a matching fragment
        else:
            # No match was good enough to consider the fragment as already known, we consider this chunk as unknown
            unknown_chunks.append(chunk)
            local_idx += 1

    logger.info(
        f"Chunks identification: {len(known_fragments)} fragments known, {len(unknown_chunks)} chunks unknown "
        f"(out of {len(chunks)} total)"
    )

    return known_fragments, unknown_chunks


def _current_fragment_is_a_match(
    chunk_match: AdChunkMatch,
    db_ad_chunks: list[Chunk],
    local_chunks: list[Chunk],
    current_local_idx: int,
    all_chunk_matches: dict[int, list[AdChunkMatch]],
) -> tuple[bool, int | None]:
    number_of_chunks_to_match = len(db_ad_chunks) - chunk_match.chunk_index
    for offset in range(1, number_of_chunks_to_match):
        next_local_idx = current_local_idx + offset
        if next_local_idx >= len(local_chunks):
            # We reached the end of the local chunks, we consider it a match (the local fragment is shorter than the DB one)
            return True, next_local_idx
        prev_chunk = local_chunks[next_local_idx - 1]
        next_chunk = local_chunks[next_local_idx]

        if next_chunk.start_sec - prev_chunk.end_sec > 1.0:
            # Too much time between the chunks, we consider it a match (the local fragment has a gap, but is still similar to the DB one)
            return True, next_local_idx

        next_chunk_matches = all_chunk_matches.get(next_local_idx, [])
        if not any(
            m.ad.id == chunk_match.ad.id
            and m.chunk_entry_index == chunk_match.chunk_entry_index
            and m.chunk_index == chunk_match.chunk_index + offset
            for m in next_chunk_matches
        ):
            # We did not find a match for the next chunk in the DB fragment, we consider the whole fragment as not matching (to avoid partial matches)
            return False, None
    # All following chunks in the DB fragment also match the next local chunks, we consider it a match
    return True, current_local_idx + number_of_chunks_to_match


def _sort_chunk_matches_by_importance(
    matches: list[AdChunkMatch],
) -> list[AdChunkMatch]:
    # We sort the matches to try the most important ones first (to maximize the chances to match a whole fragment and not a part of it)
    # The importance is first given from the type, then to the duration of the ad (longer is more interesting than shorter),
    # then by chunk_index ascending so chunk_index=0 is tried first (ensures full fragment match before partial)
    return sorted(
        (m for m in matches if m.ad.fragment_type == "advertising"),
        key=lambda m: (m.chunk_index, -m.ad.duration_sec),
    ) + sorted(
        (m for m in matches if m.ad.fragment_type != "advertising"),
        key=lambda m: (m.chunk_index, -m.ad.duration_sec),
    )
