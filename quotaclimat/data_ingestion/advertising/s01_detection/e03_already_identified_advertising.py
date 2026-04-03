import logging
from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad

from .tools.common_objects import Chunk, Fingerprint, Fragment
from .tools.fingerprint.hash import are_fingerprints_similar

logger = logging.getLogger(__name__)

CURSOR_BATCH_SIZE = 1000


@dataclass
class AdChunkMatch:
    ad: Ad
    chunk_entry_index: int  # index in ad.chunks list (the entry matching params_hash)
    chunk_index: int  # index within that entry's "fingerprints" list


async def run_chunk_identification(
    chunks: list[Chunk],
    params_hash: str,
    min_matching_hashes: int = 5,
    similarity_threshold: float = 0.05,
    freq_tol: int = 2,
    dt_tol: int = 1,
    offset_tol: int = 2,
) -> tuple[list[Fragment], list[Chunk]]:
    """
    Identifie les chunks déjà connus (présents dans la DB) et les chunks inconnus.

    Pour chaque chunk local, recherche les Ad de la DB dont les chunks correspondent
    (via acoustic pre-filter + distance-based scoring). Plusieurs Ad peuvent correspondre
    à un même chunk — toutes sont conservées dans KnownChunk.matching_ads, avec les indices
    exacts du chunk dans l'Ad (chunk_entry_index, chunk_index).

    Le traitement est fait page par page pour limiter la mémoire utilisée.

    Args:
        chunks: Les chunks locaux à identifier.
        params_hash: Hash des paramètres ChunkCreator, pour filtrer les chunks DB compatibles.
        min_matching_hashes: Nombre minimum de paires proches pour considérer deux chunks similaires.
        similarity_threshold: Score minimum (cohérence temporelle) pour valider une correspondance.
        freq_tol: Tolerance on frequency bin indices for pair matching.
        dt_tol: Tolerance on time delta for pair matching.
        offset_tol: Tolerance on temporal coherence offset.

    Returns:
        (known_chunks, unknown_chunks): chunks reconnus (avec leurs Ad correspondantes) et inconnus.
    """
    # matches[local_idx] accumulates AdChunkMatch across all pages
    matches: dict[int, list[AdChunkMatch]] = defaultdict(list)

    total_db_fingerprints = 0

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
                        db_fp = Fingerprint.from_dict(fp_dict)
                        total_db_fingerprints += 1

                        for local_idx, chunk in enumerate(chunks):
                            if are_fingerprints_similar(
                                chunk.fingerprint,
                                db_fp,
                                min_matching_hashes,
                                similarity_threshold,
                                freq_tol,
                                dt_tol,
                                offset_tol,
                            ):
                                matches[local_idx].append(
                                    AdChunkMatch(
                                        ad=ad,
                                        chunk_entry_index=entry_idx,
                                        chunk_index=fp_idx,
                                    )
                                )

    logger.info(f"Loaded {total_db_fingerprints} DB fingerprints (params_hash={params_hash})")

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
            db_fingerprint_count = len(db_entry.get("fingerprints", []))

            match, next_index = _current_fragment_is_a_match(
                chunk_match, db_fingerprint_count, chunks, local_idx, matches
            )

            if match:
                # We consider this fragment as already known, we add it to the known fragments list and skip all its chunks in the local list
                if chunk_match.chunk_index == 0:
                    start_sec = chunk.start_sec
                    end_sec = chunk.start_sec + db_ad.duration_sec
                else:
                    # We compute end_sec from the last chunk that match
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
    db_fingerprint_count: int,
    local_chunks: list[Chunk],
    current_local_idx: int,
    all_chunk_matches: dict[int, list[AdChunkMatch]],
) -> tuple[bool, int | None]:
    number_of_chunks_to_match = db_fingerprint_count - chunk_match.chunk_index
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
