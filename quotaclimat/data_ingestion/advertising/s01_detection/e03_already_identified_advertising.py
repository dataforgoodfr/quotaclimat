import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import Row, exists, func, or_, select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.compare import (
    FingerprintsCompare,
)

from .tools.common_objects import Chunk, Fingerprint, Fragment

logger = logging.getLogger(__name__)

CURSOR_BATCH_SIZE = 1000

# The comparison to the ads of the database is the longest step of the detection and
# grows with the table: its progress is logged every PROGRESS_LOG_INTERVAL_SEC seconds
PROGRESS_LOG_INTERVAL_SEC = 30

# fragment_type of the ads: "advertising" from the detection, then the content type of the
# classification (s03_classification/pipeline/e02_classify_ad.py), "AD" for an advertising
ADVERTISING_FRAGMENT_TYPES = {"advertising", "AD"}

# Only the ads detected (on any channel) less than this before the start of the processed
# window are searched for: older ads are no longer broadcast, and comparing with them costs
# time and false matches. An ad that comes back after this delay is detected again as a new ad.
KNOWN_AD_MAX_AGE = timedelta(days=90)


@dataclass
class AdChunkMatch:
    ad: Row  # id, duration_sec, fragment_type and chunks of an Ad
    chunk_entry_index: int  # index in ad.chunks list (the entry matching params_hash)
    chunk_index: int  # index within that entry's "fingerprints" list


async def run_chunk_identification(
    chunks: list[Chunk],
    params_hash: str,
    compare: FingerprintsCompare,
    reference_date: datetime,
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
        compare: FingerprintsCompare configuré avec les tolérances/seuils à utiliser.
        reference_date: début de la fenêtre traitée. Seules les Ad détectées depuis
            reference_date - KNOWN_AD_MAX_AGE (dernière occurrence non supprimée, toutes
            chaînes, ou première détection) sont recherchées.

    Returns:
        (known_chunks, unknown_chunks): chunks reconnus (avec leurs Ad correspondantes) et inconnus.
    """
    # matches[local_idx] accumulates AdChunkMatch across all pages
    matches: dict[int, list[AdChunkMatch]] = defaultdict(list)

    # Build inverted index over local chunks once — queried for each DB fingerprint
    started = time.monotonic()
    local_index = compare.build_similarity_index([c.fingerprint for c in chunks])
    logger.info(
        f"Chunks identification: index of the {len(chunks)} chunks of the week built "
        f"in {time.monotonic() - started:.0f}s"
    )

    compared_ads = 0
    total_db_fingerprints = 0
    total_candidates = 0
    total_matches = 0
    last_progress_log = time.monotonic()
    cutoff = reference_date - KNOWN_AD_MAX_AGE
    searched_ads = (
        Ad.deleted_at.is_(None),
        or_(
            Ad.first_detection_date >= cutoff,
            exists().where(
                Ad_Occurrence.ad_id == Ad.id,
                Ad_Occurrence.deleted_at.is_(None),
                Ad_Occurrence.occurrence_date >= cutoff,
            ),
        ),
    )

    with get_db_session() as session:
        total_ads = session.scalar(
            select(func.count()).select_from(Ad).where(*searched_ads)
        )
        logger.info(
            f"Chunks identification: comparing the chunks to the fingerprints of the "
            f"{total_ads} ads of the database detected since {cutoff}"
        )

        # Only the columns used here: the transcript and the prediction of the ads are
        # not read, and no ORM object is built for the hundreds of thousands of ads
        for ads in session.execute(
            select(Ad.id, Ad.duration_sec, Ad.fragment_type, Ad.chunks)
            .where(*searched_ads)
            .execution_options(yield_per=CURSOR_BATCH_SIZE)
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

                        candidates = local_index.get_similar_indices(db_fp)
                        total_candidates += len(candidates)
                        for local_idx in candidates:
                            if compare.is_similar(chunks[local_idx].fingerprint, db_fp):
                                total_matches += 1
                                matches[local_idx].append(
                                    AdChunkMatch(
                                        ad=ad,
                                        chunk_entry_index=entry_idx,
                                        chunk_index=fp_idx,
                                    )
                                )

            compared_ads += len(ads)
            if time.monotonic() - last_progress_log >= PROGRESS_LOG_INTERVAL_SEC:
                last_progress_log = time.monotonic()
                logger.info(
                    f"Chunks identification: {compared_ads}/{total_ads} ads compared "
                    f"({total_db_fingerprints} fingerprints, {total_candidates} candidate "
                    f"chunks, {total_matches} matching chunks) "
                    f"in {time.monotonic() - started:.0f}s"
                )

    logger.info(
        f"Chunks identification: {compared_ads} ads compared ({total_db_fingerprints} "
        f"fingerprints with params_hash={params_hash}, {total_candidates} candidate "
        f"chunks, {total_matches} matching chunks) in {time.monotonic() - started:.0f}s"
    )

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
        (m for m in matches if m.ad.fragment_type in ADVERTISING_FRAGMENT_TYPES),
        key=lambda m: (m.chunk_index, -m.ad.duration_sec),
    ) + sorted(
        (m for m in matches if m.ad.fragment_type not in ADVERTISING_FRAGMENT_TYPES),
        key=lambda m: (m.chunk_index, -m.ad.duration_sec),
    )
