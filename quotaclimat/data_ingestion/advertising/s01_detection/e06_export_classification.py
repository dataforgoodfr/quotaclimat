import hashlib
import json
import logging
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence

from .e04_group_chunks import canonical
from .e05_classify_fragments import Fragment

logger = logging.getLogger(__name__)

AD_CLASSIFICATIONS = {"already_known_ad", "new_ad", "jingle"}
BULK_PAGE_SIZE = 1000


def _bulk_insert_pages(session, model, rows: list[dict]) -> None:
    for i in range(0, len(rows), BULK_PAGE_SIZE):
        session.execute(
            insert(model).on_conflict_do_nothing(index_elements=["id"]),
            rows[i : i + BULK_PAGE_SIZE],
        )


def _ad_id_from_chunks(chunks) -> str:
    """Generate a stable Ad ID from the chunk audio hashes."""
    all_hash_strings = sorted(
        h for chunk in chunks for h, _ in (chunk.fingerprint.hashes or [])
    )
    raw = json.dumps(all_hash_strings, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _occurrence_id(ad_id: str, occurrence_sec: float, channel: str) -> str:
    raw = f"{ad_id}|{occurrence_sec}|{channel}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def database_storage_save(fragments: list[Fragment], chunk_hash: str):
    ad_fragments = [f for f in fragments if f.classification in AD_CLASSIFICATIONS]

    if not ad_fragments:
        logger.info("No ad fragments to save.")
        return

    # Group fragments by group_id (same group_id = same ad repeated)
    groups: dict[str, list[Fragment]] = {}
    ungrouped: list[Fragment] = []
    for fragment in ad_fragments:
        if fragment.group_id is not None:
            groups.setdefault(fragment.group_id, []).append(fragment)
        else:
            ungrouped.append(fragment)

    session = get_db_session()
    try:
        ads: list[Ad] = []
        occurrences: list[Ad_Occurrence] = []

        for group_id, group_fragments in groups.items():
            if any(f.classification == "already_known_ad" for f in group_fragments):
                # We reuse the group_id because it should be the ad id. We jump to the occurence creation.
                ad_id = group_id
            else:
                # Each fragment has the same number of chunks; build one canonical
                # chunk per position by comparing the nth chunk across all fragments.
                chunks_by_position = zip(*(f.chunks or [] for f in group_fragments))
                canonical_chunks = [
                    canonical(list(position)) for position in chunks_by_position
                ]

                ad_id = _ad_id_from_chunks(canonical_chunks)
                first_fragment = min(group_fragments, key=lambda f: f.start_sec)

                fragment_type = (
                    "jingle"
                    if all(f.classification == "jingle" for f in group_fragments)
                    else "advertising"
                )

                ads.append(
                    Ad(
                        id=ad_id,
                        first_detection_date=datetime.fromtimestamp(
                            first_fragment.start_sec
                        ),
                        duration_sec=(
                            first_fragment.end_sec - first_fragment.start_sec
                        ),
                        chunks=[
                            {
                                "hash": chunk_hash,
                                "fingerprints": [
                                    c.fingerprint.to_dict() for c in canonical_chunks
                                ],
                            }
                        ],
                        fragment_type=fragment_type,
                    )
                )

            for fragment in group_fragments:
                fragment.group_id = ad_id
                occ_id = _occurrence_id(ad_id, fragment.start_sec, fragment.channel)
                occurrences.append(
                    Ad_Occurrence(
                        id=occ_id,
                        occurrence_date=datetime.fromtimestamp(fragment.start_sec),
                        channel_name=fragment.channel,
                        ad_id=ad_id,
                    )
                )

        for fragment in ungrouped:
            chunks = fragment.chunks or []
            ad_id = _ad_id_from_chunks(chunks)
            fragment.group_id = ad_id
            fragment_type = (
                "jingle" if fragment.classification == "jingle" else "advertising"
            )

            ads.append(
                Ad(
                    id=ad_id,
                    first_detection_date=datetime.fromtimestamp(fragment.start_sec),
                    duration_sec=(fragment.end_sec - fragment.start_sec),
                    chunks=[
                        {
                            "hash": chunk_hash,
                            "fingerprints": [c.fingerprint.to_dict() for c in chunks],
                        }
                    ],
                    fragment_type=fragment_type,
                )
            )

            occ_id = _occurrence_id(ad_id, fragment.start_sec, fragment.channel)
            occurrences.append(
                Ad_Occurrence(
                    id=occ_id,
                    occurrence_date=datetime.fromtimestamp(fragment.start_sec),
                    channel_name=fragment.channel,
                    ad_id=ad_id,
                )
            )

        _bulk_insert_pages(
            session,
            Ad,
            [ad.attributes() for ad in ads],
        )
        session.flush()

        _bulk_insert_pages(
            session,
            Ad_Occurrence,
            [occ.attributes() for occ in occurrences],
        )
        session.commit()

        logger.info(f"Saved {len(ads)} ads and {len(occurrences)} occurrences.")

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
