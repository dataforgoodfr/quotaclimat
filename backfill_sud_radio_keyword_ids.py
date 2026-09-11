"""
One-off backfill: fix keywords.channel_name 'sudradio' -> 'sud-radio' AND
recompute keywords.id to match, so the rows stay consistent with what the
(now fixed) ingestion pipeline would produce.

Why id must be recomputed too:
    keywords.id = sha256(str(start) + channel_name), and (id, start) is the
    upsert conflict target in postgres/insert_data.py:insert_or_update_on_conflict.
    If we only renamed channel_name, a future reprocessing of these dates
    would compute a different id (based on "sud-radio") than what's stored,
    fail to match on conflict, and insert duplicate rows instead of updating
    the existing ones.

How the id is reconstructed:
    keywords.start is stored as a naive "timestamp without time zone" column.
    The postgres_db container (and prod) runs with TZ=Europe/Paris
    (docker-compose.yml), so a tz-aware Europe/Paris timestamp written by the
    pipeline round-trips back out as an equivalent naive Europe/Paris
    wall-clock value. Re-localizing it to Europe/Paris and running it back
    through add_primary_key() reproduces the exact hash add_primary_key
    would compute today - verified empirically against a local Postgres
    instance (insert a known tz-aware timestamp, read it back naive,
    re-localize, recompute -> identical hash).

Usage:
    POSTGRES_HOST=... POSTGRES_USER=... POSTGRES_DB=... POSTGRES_PASSWORD=... \
        poetry run python3 backfill_sud_radio_keyword_ids.py [--dry-run]
"""
import os
import sys

import pandas as pd
from sqlalchemy import text

from postgres.database_connection import connect_to_db
from quotaclimat.data_processing.mediatree.detect_keywords import add_primary_key

OLD_NAME = "sudradio"
NEW_NAME = "sud-radio"


def main(dry_run: bool) -> None:
    print(
        "Connecting to "
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"db={os.environ.get('POSTGRES_DB', 'barometre')} "
        f"user={os.environ.get('POSTGRES_USER', 'user')} "
        f"port={os.environ.get('POSTGRES_PORT', 5432)}"
    )
    engine = connect_to_db()

    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT id, start FROM keywords WHERE channel_name = :old"),
            {"old": OLD_NAME},
        ).fetchall()

        print(f"Found {len(rows)} rows with channel_name = '{OLD_NAME}'")
        if rows:
            starts = [start for _, start in rows]
            print(f"Date range impacted: {min(starts)} -> {max(starts)}")

        updates = []
        seen_new_ids = {}
        for old_id, start_value in rows:
            start_ts = pd.Timestamp(start_value)
            if start_ts.tzinfo is None:
                # naive "timestamp without time zone" column: the pipeline
                # always writes Europe/Paris wall-clock values, so re-localize.
                start_paris = start_ts.tz_localize("Europe/Paris")
            else:
                # tz-aware "timestamp with time zone" column: the value is an
                # exact instant, so just re-express it in Europe/Paris to match
                # what add_primary_key expects (it checks the tzinfo string).
                start_paris = start_ts.tz_convert("Europe/Paris")
            row = pd.Series({"start": start_paris, "channel_name": NEW_NAME})
            new_id = add_primary_key(row)

            if new_id in seen_new_ids:
                print(
                    f"WARNING: collision - old id {old_id} and {seen_new_ids[new_id]} "
                    f"both map to new id {new_id} (start={start_value}). Skipping this row."
                )
                continue
            seen_new_ids[new_id] = old_id
            updates.append({"old_id": old_id, "new_id": new_id})

        # make sure none of the new ids already exist under a different start
        # (would violate the (id, start) primary key - shouldn't happen since
        # no "sud-radio" rows exist yet, but check before writing).
        existing = conn.execute(
            text("SELECT id FROM keywords WHERE id = ANY(:ids)"),
            {"ids": [u["new_id"] for u in updates]},
        ).fetchall()
        if existing:
            print(f"ABORT: {len(existing)} target ids already exist in keywords - investigate before rerunning.")
            sys.exit(1)

        if dry_run:
            print(f"[dry-run] Would update {len(updates)} rows (id + channel_name).")
            for u in updates[:5]:
                print(f"  {u['old_id']} -> {u['new_id']}")
            return

        for u in updates:
            conn.execute(
                text(
                    "UPDATE keywords SET id = :new_id, channel_name = :new_name "
                    "WHERE id = :old_id"
                ),
                {"new_id": u["new_id"], "new_name": NEW_NAME, "old_id": u["old_id"]},
            )

        print(f"Updated {len(updates)} rows: channel_name '{OLD_NAME}' -> '{NEW_NAME}', id recomputed.")


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
