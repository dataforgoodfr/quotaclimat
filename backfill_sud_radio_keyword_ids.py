"""
One-off backfill: fix keywords.channel_name 'sudradio' -> 'sud-radio',
recompute keywords.id to match, and fill in the program metadata
(channel_program, channel_program_type, program_metadata_id) these rows
never got, so they end up consistent with what the (now fixed) ingestion
pipeline would produce.

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

Why program_metadata_id was never set:
    update_programs_and_filter_out_of_scope_programs_from_df matches rows
    against the program grid by channel_name. Since these rows were tagged
    "sudradio" but the grid (program_metadata / channel_program.py) only
    knows "sud-radio", the match always failed - get_a_program_with_start_timestamp
    returned ("", "", None), and channel_program/channel_program_type/
    program_metadata_id were left empty/NULL. The rows weren't dropped
    because the empty-match filter only drops NaN, not "". Recomputing the
    match now with the corrected channel_name backfills those columns
    using the same program grid the pipeline uses today.

Batching:
    Processed one calendar day (Europe/Paris) at a time, each in its own
    transaction. This keeps memory/row-count per batch small enough to run
    from a laptop, and makes the script resumable: a day that's already been
    fixed no longer shows up in the "still sudradio" query, so re-running
    after an interruption just picks up where it left off. Use --limit-days
    to cap how many days a single invocation processes.

Usage:
    POSTGRES_HOST=... POSTGRES_USER=... POSTGRES_DB=... POSTGRES_PASSWORD=... \
        poetry run python3 backfill_sud_radio_keyword_ids.py [--dry-run] [--limit-days N]
"""
import argparse
import os
from datetime import timedelta

import pandas as pd
from sqlalchemy import text

from postgres.database_connection import connect_to_db
from quotaclimat.data_processing.mediatree.detect_keywords import add_primary_key
from quotaclimat.data_processing.mediatree.channel_program import (
    get_programs,
    get_a_program_with_start_timestamp,
)

OLD_NAME = "sudradio"
NEW_NAME = "sud-radio"


def get_pending_days(engine) -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT date(start) AS d FROM keywords "
                "WHERE channel_name = :old ORDER BY d"
            ),
            {"old": OLD_NAME},
        ).fetchall()
    return [r[0] for r in rows]


def process_day(engine, df_programs, day, dry_run: bool) -> dict:
    day_start = day
    day_end = day + timedelta(days=1)

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id, start FROM keywords WHERE channel_name = :old "
                "AND start >= :day_start AND start < :day_end"
            ),
            {"old": OLD_NAME, "day_start": day_start, "day_end": day_end},
        ).fetchall()

        updates = []
        seen_new_ids = {}
        no_program_found = 0
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

            channel_program, channel_program_type, program_metadata_id = (
                get_a_program_with_start_timestamp(df_programs, start_paris, NEW_NAME)
            )
            if program_metadata_id is None:
                no_program_found += 1

            updates.append({
                "old_id": old_id,
                "new_id": new_id,
                "channel_program": str(channel_program),
                "channel_program_type": str(channel_program_type),
                "program_metadata_id": program_metadata_id,
            })

        if no_program_found:
            print(f"  WARNING: no matching program grid entry found for {no_program_found} row(s) on {day} - channel_program/program_metadata_id will stay empty/NULL for those.")

        # make sure none of the new ids already exist under a different start
        # (would violate the (id, start) primary key - shouldn't happen since
        # no "sud-radio" rows exist yet, but check before writing).
        existing = conn.execute(
            text("SELECT id FROM keywords WHERE id = ANY(:ids)"),
            {"ids": [u["new_id"] for u in updates]},
        ).fetchall()
        if existing:
            raise RuntimeError(f"{len(existing)} target ids already exist in keywords for {day} - investigate before rerunning.")

        # program_metadata_id is a foreign key to program_metadata.id - make sure
        # every non-null value we computed actually exists there before writing.
        program_ids = {u["program_metadata_id"] for u in updates if u["program_metadata_id"] is not None}
        if program_ids:
            found = {
                row[0]
                for row in conn.execute(
                    text("SELECT id FROM program_metadata WHERE id = ANY(:ids)"),
                    {"ids": list(program_ids)},
                ).fetchall()
            }
            missing = program_ids - found
            if missing:
                raise RuntimeError(f"{len(missing)} computed program_metadata_id value(s) don't exist in program_metadata for {day} - investigate before rerunning: {missing}")

        if not dry_run:
            for u in updates:
                conn.execute(
                    text(
                        "UPDATE keywords SET id = :new_id, channel_name = :new_name, "
                        "channel_program = :channel_program, channel_program_type = :channel_program_type, "
                        "program_metadata_id = :program_metadata_id "
                        "WHERE id = :old_id"
                    ),
                    {
                        "new_id": u["new_id"],
                        "new_name": NEW_NAME,
                        "channel_program": u["channel_program"],
                        "channel_program_type": u["channel_program_type"],
                        "program_metadata_id": u["program_metadata_id"],
                        "old_id": u["old_id"],
                    },
                )

    return {"found": len(rows), "updated": len(updates), "no_program_found": no_program_found}


def main(dry_run: bool, limit_days: int = None) -> None:
    print(
        "Connecting to "
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"db={os.environ.get('POSTGRES_DB', 'barometre')} "
        f"user={os.environ.get('POSTGRES_USER', 'user')} "
        f"port={os.environ.get('POSTGRES_PORT', 5432)}"
    )
    engine = connect_to_db()
    df_programs = get_programs()

    days = get_pending_days(engine)
    print(f"Found {len(days)} distinct day(s) with channel_name = '{OLD_NAME}'")
    if days:
        print(f"Date range impacted: {days[0]} -> {days[-1]}")

    if limit_days is not None:
        days = days[:limit_days]
        print(f"Processing only the first {len(days)} day(s) this run (--limit-days {limit_days})")

    total_found = 0
    total_updated = 0
    total_no_program = 0
    for i, day in enumerate(days, start=1):
        stats = process_day(engine, df_programs, day, dry_run)
        total_found += stats["found"]
        total_updated += stats["updated"]
        total_no_program += stats["no_program_found"]
        action = "would update" if dry_run else "updated"
        print(f"[{i}/{len(days)}] {day}: {stats['found']} row(s) found, {action} {stats['updated']}")

    prefix = "[dry-run] Would update" if dry_run else "Updated"
    print(f"{prefix} {total_updated} rows total across {len(days)} day(s) "
          f"(channel_name '{OLD_NAME}' -> '{NEW_NAME}', id recomputed, program metadata backfilled). "
          f"{total_no_program} row(s) had no matching program grid entry.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-days", type=int, default=None, help="Only process the first N pending days, then stop.")
    args = parser.parse_args()
    main(dry_run=args.dry_run, limit_days=args.limit_days)
