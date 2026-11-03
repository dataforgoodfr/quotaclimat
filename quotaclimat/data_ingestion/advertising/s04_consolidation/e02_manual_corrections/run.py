"""Stage 4 (consolidation), step 2: manual corrections of the ads, read from a private Google Sheet.

Every tab of the spreadsheet is read, except the tabs for humans (name starting with "_"). The beginning of
the name of a tab gives its kind (TAB_KINDS), all the tabs of a kind have the same columns:
- correction_ads...: one row per ad to correct (ad_id, marque, secteur, categorie, type_contenu);
- nouvelle_ad...: occurrences attributed to the wrong ad (occurrence_id), moved to a new ad, one per tab
  and per value of the optional ad_indicator column;
- dechets...: occurrences that are not ads (occurrence_id), soft deleted;
- reattribution...: occurrences attributed to the wrong ad (occurrence_id), moved to an existing ad (ad_id).

Each row is processed once: the job fills traite_le, resultat (appliqué, avertissement or erreur) and
message, and the rows with a traite_le are skipped (to process a row again, empty its traite_le). A warning
(value inconsistent with the reference data) does not stop the correction, an error changes nothing.

Env: POSTGRES_*, GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON, EXTERNAL_SOURCES_DRIVE_FOLDER,
MANUAL_CORRECTIONS_SPREADSHEET (default Corrections_publicites), BUCKET, BUCKET_SECRET,
ADVERTISING_BUCKET_NAME, MEDIATREE_BUCKET_NAME (videos of the new ads), MANUAL_CORRECTIONS_DRY_RUN
(default true: only logs, nothing written to the database, the buckets or the sheet).
"""

import json
import logging
import os
import sys
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.corrections import (
    MAX_DURATION_SPREAD_SEC,
    RESULT_ERROR,
    AdCorrection,
    Outcome,
    correct_ad,
    create_ad_from_occurrences,
    live_occurrences,
    new_ad_id,
    reattribute_occurrence,
    trash_occurrence,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.references import (
    References,
    load_references,
)
from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.sheet import (
    CorrectionsSheet,
    SheetRow,
)
from quotaclimat.utils.sentry import sentry_init

KIND_AD = "correction_ads"
KIND_NEW_AD = "nouvelle_ad"
KIND_TRASH = "dechets"
KIND_REATTRIBUTION = "reattribution"
# beginning of the name of a tab -> its kind
TAB_KINDS = (KIND_AD, KIND_NEW_AD, KIND_TRASH, KIND_REATTRIBUTION)
# column identifying the rows of a kind of tab
KEY_COLUMNS = {
    KIND_AD: "ad_id",
    KIND_NEW_AD: "occurrence_id",
    KIND_TRASH: "occurrence_id",
    KIND_REATTRIBUTION: "occurrence_id",
}

# columns filled by the job (added after the last column when missing)
COLUMN_DONE = "traite_le"
COLUMN_RESULT = "resultat"
COLUMN_MESSAGE = "message"
COLUMN_PREVIOUS = "valeurs_precedentes"
COLUMN_NEW_AD = "nouvelle_ad_id"
COLUMN_NEW_OCCURRENCE = "nouvelle_occurrence_id"
COLUMN_AD_INDICATOR = "ad_indicator"

VideoExporter = Callable[..., object]


def tab_kind(tab: str) -> str | None:
    return next((kind for kind in TAB_KINDS if tab.startswith(kind)), None)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _cells(outcome: Outcome, **extra: str) -> dict[str, str]:
    return {
        COLUMN_DONE: _now(),
        COLUMN_RESULT: outcome.result,
        COLUMN_MESSAGE: " ; ".join(outcome.messages),
        **extra,
    }


def _code(value: str | None) -> str | None:
    return value.strip().upper() if value and value.strip() else None


def process_ad_rows(
    session: Session,
    rows: list[SheetRow],
    references: References,
    spreadsheet: str,
    tab: str,
) -> list[tuple[SheetRow, dict[str, str]]]:
    updates = []
    for row in rows:
        correction = AdCorrection(
            brand=row.values.get("marque", "").strip() or None,
            sector=_code(row.values.get("secteur")),
            category=_code(row.values.get("categorie")),
            content_type=_code(row.values.get("type_contenu")),
        )
        source = {"spreadsheet": spreadsheet, "tab": tab, "row": row.number}
        try:
            with session.begin_nested():
                outcome, previous = correct_ad(
                    session, row.values["ad_id"], correction, references, source
                )
        except Exception as e:
            logging.exception("Correction of the ad %s failed", row.values["ad_id"])
            outcome, previous = Outcome.error(f"échec : {e}"), None
        extra = (
            {COLUMN_PREVIOUS: json.dumps(previous, ensure_ascii=False)}
            if previous
            else {}
        )
        updates.append((row, _cells(outcome, **extra)))
    return updates


def process_trash_rows(
    session: Session, rows: list[SheetRow]
) -> list[tuple[SheetRow, dict[str, str]]]:
    updates = []
    for row in rows:
        try:
            with session.begin_nested():
                outcome = trash_occurrence(session, row.values["occurrence_id"])
        except Exception as e:
            logging.exception(
                "Deletion of the occurrence %s failed", row.values["occurrence_id"]
            )
            outcome = Outcome.error(f"échec : {e}")
        updates.append((row, _cells(outcome)))
    return updates


def process_reattribution_rows(
    session: Session, rows: list[SheetRow]
) -> list[tuple[SheetRow, dict[str, str]]]:
    updates = []
    seen = set()
    for row in rows:
        occurrence_id = row.values["occurrence_id"]
        extra = {}
        if occurrence_id in seen:
            outcome = Outcome.error("occurrence en double dans l'onglet")
        else:
            seen.add(occurrence_id)
            try:
                with session.begin_nested():
                    outcome, new_id = reattribute_occurrence(
                        session, occurrence_id, row.values.get("ad_id", "").strip()
                    )
                if new_id:
                    extra = {COLUMN_NEW_OCCURRENCE: new_id}
            except Exception as e:
                logging.exception(
                    "Reattribution of the occurrence %s failed", occurrence_id
                )
                outcome = Outcome.error(f"échec : {e}")
        updates.append((row, _cells(outcome, **extra)))
    return updates


def _new_ad_group(
    session: Session,
    rows: list[SheetRow],
    ad_id: str,
    export_video: VideoExporter,
    dry_run: bool,
) -> list[tuple[SheetRow, dict[str, str]]]:
    """Move the occurrences of the rows (one ad_indicator of a tab) to the ad ad_id."""
    outcomes: dict[int, Outcome] = {}
    occurrence_ids = []
    for row in rows:
        occurrence_id = row.values["occurrence_id"]
        if occurrence_id in occurrence_ids:
            outcomes[row.number] = Outcome.error("occurrence en double dans l'onglet")
        else:
            occurrence_ids.append(occurrence_id)
    occurrences, errors = live_occurrences(session, occurrence_ids)
    existing_ad = session.get(Ad, ad_id)
    for occurrence_id, occurrence in list(occurrences.items()):
        if occurrence.ad_id == ad_id:
            errors[occurrence_id] = Outcome.error("occurrence déjà dans cette ad")
            del occurrences[occurrence_id]

    group_warnings: list[str] = []
    reference = None
    duration_sec = existing_ad.duration_sec if existing_ad is not None else None
    if occurrences and existing_ad is None:
        durations = dict(
            session.execute(
                select(Ad.id, Ad.duration_sec).where(
                    Ad.id.in_(list({o.ad_id for o in occurrences.values()}))
                )
            ).all()
        )
        counts = Counter(round(durations[o.ad_id], 1) for o in occurrences.values())
        duration_sec = counts.most_common(1)[0][0]
        if max(counts) - min(counts) > MAX_DURATION_SPREAD_SEC:
            group_warnings.append(
                f"ads d'origine de durées différentes ({', '.join(f'{d:g}s' for d in sorted(counts))}),"
                f" durée retenue {duration_sec:g}s"
            )
        reference = export_video(
            ad_id, list(occurrences.values()), duration_sec, dry_run
        )
        if reference is None:
            for occurrence_id in occurrences:
                errors[occurrence_id] = Outcome.error(
                    "aucune occurrence n'a sa vidéo dans le bucket mediatree : ad non créée"
                )
            occurrences = {}

    new_ids: dict[str, str] = {}
    if occurrences:
        try:
            with session.begin_nested():
                new_ids = create_ad_from_occurrences(
                    session, ad_id, list(occurrences.values()), duration_sec
                )
        except Exception as e:
            logging.exception("Creation of the ad %s failed", ad_id)
            for occurrence_id in occurrences:
                errors[occurrence_id] = Outcome.error(f"échec : {e}")
            occurrences = {}

    updates = []
    for row in rows:
        occurrence_id = row.values["occurrence_id"]
        outcome = outcomes.get(row.number) or errors.get(occurrence_id)
        if outcome is None:
            outcome = Outcome()
            for warning in group_warnings:
                outcome.warn(warning)
            if reference is not None and reference.id == occurrence_id:
                outcome.messages.append("vidéo de l'ad prise sur cette occurrence")
        extra = (
            {COLUMN_NEW_AD: ad_id, COLUMN_NEW_OCCURRENCE: new_ids[occurrence_id]}
            if outcome.result != RESULT_ERROR
            else {}
        )
        updates.append((row, _cells(outcome, **extra)))
    return updates


def process_new_ad_rows(
    session: Session,
    rows: list[SheetRow],
    spreadsheet: str,
    tab: str,
    export_video: VideoExporter,
    dry_run: bool,
) -> list[tuple[SheetRow, dict[str, str]]]:
    groups: dict[str, list[SheetRow]] = {}
    for row in rows:
        groups.setdefault(row.values.get(COLUMN_AD_INDICATOR, "").strip(), []).append(
            row
        )
    updates = []
    for indicator, group_rows in groups.items():
        ad_id = new_ad_id(spreadsheet, tab, indicator)
        updates.extend(_new_ad_group(session, group_rows, ad_id, export_video, dry_run))
    return updates


def process_tab(
    sheet,
    session: Session,
    tab: str,
    spreadsheet: str,
    references: References,
    export_video: VideoExporter,
    dry_run: bool,
) -> Counter:
    kind = tab_kind(tab)
    if kind is None:
        logging.warning(
            "Tab %s skipped: its name must start with %s (or _ for a tab for humans)",
            tab,
            ", ".join(TAB_KINDS),
        )
        return Counter()
    key_column = KEY_COLUMNS[kind]
    header, rows = sheet.read(tab)
    if key_column not in header:
        logging.error("Tab %s skipped: no column %s", tab, key_column)
        return Counter()
    pending = [
        row
        for row in rows
        if row.values.get(key_column) and not row.values.get(COLUMN_DONE)
    ]
    if not pending:
        return Counter()
    for row in pending:
        row.values[key_column] = row.values[key_column].strip()

    if kind == KIND_AD:
        updates = process_ad_rows(session, pending, references, spreadsheet, tab)
    elif kind == KIND_TRASH:
        updates = process_trash_rows(session, pending)
    elif kind == KIND_REATTRIBUTION:
        updates = process_reattribution_rows(session, pending)
    else:
        updates = process_new_ad_rows(
            session, pending, spreadsheet, tab, export_video, dry_run
        )

    results = Counter(values[COLUMN_RESULT] for _, values in updates)
    for row, values in updates:
        logging.info(
            "%s row %s (%s): %s %s",
            tab,
            row.number,
            row.values[key_column],
            values[COLUMN_RESULT],
            values[COLUMN_MESSAGE],
        )
    if dry_run:
        session.rollback()
        logging.info("Dry run: tab %s, %s, nothing written", tab, dict(results))
        return results
    # the sheet first: when it cannot be written, nothing is committed and the rows are processed again
    written = sheet.write_rows(tab, key_column, updates)
    session.commit()
    logging.info("Tab %s: %s, %s rows written", tab, dict(results), written)
    return results


def run(
    sheet,
    session: Session,
    spreadsheet: str,
    export_video: VideoExporter,
    dry_run: bool,
    references: References | None = None,
) -> Counter:
    references = references if references is not None else load_references(session)
    total = Counter()
    for tab in sheet.tabs():
        try:
            total += process_tab(
                sheet, session, tab, spreadsheet, references, export_video, dry_run
            )
        except Exception:
            session.rollback()
            logging.exception("Tab %s failed, nothing written for it", tab)
    logging.info("Manual corrections: %s", dict(total))
    return total


def main() -> Counter:
    # the video of the new ads needs the exportation code (s3 buckets, ffmpeg)
    from quotaclimat.data_ingestion.advertising.s04_consolidation.e02_manual_corrections.video import (
        export_new_ad_video,
    )

    spreadsheet = os.environ.get(
        "MANUAL_CORRECTIONS_SPREADSHEET", "Corrections_publicites"
    )
    dry_run = os.environ.get("MANUAL_CORRECTIONS_DRY_RUN", "true").lower() == "true"
    logging.info("Manual corrections of %s (dry run: %s)", spreadsheet, dry_run)
    sheet = CorrectionsSheet.open(spreadsheet)
    engine = connect_to_db(use_custom_json_serializer=True)
    try:
        with Session(engine) as session:
            return run(sheet, session, spreadsheet, export_new_ad_video, dry_run)
    finally:
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"), stream=sys.stdout)
    sentry_init()
    main()
