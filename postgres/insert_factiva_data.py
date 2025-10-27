import logging
from datetime import datetime
from typing import Any, Dict

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from postgres.schemas.models import Factiva_Article, factiva_articles_table


def process_factiva_event(event: Dict[str, Any], session: Session) -> str:
    """
    Process a single Factiva event and insert/update/delete in database.
    Returns the action performed: 'add', 'update', 'delete', 'source_delete', or 'skip'
    """
    try:
        # Check if it's a bulk event (e.g., source_delete)
        if "event_type" in event:
            return process_bulk_event(event, session)

        # Check document type
        document_type = event.get("document_type")
        if document_type != "article":
            logging.warning(
                f"Unexpected document_type: {document_type}, skipping event"
            )
            return "skip"

        # Get action and article ID
        action = event.get("action")
        an = event.get("an")

        if not an:
            logging.error("Event missing 'an' field, cannot process")
            return "skip"

        if action == "del":
            return delete_article(an, session)
        elif action in ["add", "rep"]:
            return upsert_article(event, session)
        else:
            logging.warning(f"Unknown action: {action}")
            return "skip"

    except Exception as err:
        logging.error(f"Error processing Factiva event: {err}")
        raise err


def process_bulk_event(event: Dict[str, Any], session: Session) -> str:
    """Process bulk events like source_delete"""
    event_type = event.get("event_type")

    if event_type == "source_delete":
        source_code = event.get("source_code")
        if not source_code:
            logging.error("source_delete event missing source_code")
            return "skip"

        logging.info(f"Processing source deletion for source_code: {source_code}")

        # Soft delete all articles from this source
        deleted_count = (
            session.query(Factiva_Article)
            .filter(
                Factiva_Article.source_code == source_code,
                Factiva_Article.is_deleted == False,
            )
            .update({"is_deleted": True, "updated_at": datetime.now()})
        )

        # Note: commit is handled by the caller for batch processing
        logging.info(f"Soft deleted {deleted_count} articles from source {source_code}")
        return "source_delete"
    else:
        logging.warning(f"Unknown bulk event_type: {event_type}")
        return "skip"


def delete_article(an: str, session: Session) -> str:
    """Soft delete an article by setting is_deleted flag"""
    article = session.query(Factiva_Article).filter(Factiva_Article.an == an).first()

    if article:
        article.is_deleted = True
        article.updated_at = datetime.now()
        # Note: commit is handled by the caller for batch processing
        logging.info(f"Soft deleted article {an}")
        return "delete"
    else:
        logging.info(f"Article {an} not found for deletion, skipping")
        return "skip"


def parse_datetime(dt_string: str) -> datetime:
    """Parse ISO format datetime string to datetime object"""
    if not dt_string:
        return None
    try:
        # Parse ISO format: "2025-05-14T00:00:00.000Z"
        return datetime.fromisoformat(dt_string.replace("Z", "+00:00"))
    except Exception as err:
        logging.warning(f"Could not parse datetime '{dt_string}': {err}")
        return None


def upsert_article(event: Dict[str, Any], session: Session) -> str:
    """Insert or update an article"""
    an = event.get("an")
    action = event.get("action")

    # Parse all datetime fields
    datetime_fields = [
        "publication_date",
        "publication_datetime",
        "modification_date",
        "modification_datetime",
        "ingestion_datetime",
        "availability_datetime",
    ]

    parsed_event = event.copy()
    for field in datetime_fields:
        if field in parsed_event and parsed_event[field]:
            parsed_event[field] = parse_datetime(parsed_event[field])

    # Prepare article data
    article_data = {
        "an": an,
        "document_type": parsed_event.get("document_type"),
        "action": action,
        "title": parsed_event.get("title"),
        "body": parsed_event.get("body"),
        "snippet": parsed_event.get("snippet"),
        "art": parsed_event.get("art"),
        "byline": parsed_event.get("byline"),
        "credit": parsed_event.get("credit"),
        "dateline": parsed_event.get("dateline"),
        "source_code": parsed_event.get("source_code"),
        "source_name": parsed_event.get("source_name"),
        "publisher_name": parsed_event.get("publisher_name"),
        "section": parsed_event.get("section"),
        "copyright": parsed_event.get("copyright"),
        "publication_date": parsed_event.get("publication_date"),
        "publication_datetime": parsed_event.get("publication_datetime"),
        "modification_date": parsed_event.get("modification_date"),
        "modification_datetime": parsed_event.get("modification_datetime"),
        "ingestion_datetime": parsed_event.get("ingestion_datetime"),
        "availability_datetime": parsed_event.get("availability_datetime"),
        "language_code": parsed_event.get("language_code"),
        "region_of_origin": parsed_event.get("region_of_origin"),
        "word_count": int(parsed_event.get("word_count", 0))
        if parsed_event.get("word_count")
        else None,
        "company_codes": parsed_event.get("company_codes"),
        "company_codes_about": parsed_event.get("company_codes_about"),
        "company_codes_association": parsed_event.get("company_codes_association"),
        "company_codes_lineage": parsed_event.get("company_codes_lineage"),
        "company_codes_occur": parsed_event.get("company_codes_occur"),
        "company_codes_relevance": parsed_event.get("company_codes_relevance"),
        "subject_codes": parsed_event.get("subject_codes"),
        "region_codes": parsed_event.get("region_codes"),
        "industry_codes": parsed_event.get("industry_codes"),
        "person_codes": parsed_event.get("person_codes"),
        "currency_codes": parsed_event.get("currency_codes"),
        "market_index_codes": parsed_event.get("market_index_codes"),
        "allow_translation": parsed_event.get("allow_translation"),
        "attrib_code": parsed_event.get("attrib_code"),
        "authors": parsed_event.get("authors"),
        "clusters": parsed_event.get("clusters"),
        "content_type_codes": parsed_event.get("content_type_codes"),
        "footprint_company_codes": parsed_event.get("footprint_company_codes"),
        "footprint_person_codes": parsed_event.get("footprint_person_codes"),
        "industry_classification_benchmark_codes": parsed_event.get(
            "industry_classification_benchmark_codes"
        ),
        "newswires_codes": parsed_event.get("newswires_codes"),
        "org_type_codes": parsed_event.get("org_type_codes"),
        "pub_page": parsed_event.get("pub_page"),
        "restrictor_codes": parsed_event.get("restrictor_codes"),
        "is_deleted": False,  # Reset deletion flag on update
        "updated_at": datetime.now(),
    }

    # Remove None values to avoid overwriting existing data with None
    article_data = {
        k: v for k, v in article_data.items() if v is not None or k == "is_deleted"
    }

    # Use SQLAlchemy UPSERT (INSERT ... ON CONFLICT DO UPDATE)
    stmt = insert(Factiva_Article).values(**article_data)

    # On conflict with primary key (an), update all fields except created_at
    update_dict = {
        k: v for k, v in article_data.items() if k not in ["an", "created_at"]
    }
    stmt = stmt.on_conflict_do_update(index_elements=["an"], set_=update_dict)

    try:
        session.execute(stmt)
        # Note: commit is handled by the caller for batch processing
        logging.info(f"{'Updated' if action == 'rep' else 'Inserted'} article {an}")
        return "update" if action == "rep" else "add"
    except Exception as e:
        logging.error(f"Error inserting/updating article {an}: {e}")
        raise


def get_article_stats(session: Session) -> Dict[str, int]:
    """Get statistics about articles in the database"""
    total = session.query(Factiva_Article).count()
    deleted = (
        session.query(Factiva_Article)
        .filter(Factiva_Article.is_deleted == True)
        .count()
    )
    active = total - deleted

    return {"total": total, "active": active, "deleted": deleted}
