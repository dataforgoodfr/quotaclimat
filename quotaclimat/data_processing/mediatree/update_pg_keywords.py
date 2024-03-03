### Library imports
import requests
import modin.pandas as pd

import logging

from sqlalchemy.orm import Session
from postgres.schemas.models import Keywords
from quotaclimat.data_processing.mediatree.detect_keywords import *
from sqlalchemy import func, select

def update_keywords(session: Session, batch_size: int = 50000, start_offset : int = 0) -> list:
    total_updates = get_total_count_saved_keywords(session)
    logging.info(f"Updating {total_updates} saved keywords from {start_offset} offsets - batch size {batch_size}")

    for i in range(start_offset, total_updates, batch_size):
        current_batch_saved_keywords = get_keywords_columns(session, i, batch_size)
        logging.info(f"Updating {len(current_batch_saved_keywords)} elements from {i} offsets - batch size {batch_size}")
        for keyword_id, plaintext, keywords_with_timestamp, number_of_keywords, start, srt, theme in current_batch_saved_keywords:
            matching_themes, new_keywords_with_timestamp, new_number_of_keywords = get_themes_keywords_duration(plaintext, srt, start)

            if(number_of_keywords != new_number_of_keywords or keywords_with_timestamp != new_keywords_with_timestamp or theme != matching_themes):
                logging.info(f"Difference detected for themes for ID {keyword_id} -  {theme} - {matching_themes} \
                             \nnumber_of_keywords {number_of_keywords} - {new_number_of_keywords}\
                             \nkeywords_with_timestamp : {keywords_with_timestamp}\
                             \n new_nkeywords_with_timestamp : {new_keywords_with_timestamp}"
                )
                update_keyword_row(session, keyword_id, new_number_of_keywords, new_keywords_with_timestamp, matching_themes)
            else:
                logging.debug("No difference")
        logging.info(f"bulk update done {i} out of {total_updates}")
        session.commit()

    logging.info("updated all keywords")


def get_keywords_columns(session: Session, page: int = 0, batch_size: int = 50000) -> list:
    return (
        session.query(
            Keywords.id,
            Keywords.plaintext,
            Keywords.keywords_with_timestamp,
            Keywords.number_of_keywords,
            Keywords.start,
            Keywords.srt,
            Keywords.theme
        )
        .offset(page)
        .limit(batch_size)
        .all()
    )

def get_total_count_saved_keywords(session: Session) -> int:
        statement = select(func.count()).select_from(Keywords)
        return session.execute(statement).scalar()
    

def update_keyword_row(session: Session, keyword_id: int, new_number_of_keywords: int, new_keywords_with_timestamp: List[dict], matching_themes: List[str]):
    session.query(Keywords).filter(Keywords.id == keyword_id).update(
        {
            Keywords.number_of_keywords: new_number_of_keywords,
            Keywords.keywords_with_timestamp: new_keywords_with_timestamp,
            Keywords.theme: matching_themes,
        },
        synchronize_session=False
    )