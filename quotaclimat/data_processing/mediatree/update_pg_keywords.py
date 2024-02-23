### Library imports
import requests
import modin.pandas as pd
import json

import logging
from sqlalchemy.orm import Session
from postgres.schemas.models import Keywords
from quotaclimat.data_processing.mediatree.detect_keywords import *

def update_keywords(session: Session, batch_size: int = 50000) -> list:
    saved_keywords = get_keywords_columns(session)
    total_updates = len(saved_keywords)
    logging.info(f"Updating {total_updates} saved keywords")
    for i in range(0, total_updates, batch_size):
        batch_updates = saved_keywords[i:i+batch_size]
        for keyword_id, plaintext, keywords_with_timestamp, number_of_keywords in batch_updates:
            logging
            new_number_of_keywords = count_keywords_duration_overlap_without_indirect(keywords_with_timestamp)
            logging.debug(f"{keyword_id} new value {new_number_of_keywords}")
            update_number_of_keywords(session, keyword_id, new_number_of_keywords)

        logging.info(f"bulk update done {i} out of {total_updates}")
        session.commit()

    logging.info("updated all keywords")


def get_keywords_columns(session: Session) -> list:
    return (
        session.query(
            Keywords.id,
            Keywords.plaintext,
            Keywords.keywords_with_timestamp,
            Keywords.number_of_keywords
        )
        .all()
    )

def update_number_of_keywords(session: Session, keyword_id: int, new_number_of_keywords: int):
    session.query(Keywords).filter(Keywords.id == keyword_id).update(
        {Keywords.number_of_keywords: new_number_of_keywords},
        synchronize_session=False
    )