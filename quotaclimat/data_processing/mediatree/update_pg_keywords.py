### Library imports
import requests
import pandas as pd
import json

import logging
from sqlalchemy.orm import Session
from postgres.schemas.models import Keywords
from quotaclimat.data_processing.mediatree.detect_keywords import *

def update_keywords(session: Session) -> list:
    saved_keywords = get_keywords_columns(session)
    for keyword_id, plaintext, keywords_with_timestamp, number_of_keywords in saved_keywords:
        new_number_of_keywords = count_keywords_duration_overlap(keywords_with_timestamp)

        update_number_of_keywords(session, keyword_id, new_number_of_keywords)
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
    session.commit()