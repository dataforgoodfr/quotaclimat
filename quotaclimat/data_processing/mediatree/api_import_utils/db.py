from datetime import date
import logging
from typing import Tuple
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from postgres.schemas.models import Keywords
from sqlalchemy.orm import Session
from sqlalchemy import Select, select, func, cast, Date, Integer, text
from typing import NamedTuple

class KeywordLastStats(NamedTuple):
    last_day_saved: date
    number_of_previous_days_from_yesterday: int

# Security nets to catch up delays from production servers errors
def get_last_date_and_number_of_delay_saved_in_keywords(session: Session) -> KeywordLastStats:
    logging.debug(f"get_last_date_and_number_of_delay_saved_in_keywords")
    try:
        source_subquery = (
            select(
                Keywords.start.label("start"),
                cast(
                    func.extract(
                        "day",
                        func.date_trunc("day", (func.now() - text("INTERVAL '1 day'"))) - func.date_trunc("day", Keywords.start),
                    ),
                    Integer,
                ).label("previous_days"),
            )
            .select_from(Keywords)
            .where(
                Keywords.start >= func.now() - text("INTERVAL '30 days'")
            )
            .subquery("source")
        )

        statement: Select[Tuple[date, int]] = (
            select(
                func.max(cast(source_subquery.c.start, Date)).label("last_day_saved"),
                func.min(source_subquery.c.previous_days).label("number_of_previous_days_from_yesterday"),
            )
        )

        result = session.execute(statement).fetchone()
        return KeywordLastStats(result[0], result[1])
    except Exception as err:
            logging.error("get_top_keywords_by_channel crash (%s) %s" % (type(err).__name__, err))
            raise err
    
def get_delay_date(lastSavedKeywordsDate: KeywordLastStats, normal_delay_in_days: int = 1):
    logging.warning(f"Delay detected : {lastSavedKeywordsDate.number_of_previous_days_from_yesterday } days, it should be {normal_delay_in_days} day")
    default_start_date = get_epoch_from_datetime(datetime(lastSavedKeywordsDate.last_day_saved.year,lastSavedKeywordsDate.last_day_saved.month,lastSavedKeywordsDate.last_day_saved.day))
    default_number_of_previous_days = lastSavedKeywordsDate.number_of_previous_days_from_yesterday
    return default_start_date, default_number_of_previous_days