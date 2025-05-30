### Library imports
import requests
import modin.pandas as pd

import logging
import os
from sqlalchemy.orm import Session
from postgres.schemas.models import Keywords, Stop_Word
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.api_import import get_stop_words
from quotaclimat.data_processing.mediatree.channel_program import get_programs, get_a_program_with_start_timestamp, get_channel_title_for_name
from sqlalchemy import func, select, and_, or_
from quotaclimat.data_processing.mediatree.i8n.country import FRANCE

def get_keyword_else_context(stop_word_object: Stop_Word):
    if stop_word_object.keyword is not None:
        return stop_word_object.keyword
    else:
        return stop_word_object.context

def get_top_keyword_of_stop_words(stop_word_keyword_only: bool, stop_words_objects: List[Stop_Word]):
    top_keyword_of_stop_words = []
    if stop_word_keyword_only and (len(stop_words_objects) > 0):
        logging.warning(f"Using stop words to filter rows inside Keywords table")

        top_keyword_of_stop_words = set(map(lambda stop: get_keyword_else_context(stop), stop_words_objects))
        logging.info(f"stop words keywords : {top_keyword_of_stop_words}")
    else:
        logging.info(f"No filter on plaintext for Keywords table - stop_word_keyword_only env variable to false")

    return top_keyword_of_stop_words


def get_timestamp_with_tz(start: str, country = FRANCE) -> pd.Timestamp:
    try:
        start_tz = pd.Timestamp(start)
        logging.info(f"start timezone : {start_tz.tzinfo} - timestamp {start_tz} - original {start}- {type(start_tz)}")
        if start_tz.tzinfo is None:
            logging.warning(f"start timezone is None, localizing to UTC")
            start_tz = start_tz.tz_localize("UTC")
        
        start_tz = start_tz.tz_convert(country.timezone)
        return start_tz
    except Exception as err:
        logging.error(f"Error converting start {start} to timestamp with timezone {country.timezone} : {err}")
        raise ValueError(f"Error converting start {start} to timestamp with timezone {country.timezone} : {err}")

def update_program_only(keyword_id: int, start: str, channel_name: str, channel_title: str, session: Session, country = FRANCE):
    logging.debug(f"Updating program for keyword {keyword_id} - {channel_name} - original tz : {start}")

    start_tz = get_timestamp_with_tz(start=start, country=country)
    logging.info(f"Updating program for keyword {keyword_id} - {channel_name} - converted tz : {start_tz}")
    df_programs = get_programs()
    program_name, program_name_type, program_metadata_id = \
            get_a_program_with_start_timestamp(df_program=df_programs, start_time=start_tz, channel_name=channel_name)
    
    logging.info(f"new data for keyword_id ({keyword_id}): program_name ({program_name}) - program_name_type ({program_name_type}) - program_metadata_id ({program_metadata_id})")
    try:
        update_keyword_row_program(session
            ,keyword_id
            ,channel_program=program_name
            ,channel_program_type=program_name_type
            ,channel_title=channel_title
            ,program_metadata_id=program_metadata_id
        )
    except Exception as err:
        logging.error(f"update_keyword_row_program - continuing loop but met error : {err}")
   
def update_keywords(session: Session, batch_size: int = 50000, start_date : str = "2023-04-01", program_only=False, \
                    end_date: str = "2023-04-30", channel: str = "", empty_program_only=False, \
                        stop_word_keyword_only = False, \
                        biodiversity_only = False,
                        country=FRANCE) -> list:

    filter_days_stop_word = int(os.environ.get("FILTER_DAYS_STOP_WORD", 30))
    logging.info(f"FILTER_DAYS_STOP_WORD is used to get only last {filter_days_stop_word} days of new stop words - to improve update speed")
    stop_words_objects = get_stop_words(session, validated_only=True, context_only=False, 
                                        filter_days=filter_days_stop_word
                                        ,country=country)
    stop_words = list(map(lambda stop: stop.context, stop_words_objects))
    top_keyword_of_stop_words = get_top_keyword_of_stop_words(stop_word_keyword_only,
                                                               stop_words_objects=stop_words_objects)

    total_updates = get_total_count_saved_keywords(session, start_date, end_date, channel, empty_program_only, \
                                                        keywords_to_includes=top_keyword_of_stop_words, 
                                                        biodiversity_only=biodiversity_only,
                                                        country=country)

    if total_updates == 0:
        logging.error("No rows to update - change your START_DATE_UPDATE")
        return 0
    elif (batch_size > total_updates):
        logging.info(f"Fixing batch size ({batch_size}) to {total_updates} because too high compared to saved elements")
        batch_size = total_updates

    logging.info(f"Updating {country.name} - {total_updates} saved keywords from {start_date} date to {end_date} for channel {channel} - batch size {batch_size} - totals rows")
    
    for i in range(0, total_updates, batch_size):
        current_batch_saved_keywords = get_keywords_columns(session, i, batch_size, start_date, end_date, channel, \
                                                            empty_program_only, keywords_to_includes=top_keyword_of_stop_words, \
                                                            biodiversity_only=biodiversity_only, country=country)
        logging.info(f"Updating {len(current_batch_saved_keywords)} elements from {i} offsets - batch size {batch_size} - until offset {total_updates}")
        for keyword_id, plaintext, keywords_with_timestamp, number_of_keywords, start, srt, theme, channel_name, channel_title in current_batch_saved_keywords:
            if channel_title is None:
                logging.warning(f"channel_title none, set it using channel_name {channel_name}")
                channel_title = get_channel_title_for_name(channel_name)

            if(not program_only):
                try:
                    matching_themes, \
                    new_keywords_with_timestamp, \
                    new_number_of_keywords, \
                    number_of_changement_climatique_constat \
                    ,number_of_changement_climatique_causes_directes \
                    ,number_of_changement_climatique_consequences \
                    ,number_of_attenuation_climatique_solutions_directes \
                    ,number_of_adaptation_climatique_solutions_directes \
                    ,number_of_ressources \
                    ,number_of_ressources_solutions \
                    ,number_of_biodiversite_concepts_generaux \
                    ,number_of_biodiversite_causes_directes \
                    ,number_of_biodiversite_consequences \
                    ,number_of_biodiversite_solutions_directes \
                    ,new_number_of_keywords_climat \
                    ,new_number_of_keywords_biodiversite \
                    ,new_number_of_keywords_ressources \
                    ,number_of_changement_climatique_constat_no_hrfp \
                    ,number_of_changement_climatique_causes_no_hrfp \
                    ,number_of_changement_climatique_consequences_no_hrfp \
                	,number_of_attenuation_climatique_solutions_no_hrfp \
                    ,number_of_adaptation_climatique_solutions_no_hrfp \
                    ,number_of_ressources_no_hrfp \
                    ,number_of_ressources_solutions_no_hrfp \
                    ,number_of_biodiversite_concepts_generaux_no_hrfp \
                    ,number_of_biodiversite_causes_no_hrfp \
                    ,number_of_biodiversite_consequences_no_hrfp \
                    ,number_of_biodiversite_solutions_no_hrfp \
                    ,country_name = get_themes_keywords_duration(plaintext, srt, start, stop_words=stop_words)

                    
                    if(number_of_keywords != new_number_of_keywords or
                        keywords_with_timestamp != new_keywords_with_timestamp or
                        theme != matching_themes
                        ):
                        logging.info(f"Difference detected for themes for ID {keyword_id} -  {theme} - {matching_themes} \
                                    \nnumber_of_keywords {number_of_keywords} - {new_number_of_keywords}\
                                    \nkeywords_with_timestamp : {keywords_with_timestamp}\
                                    \n new_nkeywords_with_timestamp : {new_keywords_with_timestamp}"
                        )
                    else:
                        logging.debug("No difference")
                except Exception as err:
                    logging.error(f"get_themes_keywords_duration - continuing loop but met error : {err}")
                    continue
                try:
                    update_keyword_row(session,
                    keyword_id,
                    new_number_of_keywords,
                    new_keywords_with_timestamp,
                    matching_themes
                    ,number_of_changement_climatique_constat
                    ,number_of_changement_climatique_causes_directes
                    ,number_of_changement_climatique_consequences
                    ,number_of_attenuation_climatique_solutions_directes
                    ,number_of_adaptation_climatique_solutions_directes
                    ,number_of_ressources
                    ,number_of_ressources_solutions
                    ,number_of_biodiversite_concepts_generaux
                    ,number_of_biodiversite_causes_directes
                    ,number_of_biodiversite_consequences
                    ,number_of_biodiversite_solutions_directes
                    ,channel_title
                    ,new_number_of_keywords_climat
                    ,new_number_of_keywords_biodiversite
                    ,new_number_of_keywords_ressources
                    ,number_of_changement_climatique_constat_no_hrfp
                    ,number_of_changement_climatique_causes_no_hrfp
                    ,number_of_changement_climatique_consequences_no_hrfp
                    ,number_of_attenuation_climatique_solutions_no_hrfp
                    ,number_of_adaptation_climatique_solutions_no_hrfp
                    ,number_of_ressources_no_hrfp
                    ,number_of_ressources_solutions_no_hrfp
                    ,number_of_biodiversite_concepts_generaux_no_hrfp
                    ,number_of_biodiversite_causes_no_hrfp
                    ,number_of_biodiversite_consequences_no_hrfp
                    ,number_of_biodiversite_solutions_no_hrfp
                    )
                except Exception as err:
                    logging.error(f"update_keyword_row - continuing loop but met error : {err}")
                    continue
            else: # Program only mode
                try:
                    update_program_only(
                        keyword_id=keyword_id,
                        start=start,
                        channel_name=channel_name,
                        channel_title=channel_title,
                        session=session,
                        country=country
                    )
                except Exception as err:
                    logging.error(f"update_program_only - continuing loop but met error : {err}")
                    continue
        logging.info(f"bulk update done {i} out of {total_updates} - (max offset {total_updates})")
        session.commit()

    logging.info("updated all keywords")


def get_keywords_columns(session: Session, offset: int = 0, batch_size: int = 50000, start_date: str = "2023-04-01", end_date: str = "2023-04-30",\
                         channel: str = "", empty_program_only: bool = False, keywords_to_includes: list[str] = [], \
                         biodiversity_only = False, country = FRANCE) -> list:
    logging.info(f"Getting {batch_size} elements from offset {offset} with timezone {country.timezone}")
    query = session.query(
            Keywords.id,
            Keywords.plaintext,
            Keywords.keywords_with_timestamp,
            Keywords.number_of_keywords,
            Keywords.start,
            Keywords.srt,
            Keywords.theme,
            Keywords.channel_name,
            Keywords.channel_title,
        ).filter(
        and_(
            func.date(Keywords.start) >= start_date, 
            func.date(Keywords.start) <= end_date,
            Keywords.country == country.name
        )
    ).order_by(Keywords.start, Keywords.channel_name, Keywords.plaintext)

    if channel != "":
        query = query.filter(Keywords.channel_name == channel)

    if empty_program_only:
        query = query.filter(Keywords.channel_program == "")

    if biodiversity_only:
        query = query.filter(
            or_(
                Keywords.number_of_biodiversite_concepts_generaux > 0
                ,Keywords.number_of_biodiversite_causes_directes > 0
                ,Keywords.number_of_biodiversite_consequences > 0
                ,Keywords.number_of_biodiversite_solutions_directes > 0
                ,Keywords.number_of_biodiversite_concepts_generaux_no_hrfp > 0
                ,Keywords.number_of_biodiversite_causes_no_hrfp > 0
                ,Keywords.number_of_biodiversite_consequences_no_hrfp > 0
                ,Keywords.number_of_biodiversite_solutions_no_hrfp > 0
            )
        )

    # https://stackoverflow.com/a/33389165/3535853
    if len(keywords_to_includes) > 0:
        logging.info(f"Filtering plaintext that include some {len(keywords_to_includes)} keywords")
        query = query.filter(
            or_(*[Keywords.plaintext.ilike(f"%{word}%") for word in keywords_to_includes])
        )

    return query.offset(offset) \
        .limit(batch_size) \
        .all()

def get_total_count_saved_keywords(session: Session, start_date : str, end_date : str, channel: str, empty_program_only: bool,\
                                    keywords_to_includes= [], biodiversity_only = False, country= FRANCE) -> int:
        statement = select(func.count()).filter(
            and_(func.date(Keywords.start) >= start_date, func.date(Keywords.start) <= end_date)
        ).select_from(Keywords)
        
        if channel != "":
            statement = statement.filter(Keywords.channel_name == channel)
        
        statement = statement.filter(Keywords.country == country.name)
        
        if empty_program_only:
            statement = statement.filter(
                or_(
                    Keywords.channel_program.is_(None),
                    Keywords.channel_program == "" 
                )
            )

        if biodiversity_only:
            statement = statement.filter(
                or_(
                    Keywords.number_of_biodiversite_concepts_generaux > 0
                    ,Keywords.number_of_biodiversite_causes_directes > 0
                    ,Keywords.number_of_biodiversite_consequences > 0
                    ,Keywords.number_of_biodiversite_solutions_directes > 0
                    ,Keywords.number_of_biodiversite_concepts_generaux_no_hrfp > 0
                    ,Keywords.number_of_biodiversite_causes_no_hrfp > 0
                    ,Keywords.number_of_biodiversite_consequences_no_hrfp > 0
                    ,Keywords.number_of_biodiversite_solutions_no_hrfp > 0
                )
            )

        if len(keywords_to_includes) > 0:
            logging.info(f"Filtering plaintext that include {len(keywords_to_includes)} keywords")

            statement = statement.filter(
                or_(*[Keywords.plaintext.ilike(f"%{word}%") for word in keywords_to_includes])
            )

        return session.execute(statement).scalar()

def update_keyword_row(session: Session, 
                       keyword_id: int,
                        new_number_of_keywords: int,
                        new_keywords_with_timestamp: List[dict],
                        matching_themes: List[str],
                        number_of_changement_climatique_constat: int,
                        number_of_changement_climatique_causes_directes: int,
                        number_of_changement_climatique_consequences: int,
                        number_of_attenuation_climatique_solutions_directes: int,
                        number_of_adaptation_climatique_solutions_directes: int,
                        number_of_ressources: int,
                        number_of_ressources_solutions: int,
                        number_of_biodiversite_concepts_generaux: int,
                        number_of_biodiversite_causes_directes: int,
                        number_of_biodiversite_consequences: int,
                        number_of_biodiversite_solutions_directes: int,
                        channel_title: str
                        ,number_of_keywords_climat: int
                        ,number_of_keywords_biodiversite: int
                        ,number_of_keywords_ressources: int
                        ,number_of_changement_climatique_constat_no_hrfp: int,
                        number_of_changement_climatique_causes_no_hrfp: int,
                        number_of_changement_climatique_consequences_no_hrfp: int,
                        number_of_attenuation_climatique_solutions_no_hrfp: int,
                        number_of_adaptation_climatique_solutions_no_hrfp: int,
                        number_of_ressources_no_hrfp: int,
                        number_of_ressources_solutions_no_hrfp: int,
                        number_of_biodiversite_concepts_generaux_no_hrfp: int,
                        number_of_biodiversite_causes_no_hrfp: int,
                        number_of_biodiversite_consequences_no_hrfp: int,
                        number_of_biodiversite_solutions_no_hrfp: int
    ):
    if matching_themes is not None:
        session.query(Keywords).filter(Keywords.id == keyword_id).update(
            {
                Keywords.number_of_keywords: new_number_of_keywords,
                Keywords.keywords_with_timestamp: new_keywords_with_timestamp,
                Keywords.theme: matching_themes,
                Keywords.number_of_changement_climatique_constat:number_of_changement_climatique_constat ,
                Keywords.number_of_changement_climatique_causes_directes:number_of_changement_climatique_causes_directes ,
                Keywords.number_of_changement_climatique_consequences:number_of_changement_climatique_consequences ,
                Keywords.number_of_attenuation_climatique_solutions_directes:number_of_attenuation_climatique_solutions_directes ,
                Keywords.number_of_adaptation_climatique_solutions_directes:number_of_adaptation_climatique_solutions_directes ,
                Keywords.number_of_ressources:number_of_ressources,
                Keywords.number_of_ressources_solutions:number_of_ressources_solutions ,
                Keywords.number_of_biodiversite_concepts_generaux:number_of_biodiversite_concepts_generaux ,
                Keywords.number_of_biodiversite_causes_directes:number_of_biodiversite_causes_directes ,
                Keywords.number_of_biodiversite_consequences:number_of_biodiversite_consequences ,
                Keywords.number_of_biodiversite_solutions_directes:number_of_biodiversite_solutions_directes,
                Keywords.channel_title: channel_title
                ,Keywords.number_of_keywords_climat: number_of_keywords_climat
                ,Keywords.number_of_keywords_biodiversite: number_of_keywords_biodiversite
                ,Keywords.number_of_keywords_ressources: number_of_keywords_ressources
                ,Keywords.number_of_changement_climatique_constat_no_hrfp:number_of_changement_climatique_constat_no_hrfp ,
                Keywords.number_of_changement_climatique_causes_no_hrfp:number_of_changement_climatique_causes_no_hrfp ,
                Keywords.number_of_changement_climatique_consequences_no_hrfp:number_of_changement_climatique_consequences_no_hrfp ,
                Keywords.number_of_attenuation_climatique_solutions_no_hrfp:number_of_attenuation_climatique_solutions_no_hrfp ,
                Keywords.number_of_adaptation_climatique_solutions_no_hrfp:number_of_adaptation_climatique_solutions_no_hrfp ,
                Keywords.number_of_ressources_no_hrfp:number_of_ressources_no_hrfp,
                Keywords.number_of_ressources_solutions_no_hrfp:number_of_ressources_solutions_no_hrfp ,
                Keywords.number_of_biodiversite_concepts_generaux_no_hrfp:number_of_biodiversite_concepts_generaux_no_hrfp ,
                Keywords.number_of_biodiversite_causes_no_hrfp:number_of_biodiversite_causes_no_hrfp ,
                Keywords.number_of_biodiversite_consequences_no_hrfp:number_of_biodiversite_consequences_no_hrfp ,
                Keywords.number_of_biodiversite_solutions_no_hrfp:number_of_biodiversite_solutions_no_hrfp,
            },
            synchronize_session=False
        )
    else:
        logging.warning(f"Matching themes is empty - deleting row {keyword_id}")
        delete_keywords_id(session, keyword_id)

def delete_keywords_id(session, id):
    session.query(Keywords).filter(Keywords.id == id).delete()
    session.commit()

def update_keyword_row_program(session: Session, 
                       keyword_id: int,
                        channel_program: str,
                        channel_program_type: str,
                        channel_title: str,
                        program_metadata_id: str):
    try:
        session.query(Keywords).filter(Keywords.id == keyword_id).update(
            {
                Keywords.channel_program: channel_program,
                Keywords.channel_program_type: channel_program_type,
                Keywords.channel_title: channel_title,
                Keywords.program_metadata_id: program_metadata_id,
            },
            synchronize_session=False
        )
    except Exception as err:
        logging.error(f"update_keyword_row_program error for k_id {keyword_id} {channel_title} {channel_program} program metadata id {program_metadata_id} : {err}")
        raise Exception