### Library imports
import requests
import modin.pandas as pd

import logging

from sqlalchemy.orm import Session
from postgres.schemas.models import Keywords
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import get_programs, get_a_program_with_start_timestamp, get_channel_title_for_name
from sqlalchemy import func, select, delete

def update_keywords(session: Session, batch_size: int = 50000, start_offset : int = 0, program_only=False, number_of_batch: int = 4) -> list:
    total_updates = get_total_count_saved_keywords(session)
    until_offset = start_offset + (number_of_batch * batch_size)
    if(until_offset > total_updates):
        logging.info(f"Until offset ({until_offset}) too high max ={total_updates}, using max instead - change number_of_batch env variable if needed")
        until_offset = total_updates
    
    logging.info(f"Updating {total_updates} saved keywords from {start_offset} offsets - batch size {batch_size} - until offset {until_offset}")
    df_programs = get_programs()
    logging.debug("Got channel programs")
    for i in range(start_offset, until_offset, batch_size):
        current_batch_saved_keywords = get_keywords_columns(session, i, batch_size)
        logging.info(f"Updating {len(current_batch_saved_keywords)} elements from {i} offsets - batch size {batch_size} - until offset {until_offset}")
        for keyword_id, plaintext, keywords_with_timestamp, number_of_keywords, start, srt, theme, channel_name, channel_title in current_batch_saved_keywords:
            if channel_title is None:
                logging.debug("channel_title none, set it using channel_name")
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
                    ,new_number_of_keywords_20 \
                    ,new_number_of_keywords_30 \
                    ,new_number_of_keywords_40 = get_themes_keywords_duration(plaintext, srt, start)
                except Exception as err:
                        logging.error(f"continuing loop but met error : {err}")
                        continue
                
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
                ,channel_title=channel_title
                ,number_of_keywords_20=new_number_of_keywords_20
                ,number_of_keywords_30=new_number_of_keywords_30
                ,number_of_keywords_40=new_number_of_keywords_40
                )
            else:
                program_name, program_name_type = get_a_program_with_start_timestamp(df_programs, pd.Timestamp(start).tz_convert('Europe/Paris'), channel_name)
                update_keyword_row_program(session
                ,keyword_id
                ,channel_program=program_name
                ,channel_program_type=program_name_type
                )
        logging.info(f"bulk update done {i} out of {until_offset} - (max offset {total_updates})")
        session.commit()

    logging.info("updated all keywords")


def get_keywords_columns(session: Session, page: int = 0, batch_size: int = 50000) -> list:
    logging.debug(f"Getting {batch_size} elements from offset {page}")
    return (
        session.query(
            Keywords.id,
            Keywords.plaintext,
            Keywords.keywords_with_timestamp,
            Keywords.number_of_keywords,
            func.timezone('UTC', Keywords.start).label('start'),
            Keywords.srt,
            Keywords.theme,
            Keywords.channel_name,
            Keywords.channel_title,
        )
        .offset(page)
        .limit(batch_size)
        .all()
    )

def get_total_count_saved_keywords(session: Session) -> int:
        statement = select(func.count()).select_from(Keywords)
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
                        ,number_of_keywords_20: int
                        ,number_of_keywords_30: int
                        ,number_of_keywords_40: int
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
                ,Keywords.number_of_keywords_20: number_of_keywords_20
                ,Keywords.number_of_keywords_30: number_of_keywords_30
                ,Keywords.number_of_keywords_40: number_of_keywords_40
            },
            synchronize_session=False
        )
    else:
        logging.warning(f"Matching themes is empty - deleting row {keyword_id}")
        session.query(Keywords).filter(Keywords.id == keyword_id).delete()

def update_keyword_row_program(session: Session, 
                       keyword_id: int,
                        channel_program: str,
                        channel_program_type: str):
    session.query(Keywords).filter(Keywords.id == keyword_id).update(
        {
            Keywords.channel_program: channel_program,
            Keywords.channel_program_type: channel_program_type
        },
        synchronize_session=False
    )