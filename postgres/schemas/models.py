import logging
from datetime import datetime

from sqlalchemy import Column, DateTime, String, Text, Boolean, ARRAY, JSON, Integer, Table, MetaData, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import pandas as pd
from sqlalchemy import text
from postgres.database_connection import connect_to_db, get_db_session
import os
import json

Base = declarative_base()


def get_sitemap_cols():

    cols = [
        "publication_name",
        "news_title",
        "download_date",
        "news_publication_date",
        "news_keywords",
        "section",
        "image_caption",
        "media_type",
        "url",
        "news_description",
        "id",
    ]
    return cols


sitemap_table = "sitemap_table"
# ALTER TABLE keywords_new_list
# RENAME TO keywords; 
keywords_table = "keywords"
channel_metadata_table = "channel_metadata"
program_metadata_table = "program_metadata"
stop_word_table = "stop_word"

class Sitemap(Base):
    __tablename__ = sitemap_table

    id = Column(Text, primary_key=True)
    publication_name = Column(String, nullable=False)
    news_title = Column(Text, nullable=False)
    download_date = Column(DateTime(), default=datetime.now)
    news_publication_date = Column(DateTime(), default=datetime.now)
    news_keywords = Column(Text)
    section = Column(Text)
    image_caption = Column(Text)
    media_type = Column(Text)
    url = Column(Text)
    news_description= Column(Text) # ALTER TABLE sitemap_table add news_description text;
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

class Keywords(Base):
    __tablename__ = keywords_table

    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_title = Column(String, nullable=True)
    channel_program = Column(String, nullable=True) #  arcom - alembic handles this
    channel_program_type = Column(String, nullable=True) # arcom - (magazine, journal etc) alembic handles this
    channel_radio = Column(Boolean, nullable=True)
    start = Column(DateTime())
    plaintext= Column(Text)
    theme=Column(JSON) #keyword.py  # ALTER TABLE keywords ALTER theme TYPE json USING to_json(theme);
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')")) # ALTER TABLE ONLY keywords ALTER COLUMN created_at SET DEFAULT (now() at time zone 'utc');
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)
    keywords_with_timestamp = Column(JSON) # ALTER TABLE keywords ADD keywords_with_timestamp json;
    number_of_keywords = Column(Integer) # ALTER TABLE keywords ADD number_of_keywords integer;
    srt = Column(JSON) # ALTER TABLE keywords ADD srt json;
    number_of_changement_climatique_constat= Column(Integer)  # ALTER TABLE keywords ADD number_of_changement_climatique_constat integer;
    number_of_changement_climatique_causes_directes= Column(Integer)  # ALTER TABLE keywords ADD number_of_changement_climatique_causes_directes integer;
    number_of_changement_climatique_consequences= Column(Integer)  # ALTER TABLE keywords ADD number_of_changement_climatique_consequences integer;
    number_of_attenuation_climatique_solutions_directes= Column(Integer)  # ALTER TABLE keywords ADD number_of_attenuation_climatique_solutions_directes integer;
    number_of_adaptation_climatique_solutions_directes= Column(Integer)  # ALTER TABLE keywords ADD number_of_adaptation_climatique_solutions_directes integer;
    number_of_ressources= Column(Integer)  # ALTER TABLE keywords ADD number_of_ressources_naturelles_concepts_generaux integer;
    number_of_ressources_solutions= Column(Integer)  # ALTER TABLE keywords ADD number_of_ressources_solutions integer;
    number_of_biodiversite_concepts_generaux= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_concepts_generaux integer;
    number_of_biodiversite_causes_directes= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_causes_directes integer;
    number_of_biodiversite_consequences= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_consequences integer;
    number_of_biodiversite_solutions_directes= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_solutions_directes integer;
    number_of_keywords_20 = Column(Integer) # NOT USED ANYMORE -- ALTER TABLE keywords ADD number_of_keywords_20 integer;
    number_of_keywords_30 = Column(Integer) # NOT USED ANYMORE -- ALTER TABLE keywords ADD number_of_keywords_30 integer;
    number_of_keywords_40 = Column(Integer) # NOT USED ANYMORE -- ALTER TABLE keywords ADD number_of_keywords_40 integer;
    number_of_keywords_climat = Column(Integer) # sum of all climatique counters without duplicate (like number_of_keywords)
    number_of_keywords_biodiversite = Column(Integer) # sum of all biodiversite counters without duplicate
    number_of_keywords_ressources = Column(Integer) # sum of all ressources counters without duplicate
    number_of_changement_climatique_constat_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_changement_climatique_constat integer;
    number_of_changement_climatique_causes_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_changement_climatique_causes_directes integer;
    number_of_changement_climatique_consequences_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_changement_climatique_consequences integer;
    number_of_attenuation_climatique_solutions_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_attenuation_climatique_solutions_directes integer;
    number_of_adaptation_climatique_solutions_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_adaptation_climatique_solutions_directes integer;
    number_of_ressources_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_ressources_naturelles_concepts_generaux integer;
    number_of_ressources_solutions_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_ressources_solutions integer;
    number_of_biodiversite_concepts_generaux_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_concepts_generaux integer;
    number_of_biodiversite_causes_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_causes_directes integer;
    number_of_biodiversite_consequences_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_consequences integer;
    number_of_biodiversite_solutions_no_hrfp= Column(Integer)  # ALTER TABLE keywords ADD number_of_biodiversite_solutions_directes integer;

class Channel_Metadata(Base):
    __tablename__ = channel_metadata_table
    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_title = Column(String, nullable=False)
    duration_minutes= Column(Integer)
    weekday= Column(Integer)  


class Program_Metadata(Base):
    __tablename__ = program_metadata_table
    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_title = Column(String, nullable=False)
    duration_minutes= Column(Integer)
    weekday= Column(Integer)
    start= Column(String, nullable=False)
    end= Column(String, nullable=False)
    channel_program= Column(String, nullable=False)
    channel_program_type= Column(String, nullable=False)
    public = Column(Boolean, nullable=True)
    infocontinue = Column(Boolean, nullable=True)
    radio = Column(Boolean, nullable=True)
    program_grid_start = Column(DateTime(), nullable=True)
    program_grid_end = Column(DateTime(), nullable=True)

class Stop_Word(Base):
    __tablename__ = stop_word_table
    id = Column(Text, primary_key=True)
    keyword_id = Column(Text, nullable=True)
    channel_title = Column(String, nullable=True)
    context = Column(String, nullable=False)
    count = Column(Integer, nullable=True)
    keyword = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("(now() at time zone 'utc')"))
    start_date = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(), default=datetime.now, onupdate=text("now() at time zone 'Europe/Paris'"), nullable=True)
    validated = Column(Boolean, nullable=True, default=True)

def get_sitemap(id: str):
    session = get_db_session()
    return session.get(Sitemap, id)

def get_keyword(id: str, session = None):
    if session is None:
        session = get_db_session()
    return session.get(Keywords, id)

def get_stop_word(id: str):
    session = get_db_session()
    return session.get(Stop_Word, id)

def get_last_month_sitemap_id(engine): 
    query = text("""
    SELECT id 
    FROM sitemap_table 
    WHERE download_date >= (current_date - interval '1 month'); 
    """)
    with engine.begin() as conn:
        df = pd.read_sql_query(query, conn)
        return df

def create_tables(conn=None):
    """Create tables in the PostgreSQL database"""
    logging.info("create sitemap, keywords , stop_word tables - update channel_metadata")
    try:
        if conn is None :
            engine = connect_to_db()
        else:
            engine = conn

        Base.metadata.create_all(engine, checkfirst=True)
        update_channel_metadata(engine)
        if(os.environ.get("UPDATE") != "true"):
            update_program_metadata(engine)
        else:
            logging.warning("No program update as UPDATE=true as it can create lock issues")
        logging.info("Table creation done, if not already done.")
    except (Exception) as error:
        logging.error(error)
    finally:
        if engine is not None:
            engine.dispose()

def update_channel_metadata(engine):
    logging.info("Update channel metadata")
    Session = sessionmaker(bind=engine)
    session = Session()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '..', 'channel_metadata.json')
    with open(json_file_path, 'r') as f:
        data = json.load(f)
        
        for item in data:
            metadata = {
                'id': item['ID'],
                'channel_name': item['Channel Name'],
                'channel_title': item['Channel Title'],
                'duration_minutes': int(item['Duration Minutes']),
                'weekday': int(item['Weekday'])
            }
            session.merge(Channel_Metadata(**metadata))
        
        # Commit all changes at once after processing all items
        session.commit()
        logging.info("Updated channel metadata")

def update_program_metadata(engine):
    logging.info("Update program metadata")
    Session = sessionmaker(bind=engine)
    session = Session()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, '..', 'program_metadata.json')
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
            
            # full overwrite
            logging.warning("Program_Metadata table! Full overwrite (delete/recreate)")
            session.query(Program_Metadata).delete()
            session.commit()

            for item in data:
                metadata = {
                    'id': item['id'],
                    'channel_name': item['channel_name'],
                    'channel_title': item['channel_title'],
                    'infocontinue': item['infocontinue'],
                    'public': item['public'],
                    'radio': item['radio'],
                    'duration_minutes': int(item['duration']),
                    'weekday': int(item['weekday']),
                    'channel_program': item['program_name'],
                    'channel_program_type': item['program_type'],
                    'start': item['start'],
                    'end': item['end'],
                    'program_grid_start': datetime.strptime(item['program_grid_start'], '%Y-%m-%d'),
                    'program_grid_end': datetime.strptime(item['program_grid_end'], '%Y-%m-%d'),
                }
                session.merge(Program_Metadata(**metadata))
            
            # Commit all changes at once after processing all items
            session.commit()
            logging.info("Updated program metadata")
    except (Exception) as error:
        logging.error(f"Error : Update program metadata {error}")

def empty_tables(session = None, stop_word = True):
    if(os.environ.get("POSTGRES_HOST") == "postgres_db" or os.environ.get("POSTGRES_HOST") == "localhost"):
        logging.warning("""Doing: Empty table Stop_Word / Keywords""")
        if stop_word:
            session.query(Stop_Word).delete()
        session.query(Keywords).delete()
        session.commit()
        logging.warning("""Done: Empty table Stop_Word / Keywords""")


def drop_tables(conn = None):
    
    if(os.environ.get("POSTGRES_HOST") == "postgres_db" or os.environ.get("POSTGRES_HOST") == "localhost"):
        logging.warning("""Drop table keyword / Program_Metadata / Channel_Metadata in the PostgreSQL database""")
        try:
            if conn is None :
                engine = connect_to_db()
            else:
                engine = conn
            Base.metadata.drop_all(bind=engine, tables=[Keywords.__table__])
            Base.metadata.drop_all(bind=engine, tables=[Channel_Metadata.__table__])
            Base.metadata.drop_all(bind=engine, tables=[Program_Metadata.__table__])
            Base.metadata.drop_all(bind=engine, tables=[Stop_Word.__table__])

            logging.info(f"Table keyword / Program_Metadata / Channel_Metadata deletion done")
        except (Exception) as error:
            logging.error(error)
        finally:
            if engine is not None:
                engine.dispose()