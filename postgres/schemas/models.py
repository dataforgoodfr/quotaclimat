import json
import logging
import os
from datetime import datetime
from json import JSONDecodeError

import pandas as pd
from sqlalchemy import (
    ARRAY,
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Double,
    ForeignKey,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    Uuid,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from postgres.database_connection import connect_to_db, get_db_session
from postgres.schemas.base import Base
from quotaclimat.data_processing.mediatree.i8n.country import FRANCE
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.keyword.macro_category import (
    MACRO_CATEGORIES,
)
from quotaclimat.data_processing.mediatree.time_monitored.models import Time_Monitored

sitemap_table = "sitemap_table"
# ALTER TABLE keywords_new_list
# RENAME TO keywords;
keywords_table = "keywords"
channel_metadata_table = "channel_metadata"
program_metadata_table = "program_metadata"
stop_word_table = "stop_word"
factiva_articles_table = "factiva_articles"
stats_factiva_articles_table = "stats_factiva_articles"


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
    news_description = Column(
        Text
    )  # ALTER TABLE sitemap_table add news_description text;
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)


class Program_Metadata(Base):
    __tablename__ = program_metadata_table
    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_title = Column(String, nullable=False)
    duration_minutes = Column(Integer)
    weekday = Column(Integer)
    start = Column(String, nullable=False)
    end = Column(String, nullable=False)
    channel_program = Column(String, nullable=False)
    channel_program_type = Column(String, nullable=False)
    public = Column(Boolean, nullable=True)
    infocontinue = Column(Boolean, nullable=True)
    radio = Column(Boolean, nullable=True)
    program_grid_start = Column(DateTime(), nullable=True)
    program_grid_end = Column(DateTime(), nullable=True)
    country = Column(Text, nullable=True, default=FRANCE.name)
    created_at = Column(
        DateTime(timezone=True),
        server_default=text("(now() at time zone 'utc')"),
        nullable=True,
    )
    updated_at = Column(
        DateTime(),
        default=datetime.now,
        onupdate=text("now() at time zone 'Europe/Paris'"),
        nullable=True,
    )


class Keywords(Base):
    __tablename__ = keywords_table

    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_title = Column(String, nullable=True)
    channel_program = Column(String, nullable=True)  #  arcom - alembic handles this
    channel_program_type = Column(
        String, nullable=True
    )  # arcom - (magazine, journal etc) alembic handles this
    channel_radio = Column(Boolean, nullable=True)
    start = Column(DateTime(), primary_key=True)
    plaintext = Column(Text)
    theme = Column(
        JSON
    )  # keyword.py  # ALTER TABLE keywords ALTER theme TYPE json USING to_json(theme);
    created_at = Column(
        DateTime(timezone=True), server_default=text("(now() at time zone 'utc')")
    )  # ALTER TABLE ONLY keywords ALTER COLUMN created_at SET DEFAULT (now() at time zone 'utc');
    updated_at = Column(
        DateTime(),
        default=datetime.now,
        onupdate=text("now() at time zone 'Europe/Paris'"),
        nullable=True,
    )
    keywords_with_timestamp = Column(
        JSON
    )  # ALTER TABLE keywords ADD keywords_with_timestamp json;
    number_of_keywords = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_keywords integer;
    srt = Column(JSON)  # ALTER TABLE keywords ADD srt json;
    number_of_changement_climatique_constat = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_changement_climatique_constat integer;
    number_of_changement_climatique_causes_directes = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_changement_climatique_causes_directes integer;
    number_of_changement_climatique_consequences = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_changement_climatique_consequences integer;
    number_of_attenuation_climatique_solutions_directes = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_attenuation_climatique_solutions_directes integer;
    number_of_adaptation_climatique_solutions_directes = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_adaptation_climatique_solutions_directes integer;
    number_of_ressources = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_ressources_naturelles_concepts_generaux integer;
    number_of_ressources_solutions = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_ressources_solutions integer;
    number_of_biodiversite_concepts_generaux = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_concepts_generaux integer;
    number_of_biodiversite_causes_directes = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_causes_directes integer;
    number_of_biodiversite_consequences = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_consequences integer;
    number_of_biodiversite_solutions_directes = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_solutions_directes integer;
    number_of_keywords_20 = Column(
        Integer
    )  # NOT USED ANYMORE -- ALTER TABLE keywords ADD number_of_keywords_20 integer;
    number_of_keywords_30 = Column(
        Integer
    )  # NOT USED ANYMORE -- ALTER TABLE keywords ADD number_of_keywords_30 integer;
    number_of_keywords_40 = Column(
        Integer
    )  # NOT USED ANYMORE -- ALTER TABLE keywords ADD number_of_keywords_40 integer;
    number_of_keywords_climat = Column(
        Integer
    )  # sum of all climatique counters without duplicate (like number_of_keywords)
    number_of_keywords_biodiversite = Column(
        Integer
    )  # sum of all biodiversite counters without duplicate
    number_of_keywords_ressources = Column(
        Integer
    )  # sum of all ressources counters without duplicate
    number_of_changement_climatique_constat_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_changement_climatique_constat integer;
    number_of_changement_climatique_causes_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_changement_climatique_causes_directes integer;
    number_of_changement_climatique_consequences_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_changement_climatique_consequences integer;
    number_of_attenuation_climatique_solutions_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_attenuation_climatique_solutions_directes integer;
    number_of_adaptation_climatique_solutions_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_adaptation_climatique_solutions_directes integer;
    number_of_ressources_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_ressources_naturelles_concepts_generaux integer;
    number_of_ressources_solutions_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_ressources_solutions integer;
    number_of_biodiversite_concepts_generaux_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_concepts_generaux integer;
    number_of_biodiversite_causes_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_causes_directes integer;
    number_of_biodiversite_consequences_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_consequences integer;
    number_of_biodiversite_solutions_no_hrfp = Column(
        Integer
    )  # ALTER TABLE keywords ADD number_of_biodiversite_solutions_directes integer;

    program_metadata_id = Column(Text, ForeignKey("program_metadata.id"), nullable=True)
    program_metadata = relationship(
        "Program_Metadata", foreign_keys=[program_metadata_id]
    )

    country = Column(Text, nullable=True, default=FRANCE.name)


class Channel_Metadata(Base):
    __tablename__ = channel_metadata_table
    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_title = Column(String, nullable=False)
    duration_minutes = Column(Integer)
    weekday = Column(Integer)


class Stop_Word(Base):
    __tablename__ = stop_word_table
    id = Column(Text, primary_key=True)
    keyword_id = Column(Text, nullable=True)
    channel_title = Column(String, nullable=True)
    context = Column(String, nullable=False)
    count = Column(Integer, nullable=True)
    keyword = Column(String, nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=text("(now() at time zone 'utc')")
    )
    start_date = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(
        DateTime(),
        default=datetime.now,
        onupdate=text("now() at time zone 'Europe/Paris'"),
        nullable=True,
    )
    validated = Column(Boolean, nullable=True, default=True)
    country = Column(Text, nullable=True, default=FRANCE.name)  # TODO PK for country


class Dictionary(Base):
    __tablename__ = "dictionary"

    keyword = Column(String, nullable=False)

    high_risk_of_false_positive = Column(Boolean, nullable=True, default=True)

    category = Column(
        String, nullable=False
    )  # example "Concepts généraux" - can be empty string
    theme = Column(String, nullable=False)  # the actual "changement_climatique_constat"

    # all translation of the original keyword
    language = Column(String, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint(
            "keyword",
            "language",
            "category",
            "theme",
            name="pk_keyword_language_category_theme",
        ),
    )


class Keyword_Macro_Category(Base):
    __tablename__ = "keyword_macro_category"
    keyword = Column(String, primary_key=True)  # linked to Dictionary.keyword
    is_empty = Column(Boolean, nullable=True, default=False)
    general = Column(Boolean, nullable=True, default=False)
    agriculture = Column(Boolean, nullable=True, default=False)
    transport = Column(Boolean, nullable=True, default=False)
    batiments = Column(Boolean, nullable=True, default=False)
    energie = Column(Boolean, nullable=True, default=False)
    industrie = Column(Boolean, nullable=True, default=False)
    eau = Column(Boolean, nullable=True, default=False)
    ecosysteme = Column(Boolean, nullable=True, default=False)
    economie_ressources = Column(Boolean, nullable=True, default=False)


class Factiva_Article(Base):
    __tablename__ = factiva_articles_table

    # Primary key - Accession Number (Unique document ID)
    an = Column(String, primary_key=True)

    # Event metadata
    document_type = Column(String, nullable=True)
    action = Column(String, nullable=True)  # add, rep, del
    event_type = Column(String, nullable=True)  # for bulk events like source_delete

    # Article content
    title = Column(Text, nullable=True)
    body = Column(Text, nullable=True)
    snippet = Column(Text, nullable=True)
    art = Column(Text, nullable=True)  # Caption text and descriptions

    # Author and attribution
    byline = Column(Text, nullable=True)
    credit = Column(Text, nullable=True)
    dateline = Column(Text, nullable=True)

    # Publication information
    source_code = Column(String, nullable=True)
    source_name = Column(Text, nullable=True)
    publisher_name = Column(Text, nullable=True)
    section = Column(Text, nullable=True)
    copyright = Column(Text, nullable=True)

    # Dates and timestamps
    publication_date = Column(DateTime(timezone=True), nullable=True)
    publication_datetime = Column(DateTime(timezone=True), nullable=True)
    modification_date = Column(DateTime(timezone=True), nullable=True)
    modification_datetime = Column(DateTime(timezone=True), nullable=True)
    ingestion_datetime = Column(DateTime(timezone=True), nullable=True)
    availability_datetime = Column(DateTime(timezone=True), nullable=True)

    # Language and region
    language_code = Column(String, nullable=True)
    region_of_origin = Column(String, nullable=True)

    # Metadata
    word_count = Column(Integer, nullable=True)

    # Codes - stored as comma-separated strings or JSON
    company_codes = Column(Text, nullable=True)
    company_codes_about = Column(Text, nullable=True)
    company_codes_association = Column(Text, nullable=True)
    company_codes_lineage = Column(Text, nullable=True)
    company_codes_occur = Column(Text, nullable=True)
    company_codes_relevance = Column(Text, nullable=True)
    subject_codes = Column(Text, nullable=True)
    region_codes = Column(Text, nullable=True)
    industry_codes = Column(Text, nullable=True)
    person_codes = Column(Text, nullable=True)
    currency_codes = Column(Text, nullable=True)
    market_index_codes = Column(Text, nullable=True)

    # Additional metadata (v2.44+)
    allow_translation = Column(Boolean, nullable=True)
    attrib_code = Column(String, nullable=True)
    authors = Column(JSON, nullable=True)  # Array of author names and IDs
    clusters = Column(JSON, nullable=True)  # Array of similar article IDs
    content_type_codes = Column(Text, nullable=True)
    footprint_company_codes = Column(Text, nullable=True)
    footprint_person_codes = Column(Text, nullable=True)
    industry_classification_benchmark_codes = Column(Text, nullable=True)
    newswires_codes = Column(Text, nullable=True)
    org_type_codes = Column(Text, nullable=True)
    pub_page = Column(String, nullable=True)
    restrictor_codes = Column(Text, nullable=True)

    # Internal tracking
    created_at = Column(
        DateTime(timezone=True), server_default=text("(now() at time zone 'utc')")
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.now,
        onupdate=text("now() at time zone 'Europe/Paris'"),
        nullable=True,
    )
    is_deleted = Column(Boolean, default=False, nullable=False)  # Soft delete flag

    # Keyword counts - non HRFP (high risk of false positive) - UNIQUE keywords only
    number_of_changement_climatique_constat_no_hrfp = Column(Integer, nullable=True)
    number_of_changement_climatique_causes_no_hrfp = Column(Integer, nullable=True)
    number_of_changement_climatique_consequences_no_hrfp = Column(Integer, nullable=True)
    number_of_attenuation_climatique_solutions_no_hrfp = Column(Integer, nullable=True)
    number_of_adaptation_climatique_solutions_no_hrfp = Column(Integer, nullable=True)
    number_of_changement_climatique_solutions_no_hrfp = Column(Integer, nullable=True)  # Combined solutions
    number_of_ressources_constat_no_hrfp = Column(Integer, nullable=True)
    number_of_ressources_solutions_no_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_concepts_generaux_no_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_causes_no_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_consequences_no_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_solutions_no_hrfp = Column(Integer, nullable=True)

    # Keyword counts - HRFP (high risk of false positive) - UNIQUE keywords only
    number_of_changement_climatique_constat_hrfp = Column(Integer, nullable=True)
    number_of_changement_climatique_causes_hrfp = Column(Integer, nullable=True)
    number_of_changement_climatique_consequences_hrfp = Column(Integer, nullable=True)
    number_of_attenuation_climatique_solutions_hrfp = Column(Integer, nullable=True)
    number_of_adaptation_climatique_solutions_hrfp = Column(Integer, nullable=True)
    number_of_changement_climatique_solutions_hrfp = Column(Integer, nullable=True)  # Combined solutions
    number_of_ressources_constat_hrfp = Column(Integer, nullable=True)
    number_of_ressources_solutions_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_concepts_generaux_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_causes_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_consequences_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_solutions_hrfp = Column(Integer, nullable=True)

    # Aggregated counts by crisis type - non HRFP (sum of causal links)
    number_of_climat_no_hrfp = Column(Integer, nullable=True)
    number_of_ressources_no_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_no_hrfp = Column(Integer, nullable=True)

    # Aggregated counts by crisis type - HRFP (sum of causal links)
    number_of_climat_hrfp = Column(Integer, nullable=True)
    number_of_ressources_hrfp = Column(Integer, nullable=True)
    number_of_biodiversite_hrfp = Column(Integer, nullable=True)

    # Keyword lists by causal link - non HRFP - JSON arrays with ALL occurrences (including duplicates)
    changement_climatique_constat_keywords = Column(JSON, nullable=True)
    changement_climatique_causes_keywords = Column(JSON, nullable=True)
    changement_climatique_consequences_keywords = Column(JSON, nullable=True)
    attenuation_climatique_solutions_keywords = Column(JSON, nullable=True)
    adaptation_climatique_solutions_keywords = Column(JSON, nullable=True)
    changement_climatique_solutions_keywords = Column(JSON, nullable=True)  # Combined solutions
    ressources_constat_keywords = Column(JSON, nullable=True)
    ressources_solutions_keywords = Column(JSON, nullable=True)
    biodiversite_concepts_generaux_keywords = Column(JSON, nullable=True)
    biodiversite_causes_keywords = Column(JSON, nullable=True)
    biodiversite_consequences_keywords = Column(JSON, nullable=True)
    biodiversite_solutions_keywords = Column(JSON, nullable=True)

    # Keyword lists by causal link - HRFP - JSON arrays with ALL occurrences (including duplicates)
    changement_climatique_constat_keywords_hrfp = Column(JSON, nullable=True)
    changement_climatique_causes_keywords_hrfp = Column(JSON, nullable=True)
    changement_climatique_consequences_keywords_hrfp = Column(JSON, nullable=True)
    attenuation_climatique_solutions_keywords_hrfp = Column(JSON, nullable=True)
    adaptation_climatique_solutions_keywords_hrfp = Column(JSON, nullable=True)
    changement_climatique_solutions_keywords_hrfp = Column(JSON, nullable=True)  # Combined solutions
    ressources_constat_keywords_hrfp = Column(JSON, nullable=True)
    ressources_solutions_keywords_hrfp = Column(JSON, nullable=True)
    biodiversite_concepts_generaux_keywords_hrfp = Column(JSON, nullable=True)
    biodiversite_causes_keywords_hrfp = Column(JSON, nullable=True)
    biodiversite_consequences_keywords_hrfp = Column(JSON, nullable=True)
    biodiversite_solutions_keywords_hrfp = Column(JSON, nullable=True)

    # All keywords with full metadata (keyword, theme, category, count_keyword, is_hrfp)
    all_keywords = Column(JSON, nullable=True)


class Stats_Factiva_Article(Base):
    __tablename__ = stats_factiva_articles_table

    # Composite primary key on source_code and publication_datetime
    source_code = Column(String, primary_key=True)
    publication_datetime = Column(DateTime(timezone=True), primary_key=True)

    # Article count
    count = Column(Integer, nullable=False)

    # Internal tracking
    created_at = Column(
        DateTime(timezone=True), server_default=text("(now() at time zone 'utc')")
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.now,
        onupdate=text("now() at time zone 'Europe/Paris'"),
        nullable=True,
    )


def get_sitemap(id: str):
    session = get_db_session()
    return session.get(Sitemap, id)


def get_keyword(id: str, session=None):
    if session is None:
        session = get_db_session()

    return session.query(Keywords).filter_by(id=id).one_or_none()


def get_stop_word(id: str):
    session = get_db_session()
    return session.get(Stop_Word, id)


def get_factiva_article(an: str):
    session = get_db_session()
    return session.get(Factiva_Article, an)


def get_stats_factiva_article(source_code: str, publication_datetime: datetime):
    session = get_db_session()
    return session.get(Stats_Factiva_Article, (source_code, publication_datetime))


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
    logging.info(
        "create sitemap, keywords , time_monitored, stop_word tables, dictionnary - update channel_metadata"
    )
    try:
        if conn is None:
            engine = connect_to_db()
        else:
            engine = conn

        Base.metadata.create_all(engine, checkfirst=True)
        update_channel_metadata(engine)
        update_dictionary(
            engine, theme_keywords=THEME_KEYWORDS, macro_categories=MACRO_CATEGORIES
        )

        if os.environ.get("UPDATE") != "true":
            update_program_metadata(engine)
        else:
            logging.warning(
                "No program update as UPDATE=true as it can create lock issues"
            )
        logging.info("Table creation done, if not already done.")
    except Exception as error:
        logging.error(error)
    finally:
        if engine is not None:
            engine.dispose()


def create_factiva_tables(conn=None):
    """Create only the Factiva tables in the PostgreSQL database (lightweight for Factiva job)"""
    logging.info("Creating factiva_articles table")
    try:
        if conn is None:
            engine = connect_to_db()
        else:
            engine = conn

        # Only create the factiva_articles table (not Mediatree tables)
        Factiva_Article.__table__.create(engine, checkfirst=True)

        logging.info("Factiva table creation done")
    except Exception as error:
        logging.error(f"Error creating Factiva tables: {error}")
    finally:
        if engine is not None:
            engine.dispose()


def update_channel_metadata(engine):
    logging.info("Update channel metadata")
    Session = sessionmaker(bind=engine)
    session = Session()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, "..", "channel_metadata.json")
    with open(json_file_path, "r") as f:
        data = json.load(f)

        for item in data:
            metadata = {
                "id": item["ID"],
                "channel_name": item["Channel Name"],
                "channel_title": item["Channel Title"],
                "duration_minutes": int(item["Duration Minutes"]),
                "weekday": int(item["Weekday"]),
            }
            session.merge(Channel_Metadata(**metadata))

        # Commit all changes at once after processing all items
        session.commit()
        logging.info("Updated channel metadata")


def update_program_metadata(engine):
    logging.info("Updating program metadata")
    Session = sessionmaker(bind=engine)
    session = Session()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, "..", "program_metadata.json")

    try:
        with open(json_file_path, "r") as f:
            data = json.load(f)

        for item in data:
            metadata = {
                "id": item["id"],
                "channel_name": item["channel_name"],
                "channel_title": item["channel_title"],
                "infocontinue": item["infocontinue"],
                "public": item["public"],
                "radio": item["radio"],
                "duration_minutes": int(item["duration"]),
                "weekday": int(item["weekday"]),
                "channel_program": item["program_name"],
                "channel_program_type": item["program_type"],
                "start": item["start"],
                "end": item["end"],
                "program_grid_start": datetime.strptime(
                    item["program_grid_start"], "%Y-%m-%d"
                ),
                "program_grid_end": datetime.strptime(
                    item["program_grid_end"], "%Y-%m-%d"
                ),
                "country": item.get("country", FRANCE.name),
            }

            # Check if the record exists
            existing_record = session.get(Program_Metadata, item["id"])
            if existing_record:
                for key, value in metadata.items():
                    setattr(existing_record, key, value)
            else:
                logging.warning(
                    f"New programs : {item['channel_title']} - {item['program_name']} - {item['id']}"
                )
                session.add(Program_Metadata(**metadata))

        session.commit()
        logging.info("Program metadata updated successfully")

    except (OSError, JSONDecodeError) as file_error:
        logging.error(f"Error reading JSON file: {file_error}")
        session.rollback()
    except SQLAlchemyError as db_error:
        logging.error(f"Database error while updating program metadata: {db_error}")
        session.rollback()
    except Exception as error:
        logging.error(f"Unexpected error while updating program metadata: {error}")
        session.rollback()
    finally:
        session.close()


def update_dictionary(engine, theme_keywords, macro_categories=MACRO_CATEGORIES):
    logging.info("Updating dictionary data")
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        seen = set()
        logging.warning(
            "Dictionary and Keyword_Macro_Category table! Full overwrite (delete/recreate)"
        )
        session.query(Dictionary).delete()
        session.query(Keyword_Macro_Category).delete()
        session.commit()
        logging.info(
            "Deleted all entries in the dictionary and Keyword_Macro_Category table"
        )

        bulk_data = []
        for theme, keywords_list in theme_keywords.items():
            for item in keywords_list:
                entry_tuple = (
                    item["keyword"],
                    item.get("language"),
                    item.get("category", ""),
                    theme,
                )
                if entry_tuple not in seen:
                    seen.add(entry_tuple)
                    bulk_data.append(
                        {
                            "keyword": item["keyword"],
                            "language": item.get("language"),
                            "category": item.get("category", ""),
                            "theme": theme,
                            "high_risk_of_false_positive": item.get(
                                "high_risk_of_false_positive", False
                            ),
                        }
                    )
        session.bulk_insert_mappings(Dictionary, bulk_data)
        session.commit()
        logging.info(f"Inserted {len(bulk_data)} dictionary records successfully")

        # insert Keyword_Macro_Category
        logging.info(
            f"Inserting {len(macro_categories)} Keyword_Macro_Category data..."
        )
        session.bulk_insert_mappings(Keyword_Macro_Category, macro_categories)
        session.commit()
        logging.info(
            f"Inserted {len(macro_categories)} Keyword_Macro_Category records successfully"
        )
    except Exception as error:
        logging.error(f"Error updating dictionary data: {error}")
        session.rollback()
    finally:
        session.close()


def empty_tables(session=None, stop_word=True):
    if (
        os.environ.get("POSTGRES_HOST") == "postgres_db"
        or os.environ.get("POSTGRES_HOST") == "localhost"
    ) and os.environ.get("ENV") != "prod":
        logging.warning("""Doing: Empty table Stop_Word / Keywords""")
        if stop_word:
            session.query(Stop_Word).delete()
        session.query(Keywords).delete()

        session.commit()
        logging.warning("""Done: Empty table Stop_Word / Keywords""")


def drop_tables(conn=None):
    if (
        os.environ.get("POSTGRES_HOST") == "postgres_db"
        or os.environ.get("POSTGRES_HOST") == "localhost"
    ) and os.environ.get("ENV") != "prod":
        logging.warning(
            """Drop table keyword / Program_Metadata / Channel_Metadata in the PostgreSQL database"""
        )
        try:
            if conn is None:
                engine = connect_to_db()
            else:
                engine = conn
            logging.info(f"Drop all {Keywords.__tablename__}")
            Base.metadata.drop_all(bind=engine, tables=[Keywords.__table__])
            logging.info(f"Drop all {Channel_Metadata.__tablename__}")
            Base.metadata.drop_all(bind=engine, tables=[Channel_Metadata.__table__])
            logging.info(f"Drop all {Program_Metadata.__tablename__}")
            Base.metadata.drop_all(bind=engine, tables=[Program_Metadata.__table__])
            logging.info(f"Drop all {Stop_Word.__tablename__}")
            Base.metadata.drop_all(bind=engine, tables=[Stop_Word.__table__])
            logging.info(f"Drop all {Dictionary.__tablename__}")
            Base.metadata.drop_all(bind=engine, tables=[Dictionary.__table__])
            logging.info(f"Drop all {Time_Monitored.__tablename__}")
            Base.metadata.drop_all(bind=engine, tables=[Time_Monitored.__table__])

            logging.info(
                f"Table keyword / Program_Metadata / Channel_Metadata deletion done"
            )
        except Exception as error:
            logging.error(error)
        finally:
            if engine is not None:
                engine.dispose()
