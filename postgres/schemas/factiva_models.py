"""
Factiva models - Print media data models
Separate from audiovisual models to maintain independent migration histories
"""
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    text,
)
from sqlalchemy.orm import declarative_base

# Separate base for Factiva models to isolate migrations
FactivaBase = declarative_base()

# Table names
source_classification_table = "source_classification"
factiva_articles_table = "factiva_articles"
stats_factiva_articles_table = "stats_factiva_articles"


class Source_Classification(FactivaBase):
    __tablename__ = source_classification_table
    
    source_type = Column(String, nullable=False)  # PQN, PQR, Magazine, Web
    source_name = Column(String, nullable=False)  # Le Monde, Le Figaro, etc.
    source_code = Column(String, primary_key=True)  # LEMOND, FIGARO, etc.
    source_owner = Column(String, nullable=True)  # LVMH, Dassault, etc.


class Factiva_Article(FactivaBase):
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

    # Aggregated counts for ALL crises combined - non HRFP (unique keywords across all crises)
    number_of_crises_no_hrfp = Column(Integer, nullable=True)
    crises_keywords = Column(JSON, nullable=True)  # All unique keywords from all crises (non-HRFP)
    
    # Aggregated counts for ALL crises combined - HRFP (unique keywords across all crises)
    number_of_crises_hrfp = Column(Integer, nullable=True)
    crises_keywords_hrfp = Column(JSON, nullable=True)  # All unique keywords from all crises (HRFP)

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
    
    # Duplicate detection status
    # - "NOT_DUP": Article is not a duplicate
    # - "DUP_UNIQUE_VERSION": The unique version to keep among duplicates (most recent modification_datetime)
    # - "DUP": A duplicate article (should be excluded from analysis)
    duplicate_status = Column(String, nullable=True, default="NOT_DUP")
    
    # Prediction flags - Pre-calculated crisis and causal link predictions
    # These flags are calculated based on keyword scores, HRFP multipliers, article length segments,
    # and threshold comparisons (same logic as in dbt print_media_crises_indicators)
    # Updated on every processing run (not just for new articles)
    
    # Global crisis predictions (aggregated)
    predict_at_least_one_crise = Column(Boolean, nullable=True)
    predict_climat = Column(Boolean, nullable=True)
    predict_biodiversite = Column(Boolean, nullable=True)
    predict_ressources = Column(Boolean, nullable=True)
    
    # Climate causal link predictions
    predict_climat_constat = Column(Boolean, nullable=True)
    predict_climat_cause = Column(Boolean, nullable=True)
    predict_climat_consequence = Column(Boolean, nullable=True)
    predict_climat_solution = Column(Boolean, nullable=True)
    
    # Biodiversity causal link predictions
    predict_biodiversite_constat = Column(Boolean, nullable=True)
    predict_biodiversite_cause = Column(Boolean, nullable=True)
    predict_biodiversite_consequence = Column(Boolean, nullable=True)
    predict_biodiversite_solution = Column(Boolean, nullable=True)
    
    # Resource causal link predictions
    predict_ressources_constat = Column(Boolean, nullable=True)
    predict_ressources_solution = Column(Boolean, nullable=True)


class Stats_Factiva_Article(FactivaBase):
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


class Dictionary(FactivaBase):
    """Climate keywords dictionary for Factiva environment"""
    __tablename__ = "dictionary"

    keyword = Column(String, nullable=False)
    high_risk_of_false_positive = Column(Boolean, nullable=True, default=True)
    category = Column(String, nullable=False)  # example "Concepts généraux" - can be empty string
    theme = Column(String, nullable=False)  # the actual "changement_climatique_constat"
    language = Column(String, nullable=False)  # all translation of the original keyword
    
    __table_args__ = (
        PrimaryKeyConstraint(
            "keyword",
            "language",
            "category",
            "theme",
            name="pk_keyword_language_category_theme",
        ),
    )


class Keyword_Macro_Category(FactivaBase):
    """Keyword macro categories for Factiva environment"""
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


def get_factiva_article(an: str):
    """Get Factiva article by accession number"""
    from postgres.database_connection import get_db_session
    session = get_db_session()
    return session.get(Factiva_Article, an)


def get_stats_factiva_article(source_code: str, publication_datetime: datetime):
    """Get Factiva article statistics"""
    from postgres.database_connection import get_db_session
    session = get_db_session()
    return session.get(Stats_Factiva_Article, (source_code, publication_datetime))


def create_factiva_tables(conn=None):
    """Create only the Factiva tables in the PostgreSQL database (lightweight for Factiva job)"""
    import logging

    from postgres.database_connection import connect_to_db
    
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


