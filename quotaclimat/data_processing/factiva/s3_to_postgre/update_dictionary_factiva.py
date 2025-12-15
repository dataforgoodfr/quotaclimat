"""Update Dictionary and Keyword_Macro_Category tables for Factiva job."""

import logging

from sqlalchemy.orm import sessionmaker

from postgres.schemas.models import Dictionary, Keyword_Macro_Category
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.keyword.macro_category import (
    MACRO_CATEGORIES,
)


def update_dictionary_factiva(engine, theme_keywords=THEME_KEYWORDS, macro_categories=MACRO_CATEGORIES):
    """
    Update Dictionary and Keyword_Macro_Category tables for Factiva job.
    
    Args:
        engine: SQLAlchemy engine
        theme_keywords: Dictionary of theme keywords (default: THEME_KEYWORDS)
        macro_categories: List of macro category dictionaries (default: MACRO_CATEGORIES)
    """
    logging.info("Updating dictionary and keyword_macro_category tables for Factiva job")
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        seen = set()
        logging.warning(
            "Dictionary and Keyword_Macro_Category tables! Full overwrite (delete/recreate)"
        )
        session.query(Dictionary).delete()
        session.query(Keyword_Macro_Category).delete()
        session.commit()
        logging.info(
            "Deleted all entries in the dictionary and Keyword_Macro_Category tables"
        )

        # Insert Dictionary data
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

        # Insert Keyword_Macro_Category data
        logging.info(
            f"Inserting {len(macro_categories)} Keyword_Macro_Category records..."
        )
        session.bulk_insert_mappings(Keyword_Macro_Category, macro_categories)
        session.commit()
        logging.info(
            f"Inserted {len(macro_categories)} Keyword_Macro_Category records successfully"
        )
    except Exception as error:
        logging.error(f"Error updating dictionary data for Factiva job: {error}")
        session.rollback()
        raise
    finally:
        session.close()
