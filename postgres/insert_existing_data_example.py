import logging
import os

import pandas as pd

from postgres.schemas.models import get_sitemap_cols
from quotaclimat.data_processing.sitemap.sitemap_processing import load_all

def parse_section(section: str):
    logging.debug(section)
    if "," not in section:
        return section
    else:
        return ",".join(map(str, section))

def transformation_from_dumps_to_table_entry(df: pd.DataFrame):
    try:
        cols = get_sitemap_cols()
        df_template_db = pd.DataFrame(columns=cols)
        df_consistent = pd.concat([df, df_template_db])

        df_consistent.section = df_consistent.section.apply(parse_section)

        return df_consistent[cols]
    except Exception as err:
        logging.error("Could not transform %s" % (err))
        return None