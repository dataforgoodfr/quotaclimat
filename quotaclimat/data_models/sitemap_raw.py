from datetime import datetime
from xmlrpc.client import Boolean

import pandas as pd
import pandera as pa
from pandera import Field
from pandera.typing import Index as IndexType
from pandera.typing import Series



class SitemapRaw(pa.SchemaModel):
    url: Series[str] = Field(nullable=False)
    publication_name: Series[str] = Field(nullable=False)
    news_publication_date: Series[datetime] = Field(nullable=False)
    news_title: Series[str] = Field(nullable=False)
    image_caption: Series[str] = Field(nullable=True)
    download_date: Series[datetime] = Field(nullable=False)
    section: Series[object] = Field(nullable=True)
    #TODO: convert df['lastmod'] = pd.to_datetime(df.lastmod).dt.tz_localize(None) in historical data
    # lastmod: Series[datetime] = Field(nullable=True)

    @pa.dataframe_check
    def _has_at_least_one_row(cls, df):
        return len(df) > 0
    