from datetime import datetime
from xmlrpc.client import Boolean

import pandas as pd
import pandera as pa
from pandera import Field
from pandera.typing import Index as IndexType
from pandera.typing import Series

# TODO: 'START CHUNK', 'END CHUNK' are missing because the formatting is not conform to what python supports


class MediatreeDataImport(pa.SchemaModel):
    CHANNEL: Series[str] = Field(nullable=False)
    RADIO: Series[bool] = Field(nullable=False)
    TEXT: Series[str] = Field(nullable=False)
    HIGHLIGHT: Series[str] = Field(nullable=False)
    URL: Series[str] = Field(nullable=False)
    TEXT: Series[str] = Field(nullable=False)
    DATE: Series[str] = Field(nullable=False)
    ORIGIN: Series[str] = Field(nullable=False)

    @pa.dataframe_check
    def _has_at_least_one_row(cls, df):
        return len(df) > 0
