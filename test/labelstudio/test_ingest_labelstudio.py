import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Import your module
import quotaclimat.data_ingestion.labelstudio.ingest_labelstudio as ls
from quotaclimat.data_ingestion.labelstudio import models

