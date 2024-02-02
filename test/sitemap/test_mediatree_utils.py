import pytest
import pandas as pd
from quotaclimat.data_ingestion.scrap_html.scrap_description_article import get_meta_news, get_hat_20minutes, get_url_content
from quotaclimat.data_ingestion.scrap_sitemap import get_description_article
from bs4 import BeautifulSoup
from utils import get_localhost, debug_df
from quotaclimat.data_processing.mediatree.utils import get_yesterday, date_to_epoch
import json 
import logging
from time import strftime,localtime

localhost = get_localhost()

def test_date_to_epoch():
    epoch = date_to_epoch("2024-01-20 10:11:55")
    assert epoch == 1705741915

def test_get_yesterday():
    yesterday = get_yesterday()
    yesterday_string = strftime('%Y-%m-%d %H:%M:%S', localtime(yesterday))
    logging.info(f"yesterday_string {yesterday_string}")
    assert '00:00:00' in yesterday_string
