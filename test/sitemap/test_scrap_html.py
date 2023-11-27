import pytest
import pandas as pd
from quotaclimat.data_ingestion.scrap_html.scrap_description_article import get_meta_news, get_hat_20minutes, get_url_content
from quotaclimat.data_ingestion.scrap_sitemap import get_description_article
from bs4 import BeautifulSoup
from utils import get_localhost, debug_df

localhost = get_localhost()

@pytest.mark.asyncio
async def test_get_description_article():
    url_to_parse = f"{localhost}/mediapart_website.html"
    media = "Le Figaro"
    df_articles = pd.DataFrame([{
        "url" : url_to_parse,
        "news_title" :media,
    }])

    expected_result = pd.DataFrame([{
        "url" : url_to_parse,
        "news_title" :media,
        "news_description" : "description could be parsed with success"
    }])

    df_articles["news_description"] = await get_description_article(media, df_articles)
    debug_df(df_articles)
    pd.testing.assert_frame_equal(df_articles.reset_index(drop=True), expected_result.reset_index(drop=True))

@pytest.mark.asyncio
async def test_get_meta_news():
    url_to_parse = f"{localhost}/mediapart_website.html"

    ouput = await get_meta_news(url_to_parse, "media")
    assert ouput["description"] == "description could be parsed with success"

@pytest.mark.asyncio
async def test_get_hat_20minutes():
    url_to_parse = f"{localhost}/20minutes_website.html"

    response = await get_url_content(url_to_parse)
    hat = get_hat_20minutes(BeautifulSoup(response, "html.parser"))
    assert hat == "howdy there"