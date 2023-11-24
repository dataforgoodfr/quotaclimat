import logging
import pytest
import os
from quotaclimat.data_ingestion.scrap_html.scrap_description_article import get_meta_news, get_hat_20minutes, get_url_content
from bs4 import BeautifulSoup

localhost = ""
if(os.environ.get("ENV") == "docker"):
    localhost ="http://nginxtest:80"
else:
    localhost = "http://localhost:8000"

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