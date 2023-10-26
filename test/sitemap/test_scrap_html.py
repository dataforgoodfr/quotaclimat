import logging
import pytest
import os
from quotaclimat.data_ingestion.scrap_html.scrap_description_article import get_meta_description

@pytest.mark.asyncio
async def test_get_meta_description():
    url_to_parse = "" 

    if(os.environ.get("ENV") == "docker"):
        url_to_parse ="http://nginxtest:80/mediapart_website.html"
    else:
        url_to_parse = "http://localhost:8000/mediapart_website.html"

    ouput = await get_meta_description(url_to_parse, "media")
    assert ouput == "description could be parsed with success"