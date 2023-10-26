import logging

import aiohttp
from bs4 import BeautifulSoup
import asyncio
import re

async def get_url_content(url_article: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url_article, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"}) as response:
            return await response.text()

# get https://developer.mozilla.org/en-US/docs/Web/HTML/Element/meta
async def get_meta_description(url_article, media) -> str:
    if(media != "ouest-france"):
        response = await get_url_content(str(url_article))
    else: # https://www.zenrows.com/blog/scraping-javascript-rendered-web-pages#requirements
        return "" # TODO implement a selenium browserless to parse JS website as ouest-france 

    soup = BeautifulSoup(response, "html.parser")
    if soup.find(name="meta", attrs={'name': 'description'}) is not None:
        description = soup.find(name="meta", attrs={'name': 'description'}).get("content")
        logging.debug(f"description for {url_article} is \n {description}")
        return description
    else:
        logging.warning(f"could not find description for {url_article}")


