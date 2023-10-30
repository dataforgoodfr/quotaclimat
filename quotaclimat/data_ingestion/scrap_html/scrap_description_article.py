import logging

import aiohttp
from bs4 import BeautifulSoup
import asyncio
import re

async def get_url_content(url_article: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url_article, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"}) as response:
            return await response.text()

def get_hat_20minutes(soup, url_article = ""):
    hat = soup.select_one(".hat-summary")
    if hat is not None:
        return (hat.text).strip()
    else:
        logging.warning(f"could not get hat : {url_article}")
        return ""

# get https://developer.mozilla.org/en-US/docs/Web/HTML/Element/meta
async def get_meta_news(url_article, media):
    result = {
        "title": "",
        "description": "",
    }

    if(media != "ouest-france"): # anti robot
        response = await get_url_content(str(url_article))
    else:
        return result

    soup = BeautifulSoup(response, "html.parser")
    soup_description = soup.find(name="meta", attrs={'name': 'description'})
    if soup_description is not None:
        description = soup_description.get("content").strip()
        logging.debug(f"description for {url_article} is \n {description}")
        result["description"] = description
    elif media == "20_minutes": # does not have meta description
        hat = get_hat_20minutes(soup, url_article)
        logging.info(f"reading hat for {media} - {hat}")
        result["description"] = hat
    else:
        logging.warning(f"could not find description for {url_article}")

    # TODO : use it someday to parse missing data
    soup_title = soup.find(name="title")
    if soup_title is not None:
        result["title"] = (soup_title.string).strip()
        
    return result


