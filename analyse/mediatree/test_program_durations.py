import argparse
import asyncio
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
import json
import os

import aiohttp
import pandas as pd
import requests
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm

sys.path.append("../../")

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="channel_coverages.log", encoding="utf-8", level=logging.INFO
)

from quotaclimat.data_processing.mediatree.i8n.brazil.channel_program import (
    channels_programs_brazil,
)
from quotaclimat.data_processing.mediatree.i8n.france.channel_program import (
    channels_programs_france,
)
from quotaclimat.data_processing.mediatree.i8n.poland.channel_program import (
    channels_programs_poland,
)
from quotaclimat.data_processing.mediatree.i8n.spain.channel_program import (
    channels_programs_spain,
)
from quotaclimat.data_processing.mediatree.i8n.germany.channel_program import (
    channels_programs_germany,
)

channels_programs_poland = [
    dict(country="poland", **channel_program)
    for channel_program in channels_programs_poland
]
channels_programs_brazil = [
    dict(country="brazil", **channel_program)
    for channel_program in channels_programs_brazil
]
channels_programs_spain = [
    dict(country="spain", **channel_program)
    for channel_program in channels_programs_spain
]
channels_programs_germany = [
    dict(country="germany", **channel_program)
    for channel_program in channels_programs_germany
]

channel_programs_map = {
    "poland": channels_programs_poland,
    "spain": channels_programs_spain,
    "brazil": channels_programs_brazil,
    "france": channels_programs_france,
    "germany": channels_programs_germany,
}
timezones = {
    "poland": "Europe/Warsaw",
    "spain": "Europe/Madrid",
    "germany": "Europe/Berlin",
    "brazil": "America/Sao_Paulo",
    "france": "Europe/Paris",
}


# async def get_percentage_coverage_async(
#     token, channel_name, date, start, end, connector=None,
# ):
#     results = await get_chunks(token, channel_name, date, start, end, connector=connector)
#     # url, result = results
#     # logging.info(f"{url}\t{result['total_results']}")
#     # return int(result["total_results"] > 0)
#     return results


def get_percentage_coverage(
    token,
    channel_name,
    date,
    start,
    end,
):
    total_minutes = (
        date.replace(hour=int(end.split(":")[0]), minute=int(end.split(":")[1]))
        - date.replace(hour=int(start.split(":")[0]), minute=int(start.split(":")[1]))
    ).seconds / 60
    start_timestamp = int(
        date.replace(
            hour=int(start.split(":")[0]), minute=int(start.split(":")[1])
        ).timestamp()
        - 300
    )
    end_timestamp = int(
        date.replace(
            hour=int(start.split(":")[0]), minute=int(start.split(":")[1])
        ).timestamp()
        + 300
    )
    response = requests.get(
        (
            "https://keywords.mediatree.fr/api/v2/subtitle?"
            f"token={token}&channel={channel_name}&start_gte={start_timestamp}&start_lte={end_timestamp}&type=s2t&size=500"
        )
    )
    if response.status_code == 200:
        body = response.json()
        return body["total_results"] * 2 / total_minutes
    else:
        return 0


def get_coverages_for_day(day, country, channel_programs):
    records = []
    post_arguments = {
        "grant_type": "password",
        "username": open("secrets/username_api.txt", "r").read(),
        "password": open("secrets/pwd_api.txt", "r").read(),
    }
    response = requests.post(
        "https://keywords.mediatree.fr/api/auth/token/", data=post_arguments
    )
    token = response.json()["data"]["access_token"]
    pbar = tqdm(desc="Looping over programs", total=len(channel_programs))
    idx = 0
    retry = 0
    while True:
        if idx == len(channel_programs):
            tqdm.write(f"Finshed programs")
            break
        elif retry > 7:
            tqdm.write(f"break on {channel_programs[idx]} retry {retry}")
            break
        channel_program = channel_programs[idx]
        if day < pd.to_datetime(channel_program["program_grid_start"]).replace(
            tzinfo=ZoneInfo(timezones[country])
        ) or day > pd.to_datetime(channel_program["program_grid_end"]).replace(
            tzinfo=ZoneInfo(timezones[country])
        ):
            idx += 1
            tqdm.write("out of range")
            continue
        if idx > len(channel_programs) // 2:
            response = requests.post(
                "https://keywords.mediatree.fr/api/auth/token/", data=post_arguments
            )
            token = response.json()["data"]["access_token"]
        try:
            if channel_program["weekday"] == "*":
                records.append(
                    {
                        "date": day.strftime("%Y-%m-%d"),
                        "country": country,
                        "channel_name": channel_program["channel_name"],
                        "start": channel_program["start"],
                        "end": channel_program["end"],
                        "coverage": get_percentage_coverage(
                            token,
                            channel_program["channel_name"],
                            day,
                            channel_program["start"],
                            channel_program["end"],
                        ),
                    }
                )
            elif channel_program["weekday"] == "weekend" and day.weekday() in [
                5,
                6,
            ]:
                records.append(
                    {
                        "date": day.strftime("%Y-%m-%d"),
                        "country": country,
                        "channel_name": channel_program["channel_name"],
                        "start": channel_program["start"],
                        "end": channel_program["end"],
                        "coverage": get_percentage_coverage(
                            token,
                            channel_program["channel_name"],
                            day,
                            channel_program["start"],
                            channel_program["end"],
                        ),
                    }
                )
            elif channel_program["weekday"] == "weekday" and day.weekday() < 5:
                records.append(
                    {
                        "date": day.strftime("%Y-%m-%d"),
                        "country": country,
                        "channel_name": channel_program["channel_name"],
                        "start": channel_program["start"],
                        "end": channel_program["end"],
                        "coverage": get_percentage_coverage(
                            token,
                            channel_program["channel_name"],
                            day,
                            channel_program["start"],
                            channel_program["end"],
                        ),
                    }
                )
            idx += 1
            retry = 0
            pbar.update()
        except Exception as e:
            print(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "country": country,
                    "channel_name": channel_program["channel_name"],
                    "start": channel_program["start"],
                    "end": channel_program["end"],
                }
            )
            retry += 1
            raise e
            continue
    return records


async def get_async(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
        else:
            data = {"total_results": 0}
        return data


async def post_async(session, url, data):
    async with session.post(url, data=data) as response:
        response.raise_for_status()
        data = await response.json()
        return data


async def get_percentage_coverage_async(
    token,
    country,
    channel_name,
    date,
    start,
    end,
    session,
    sem: asyncio.Semaphore = None,
):
    total_minutes = (
        date.replace(hour=int(end.split(":")[0]), minute=int(end.split(":")[1]))
        - date.replace(hour=int(start.split(":")[0]), minute=int(start.split(":")[1]))
    ).seconds / 60
    base_url = (
        "https://keywords.mediatree.fr/api/v2/subtitle/?"
        "channel={channel_name}&start_gte={start_chunk_timestamp}&start_lte={end_chunk_timestamp}&"
        "type=s2t&size=200&token={token}"
    )
    # base_url = (
    #     "https://keywords.mediatree.fr/api/v2/subtitle?"
    #     "token={token}&channel={channel_name}&start_gte={start_chunk_timestamp}&start_lte={end_chunk_timestamp}&type=s2t&size=200"
    # )
    start_timestamp = int(
        date.replace(
            hour=int(start.split(":")[0]), minute=int(start.split(":")[1])
        ).timestamp()
        - 30
    )
    end_timestamp = int(
        date.replace(
            hour=int(end.split(":")[0]), minute=int(end.split(":")[1])
        ).timestamp()
        + 30
    )
    url = base_url.format(
        token=token,
        channel_name=channel_name,
        start_chunk_timestamp=start_timestamp,
        end_chunk_timestamp=end_timestamp,
    )
    if sem is not None:
        await sem.acquire()
    try:
        result = await get_async(session, url)
    except aiohttp.client_exceptions.ClientResponseError as e:
        print(e)
        post_arguments = {
            "grant_type": "password",
            "username": open("secrets/username_api.txt", "r").read(),
            "password": open("secrets/pwd_api.txt", "r").read(),
        }
        token_url = "https://keywords.mediatree.fr/api/auth/token/"
        token_result = await post_async(session, token_url, data=post_arguments)
        token = token_result["data"]["access_token"]
        url = base_url.format(
            token=token,
            channel_name=channel_name,
            start_chunk_timestamp=start_timestamp,
            end_chunk_timestamp=end_timestamp,
        )
        result = await get_async(session, url)
    finally:
        if sem:
            sem.release()
    return {
        "date": date.strftime("%Y-%m-%d"),
        "country": country,
        "channel_name": channel_name,
        "start": start,
        "end": end,
        "total_results": result["total_results"],
        "total_minutes": total_minutes,
        "coverage": min(result["total_results"] * 2 / total_minutes, 1.0),
    }


async def async_get_percentage_coverage_for_day(
    day, country, channel_programs, connector_limit=50, excluded_channels=[]
):
    records = []
    post_arguments = {
        "grant_type": "password",
        "username": open("secrets/username_api.txt", "r").read(),
        "password": open("secrets/pwd_api.txt", "r").read(),
    }
    response = requests.post(
        "https://keywords.mediatree.fr/api/auth/token/", data=post_arguments
    )
    token = response.json()["data"]["access_token"]
    valid_channel_programs = []
    for channel_program in channel_programs:
        if channel_program["program_grid_end"] == "":
            channel_program["program_grid_end"] = "2100-01-01"
        if (
            day
            >= pd.to_datetime(channel_program["program_grid_start"]).replace(
                tzinfo=ZoneInfo(timezones[country])
            )
            and day
            < pd.to_datetime(channel_program["program_grid_end"]).replace(
                tzinfo=ZoneInfo(timezones[country])
            )
            and int(channel_program["weekday"]) == day.weekday() if str(channel_program["weekday"]).isdigit() else (
                channel_program["weekday"] == "*"
                or (
                    channel_program["weekday"] == "weekend"
                    and day.weekday()
                    in [
                        5,
                        6,
                    ]
                )
                or (channel_program["weekday"] == "weekday" and day.weekday() < 5)
            ) 
            and channel_program["channel_name"] != "d8"
            and channel_program["channel_name"] != "daserste"
            and channel_program["channel_name"] != "zdf-neo"
            and not channel_program["channel_name"] in excluded_channels
        ):
            if not channel_program["channel_name"] == "daserste":
                valid_channel_programs.append(channel_program)
    sem = asyncio.Semaphore(connector_limit - 5)
    connector = aiohttp.TCPConnector(limit=connector_limit, force_close=True)
    async with aiohttp.ClientSession(
        connector=connector, raise_for_status=True
    ) as session:
        tasks = [
            get_percentage_coverage_async(
                token,
                country,
                channel_program["channel_name"],
                day,
                channel_program["start"],
                channel_program["end"],
                session=session,
                sem=sem,
            )
            for channel_program in valid_channel_programs
        ]
        records = await atqdm.gather(*tasks)
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=datetime.now(), type=str)
    parser.add_argument("--n-days", default=7, type=int)
    parser.add_argument("--tcp-connectionlimit", default=20, type=int)
    parser.add_argument("--use-async", action=argparse.BooleanOptionalAction)
    parser.add_argument('--exclude', action='append', default=[])

    args = parser.parse_args()

    records = []
    for country, channel_programs in channel_programs_map.items():
        if isinstance(args.start_date, str):
            today = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo(timezones[country])
            )
        else:
            today = datetime.now().replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=ZoneInfo(timezones[country]),
            )
        date_range = pd.date_range(end=today, periods=args.n_days, freq="D")
        print(f"Testing for country: {country}")
        for day in tqdm(
            reversed(date_range), desc="Looping over dates", total=len(date_range)
        ):
            tqdm.write(f"Parsing date {day.strftime('%Y-%m-%d')}")
            if args.use_async:
                _records = asyncio.run(
                    async_get_percentage_coverage_for_day(
                        day,
                        country,
                        channel_programs,
                        connector_limit=50,
                        excluded_channels=args.exclude,
                    )
                )
                records.extend(_records)
            else:
                records.extend(get_coverages_for_day(day, country, channel_programs))

    df = pd.DataFrame.from_records(records)
    df_group = df.groupby(["date", "country", "channel_name"]).agg(
        {"coverage": "mean", "total_results": "sum", "total_minutes": "sum"}
    )
    print(df_group.head())
    df_group.to_csv(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "data",
            f"mediatree_channel_coverages_{today.strftime('%Y-%m-%d')}"
        )
    )
