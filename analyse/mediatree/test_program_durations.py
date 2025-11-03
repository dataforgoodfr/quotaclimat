import asyncio
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
import pandas as pd
import requests
from tqdm import tqdm

sys.path.append("../../")

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

channel_programs_map = {
    "poland": channels_programs_poland,
    "spain": channels_programs_spain,
    "brazil": channels_programs_brazil,
    "france": channels_programs_france,
}
timezones = {
    "poland": "Europe/Warsaw",
    "spain": "Europe/Madrid",
    "brazil": "America/Sao_Paulo",
    "france": "Europe/Paris",
}

country = "france"


async def get_async(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
        else:
            data = {"total_results": 0}
        return data


async def get_chunks(token, channel_name, date, start, total_minutes, sem=5):
    connector = aiohttp.TCPConnector(limit=50, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        base_url = (
            "https://keywords.mediatree.fr/api/v2/subtitle?"
            "token={token}&channel={channel_name}&start_gte={start_chunk_timestamp}&type=s2t&size=200"
        )
        start_timestamps = [
            (
                date.replace(
                    hour=int(start.split(":")[0]), minute=int(start.split(":")[1])
                )
                + timedelta(minutes=i * 2)
            ).timestamp()
            for i in range(int(total_minutes // 2))
        ]
        async with asyncio.Semaphore(sem):
            tasks = [
                get_async(
                    session,
                    base_url.format(
                        token=token,
                        channel_name=channel_name,
                        start_chunk_timestamp=int(start_ts),
                    ),
                )
                for start_ts in start_timestamps
            ]
            results = await asyncio.gather(*tasks)
        return results


def get_percentage_coverage(
    token, channel_name, date, start, end, use_async=False, sem=5
):
    total_minutes = (
        date.replace(hour=int(end.split(":")[0]), minute=int(end.split(":")[1]))
        - date.replace(hour=int(start.split(":")[0]), minute=int(start.split(":")[1]))
    ).seconds / 60
    yes = 0
    no = 0
    if use_async:
        results = asyncio.run(
            get_chunks(token, channel_name, date, start, total_minutes, sem)
        )
        for result in results:
            yes += int(result["total_results"] > 0)
            no += int(result["total_results"] == 0)
    else:
        for i in range(int(total_minutes // 2)):
            start_chunk = date.replace(
                hour=int(start.split(":")[0]), minute=int(start.split(":")[1])
            ) + timedelta(minutes=i * 2)
            start_chunk_timestamp = int(start_chunk.timestamp())
            response = requests.get(
                (
                    "https://keywords.mediatree.fr/api/v2/subtitle?"
                    f"token={token}&channel={channel_name}&start_gte={start_chunk_timestamp}&type=s2t&size=200"
                )
            )
            if response.status_code == 200:
                body = response.json()
                yes += int(body["total_results"] > 0)
                no += int(body["total_results"] == 0)
            else:
                no += 1
    return yes / (yes + no)


if __name__ == "__main__":
    today = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=ZoneInfo(timezones[country])
    )
    records = []
    date_range = pd.date_range(end=today, periods=16, freq="D")

    for country, channel_programs in channel_programs_map.items():
        print(f"Testing for country: {country}")
        for day in tqdm(
            reversed(date_range), desc="Looping over dates", total=len(date_range)
        ):
            tqdm.write(f"Parsing date {day.strftime('%Y-%m-%d')}")
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
                if idx == len(channel_programs) or retry > 7:
                    break
                channel_program = channel_programs[idx]
                if not day >= pd.to_datetime(channel_program["program_grid_start"]).replace(
                    tzinfo=ZoneInfo(timezones[country])
                ) or not day < pd.to_datetime(channel_program["program_grid_end"]).replace(
                    tzinfo=ZoneInfo(timezones[country])
                ):
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
                                    use_async=True,
                                    sem=5,
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
                                    use_async=True,
                                    sem=5,
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
                                    use_async=True,
                                    sem=5,
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
                    continue

    df = pd.DataFrame.from_records(records)
    df_group = df.groupby(["date", "country", "channel_name"]).agg({"coverage": "mean"})
    print(df_group.head())
    df_group.to_csv(
        f"mediatree_channel_coverages_{today.strftime('%Y-%m-%d')}"
    )
