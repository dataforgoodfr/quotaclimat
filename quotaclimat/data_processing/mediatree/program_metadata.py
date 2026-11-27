"""Program grid of every country (table program_metadata), built from the
`i8n/<country>/channel_program.py` files."""

import hashlib
import logging
import os
from datetime import datetime

from quotaclimat.data_processing.mediatree.i8n.belgium.channel_program import (
    channels_programs_belgium,
)
from quotaclimat.data_processing.mediatree.i8n.belgium_flanders.channel_program import (
    channels_programs_belgium_flanders,
)
from quotaclimat.data_processing.mediatree.i8n.brazil.channel_program import (
    channels_programs_brazil,
)
from quotaclimat.data_processing.mediatree.i8n.country import (
    BELGIUM,
    BRAZIL,
    FRANCE,
    GERMANY,
    POLAND,
    SPAIN,
)
from quotaclimat.data_processing.mediatree.i8n.extended_france.channel_program import (
    channels_programs_extended_france,
)
from quotaclimat.data_processing.mediatree.i8n.france.channel_program import (
    channels_programs_france,
)
from quotaclimat.data_processing.mediatree.i8n.germany.channel_program import (
    channels_programs_germany,
)
from quotaclimat.data_processing.mediatree.i8n.poland.channel_program import (
    channels_programs_poland,
)
from quotaclimat.data_processing.mediatree.i8n.spain.channel_program import (
    channels_programs_spain,
)

GRID_END_WITHOUT_LIMIT = "2100-01-01"


def generate_program_id(channel_name, weekday, program_name, program_grid_start) -> str:
    data_str = f"{channel_name}-{weekday}-{program_name}-{program_grid_start}"
    return hashlib.sha256(data_str.encode("utf-8")).hexdigest()


# Function to calculate duration in minutes between two time strings
def calculate_duration(start_time, end_time):
    fmt = "%H:%M"
    start_dt = datetime.strptime(start_time, fmt)
    end_dt = datetime.strptime(end_time, fmt)
    duration_minutes = (end_dt - start_dt).seconds // 60
    return duration_minutes


channel_mapping = {
    # 🇫🇷 France
    "tf1": {
        "title": "TF1",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "france2": {
        "title": "France 2",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "fr3-idf": {
        "title": "France 3-idf",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "m6": {
        "title": "M6",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "arte": {
        "title": "Arte",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "d8": {
        "title": "C8",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "bfmtv": {
        "title": "BFM TV",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name,
    },
    "lci": {
        "title": "LCI",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name,
    },
    "franceinfotv": {
        "title": "France Info TV",
        "public": True,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name,
    },
    "itele": {
        "title": "CNews",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name,
    },
    "europe1": {
        "title": "Europe 1",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    "france-culture": {
        "title": "France Culture",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    "france-inter": {
        "title": "France Inter",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    "sud-radio": {
        "title": "Sud Radio",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    "rmc": {
        "title": "RMC",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    "rtl": {
        "title": "RTL",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    "france24": {
        "title": "France 24",
        "public": True,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name,
    },
    "france-info": {
        "title": "FranceinfoRadio",
        "public": True,
        "infocontinue": True,
        "radio": True,
        "country": FRANCE.name,
    },
    "rfi": {
        "title": "RFI",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name,
    },
    # 🇫🇷 France - extended perimeter
    "tmc": {
        "title": "TMC",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "lcp": {
        "title": "LCP",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    "france5": {
        "title": "France 5",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name,
    },
    # 🇩🇪 Germany
    "daserste": {
        "title": "Das Erste",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name,
    },
    "zdf-neo": {
        "title": "ZDFneo",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name,
    },
    "rtl-television": {
        "title": "RTL",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name,
    },
    "sat1": {
        "title": "Sat.1",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name,
    },
    "prosieben": {
        "title": "ProSieben",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name,
    },
    "kabel-eins": {
        "title": "Kabel Eins",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name,
    },
    # 🇧🇷 Brazil
    "tvbrasil": {
        "title": "TV Brasil",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name,
    },
    "tvglobo": {
        "title": "TV Globo",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name,
    },
    "tvrecord": {
        "title": "TV Record",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name,
    },
    "sbt": {
        "title": "SBT",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name,
    },
    "redebandeirantes": {
        "title": "Band",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name,
    },
    "jovempan": {
        "title": "Jovem Pan",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BRAZIL.name,
    },
    "cnnbrasil": {
        "title": "CNN Brasil",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": BRAZIL.name,
    },
    # Poland
    "polskie-radio": {
        "title": "Polskie Radio",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name,
    },
    "radio-zet": {
        "title": "Radio Zet",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name,
    },
    "eska": {
        "title": "Eska",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name,
    },
    "polsat": {
        "title": "Polsat",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "tvn": {
        "title": "TVN",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "tvp": {
        "title": "TVP",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "tokfm": {
        "title": "TOKFM",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name,
    },
    "tv-republika": {
        "title": "TV Republika",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "tv-trwam": {
        "title": "TV Trwa",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "radio-maryja": {
        "title": "Radio Maryj",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name,
    },
    "tvs": {
        "title": "TVS",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "wpolsce24": {
        "title": "wPolsce24",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": POLAND.name,
    },
    "tv-puls": {
        "title": "TV Plus",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    "fokus-tv": {
        "title": "Fokus TV",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name,
    },
    # Spain
    "antenna-3": {
        "title": "Antenna 3",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name,
    },
    "rtve-24h": {
        "title": "RTVE 24h",
        "public": True,
        "infocontinue": True,
        "radio": False,
        "country": SPAIN.name,
    },
    "rtve-la-1": {
        "title": "RTVE La 1",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name,
    },
    "lasexta-news": {
        "title": "LaSexta News",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name,
    },
    "telecinco-news": {
        "title": "Telecinco News",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name,
    },
    "cuatro-news": {
        "title": "Cuatro News",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name,
    },
    # Belgium-Wallonia
    "la-premiere": {
        "title": "La Premi\u00e8re",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "bel-rtl": {
        "title": "BEL RTL",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "vivacite": {
        "title": "Vivacit\u00e9",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "ln-radio": {
        "title": "LN RADIO",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    # Belgium-Flanders
    "canvas": {
        "title": "Canvas",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BELGIUM.name,
    },
    "radio-2": {
        "title": "RADIO 2",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "vtm": {
        "title": "VTM",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BELGIUM.name,
    },
    "radio-1": {
        "title": "RADIO 1",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "stu-bru": {
        "title": "STU BRU",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "qmusic": {
        "title": "QMUSIC",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BELGIUM.name,
    },
    "play": {
        "title": "Play",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BELGIUM.name,
    },
    "vrt1": {
        "title": "VRT 1",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BELGIUM.name,
    },
    "atv": {
        "title": "ATV",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BELGIUM.name,
    },
    "tvl": {
        "title": "TVL",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BELGIUM.name,
    },
}


def _expand_weekdays(program_data: dict) -> list[dict]:
    # One entry per weekday, from 1 (Monday) to 7 (Sunday) to simplify SQL queries
    weekday = program_data["weekday"]
    if weekday == "*":
        days = range(1, 8)
    elif weekday == "weekend":
        days = [6, 7]
    elif weekday == "weekday":
        days = [1, 2, 3, 4, 5]
    else:
        # channel_program.py weekdays go from 0 to 6
        days = [int(weekday) + 1]
    return [{**program_data, "weekday": day} for day in days]


def build_program_metadata(extended_perimeter: bool | None = None) -> list[dict]:
    """Every program of the grid, one entry per weekday, sorted by weekday and channel.
    With the extended perimeter (default: EXTENDED_PERIMETER env var), its programs are added.
    """
    if extended_perimeter is None:
        extended_perimeter = os.getenv("EXTENDED_PERIMETER", "false").lower() == "true"

    channels_programs = (
        channels_programs_france
        + channels_programs_brazil
        + channels_programs_germany
        + channels_programs_poland
        + channels_programs_spain
        + channels_programs_belgium
        + channels_programs_belgium_flanders
    )
    if extended_perimeter:
        logging.info(
            "EXTENDED_PERIMETER is true - importing extended perimeter programs for France"
        )
        channels_programs = channels_programs + channels_programs_extended_france

    programs = []
    for source in channels_programs:
        # Copy: the source lists are also read as they are by the import
        program_data = dict(source)
        program_data["duration"] = calculate_duration(
            program_data["start"], program_data["end"]
        )
        if program_data["program_grid_end"] == "":
            program_data["program_grid_end"] = GRID_END_WITHOUT_LIMIT

        channel_name = program_data["channel_name"]
        if channel_name in channel_mapping:
            program_data["channel_title"] = channel_mapping[channel_name]["title"]
            program_data["public"] = channel_mapping[channel_name]["public"]
            program_data["infocontinue"] = channel_mapping[channel_name]["infocontinue"]
            program_data["radio"] = channel_mapping[channel_name]["radio"]
            program_data["country"] = channel_mapping[channel_name]["country"]
        else:
            logging.error(f"Unknown channel_name {channel_name}")
            program_data["channel_title"] = channel_name
            program_data["public"] = False
            program_data["infocontinue"] = False
            program_data["radio"] = False

        programs += _expand_weekdays(program_data)

    for program in programs:
        program["id"] = generate_program_id(
            program["channel_name"],
            program["weekday"],
            program["program_name"],
            program["program_grid_start"],
        )

    return sorted(programs, key=lambda x: (x["weekday"], x["channel_name"]))
