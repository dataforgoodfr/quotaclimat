import logging
import os

FRANCE="fra"
FRANCE_CHANNELS=  ["tf1", "france2", "fr3-idf", "m6", "arte", "bfmtv", "lci", "franceinfotv", "itele",
            "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]
GERMANY="ger"
GERMANY_CHANNELS=[
    ,"daserste"
    ,"zdf-neo"
    ,"rtl-television"
    ,"sat1"
    ,"prosieben"
    ,"kabel-eins"
]
BRAZIL="bra"
BRAZIL_CHANNELS=[
    ,"tvglobo"
    ,"tvrecord"
    ,"sbt"
    ,"redebandeirantes"
    ,"jovempan"
    ,"cnnbrasil"
]

def get_channels(country=FRANCE):
    if(os.environ.get("ENV") == "docker" or os.environ.get("CHANNEL") is not None):
        default_channel = os.environ.get("CHANNEL") or "france2"
        logging.warning(f"Only one channel of env var CHANNEL {default_channel} (default to france2) is used")

        return [default_channel]
    else: #prod  - all channels
        logging.warning(f"All channels are used for {country}")
        match country:
            case FRANCE:
                return FRANCE_CHANNELS
            case BRAZIL:
                return BRAZIL_CHANNELS
            case GERMANY:
                return GERMANY_CHANNELS
            case _:
                logging.error(f"Unknown country {country} - empty array")
                return []