import logging
import os
from typing import Optional, Literal, Union, List

from quotaclimat.data_processing.mediatree.i8n.brazil.channel_program import *
from quotaclimat.data_processing.mediatree.i8n.germany.channel_program import *
from quotaclimat.data_processing.mediatree.channel_program_data import channels_programs

# Define country codes as Literal types
FranceCode = Literal["fra"]
GermanyCode = Literal["ger"]
BrazilCode = Literal["bra"]
AllCode = Literal["all"]
CountryCode = Union[FranceCode, GermanyCode, BrazilCode, AllCode]

class CountryMediaTree:   
    def __init__(self, code: CountryCode, language, channels: List[str], programs: List[str], timezone: str):
        """
        Initialize a CountryMediaTree instance.
        
        Args:
            code (str): The country code.
            channels (list): List of media channels.
            timezone (str): The country's timezone.
        """
        self.code = code
        self.language = language
        self.channels = channels
        self.programs = programs
        self.timezone = timezone

    def __str__(self):
        """Return a string representation of the CountryMediaTree."""
        return f"CountryMediaTree(code='{self.code}', channels={self.channels}, programs={self.programs}, language={self.language}, timezone='{self.timezone}')"


FRANCE_CODE : FranceCode = "fra"
FRANCE_CHANNELS= ["tf1", "france2", "fr3-idf", "m6", "arte", "bfmtv", "lci", "franceinfotv", "itele"
            "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]
FRANCE_TZ = "Europe/Paris"
FRANCE_LANGUAGE = "french"
FRANCE = CountryMediaTree(code=FRANCE_CODE,channels=FRANCE_CHANNELS, timezone=FRANCE_TZ, language=FRANCE_LANGUAGE, programs=channels_programs)

GERMANY_CODE: GermanyCode ="ger"
GERMANY_CHANNELS= ["daserste"
    ,"zdf-neo"
    ,"rtl-television"
    ,"sat1"
    ,"prosieben"
    ,"kabel-eins"]
GERMANY_TZ = "Europe/Berlin"
GERMANY_LANGUAGE = "german"
GERMANY_PROGRAMS: list[dict[str, str]] = channels_programs_germany
GERMANY = CountryMediaTree(code=GERMANY_CODE,channels=GERMANY_CHANNELS, timezone=GERMANY_TZ, language=GERMANY_LANGUAGE, programs=channels_programs_germany)

BRAZIL_CODE: BrazilCode="bra"
BRAZIL_CHANNELS=["tvglobo"
    ,"tvrecord"
    ,"sbt"
    ,"redebandeirantes"
    ,"jovempan"
    ,"cnnbrasil"
]
BRAZIL_TZ = "America/Sao_Paulo"
BRAZIL_LANGUAGE = "portuguese"
BRAZIL = CountryMediaTree(code=BRAZIL_CODE,channels=BRAZIL_CHANNELS, timezone=BRAZIL_TZ, language=BRAZIL_LANGUAGE, programs=channels_programs_brazil)

COUNTRIES = {
    FRANCE.code: FRANCE,
    GERMANY.code: GERMANY,
    BRAZIL.code: BRAZIL,
}

ALL_COUNTRIES_CODE : AllCode = "all"
ALL_COUNTRIES = [FRANCE, BRAZIL, GERMANY]

def get_all_countries():
    logging.info(f"Getting all countries : {ALL_COUNTRIES}")
    return ALL_COUNTRIES

def validate_country_code(code: str) -> CountryCode:
    """Validate that a string is a valid country code."""
    if code in (FRANCE_CODE, GERMANY_CODE, BRAZIL_CODE, ALL_COUNTRIES_CODE):
        return code
    raise ValueError(f"Invalid country code: {code}")

def get_country_from_code(country_code: str) -> CountryMediaTree:
    return COUNTRIES.get(country_code)

def get_channels(country_code=FRANCE.code) -> List[str]:
    if(os.environ.get("ENV") == "docker" or os.environ.get("CHANNEL") is not None):
        default_channel = os.environ.get("CHANNEL") or "france2"
        logging.warning(f"Only one channel of env var CHANNEL {default_channel} (default to france2) is used")

        return [default_channel]
    else: #prod  - all channels
        logging.warning(f"All channels are used for {country_code}")
        country = get_country_from_code(country_code)
        if country:
            return country.channels
        
        logging.error(f"Unknown country {country_code} - empty array - known array are {COUNTRIES}")
        return []
    
def get_channel_title_for_name(channel_name: str, country: CountryMediaTree = FRANCE) -> str:
    if country.code == FRANCE_CODE:
        match channel_name:
            case "tf1":
                return "TF1"
            case "france2":
                return "France 2"
            case "fr3-idf":
                return "France 3-idf"
            case "m6":
                return "M6"
            case "arte":
                return "Arte"
            case "d8":
                return "C8"
            case "bfmtv":
                return "BFM TV"
            case "lci":
                return "LCI"
            case "franceinfotv":
                return "France Info TV"
            case "itele":
                return "CNews"
            case "europe1":
                return "Europe 1"
            case "france-culture":
                return "France Culture"
            case "france-inter":
                return "France Inter"
            case "sud-radio":
                return "Sud Radio"
            case "rmc":
                return "RMC"
            case "rtl":
                return "RTL"
            case "france24":
                return "France 24"
            case "france-info":
                return "FranceinfoRadio"
            case "rfi":
                return "RFI"
            case _:
                logging.error(f"Channel_name unknown {channel_name}")
                return ""

    elif country.code == GERMANY_CODE:
        match channel_name:
            case "ard":
                return "ARD"
            case "zdf":
                return "ZDF"
            case "rtl":
                return "RTL"
            case "sat1":
                return "SAT.1"
            case "prosieben":
                return "ProSieben"
            case "vox":
                return "VOX"
            case "kabel1":
                return "Kabel Eins"
            case "n24":
                return "WELT (ex-N24)"
            case "ntv":
                return "n-tv"
            case "deutschlandfunk":
                return "Deutschlandfunk"
            case "br":
                return "Bayerischer Rundfunk"
            case "wdr":
                return "Westdeutscher Rundfunk"
            case "swr":
                return "Südwestrundfunk"
            case "mdr":
                return "Mitteldeutscher Rundfunk"
            case "ndr":
                return "Norddeutscher Rundfunk"
            case _:
                logging.error(f"Unknown channel name: {country.code} {channel_name}")
                return ""

    elif country.code == BRAZIL_CODE:
        match channel_name:
            case "tvglobo":
                return "TV Globo"
            case "tvrecord":
                return "TV Record"
            case "sbt":
                return "SBT"
            case "band":
                return "Band"
            case "jovempan":
                return "Jovem Pan"
            case "cnnbrasil":
                return "CNN Brasil"
            case "redevida":
                return "Rede Vida"
            case "gazeta":
                return "TV Gazeta"
            case "cultura":
                return "TV Cultura"
            case _:
                logging.error(f"Unknown channel name: {country.code} {channel_name}")
                return ""

    else:
        logging.error(f"Unsupported country code: {country.code}")
        return ""


def get_countries_array(country_code: str):
    if validate_country_code(country_code):
        if country_code == ALL_COUNTRIES_CODE:
            logging.info(f"Getting all countries {country_code}")
            countries = get_all_countries()
        else:
            logging.info(f"Getting one country only {country_code}")
            countries = [get_country_from_code(country_code = country_code)]

    return countries