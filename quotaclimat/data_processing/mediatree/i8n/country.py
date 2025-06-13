import logging
import os
from typing import Optional, Literal, Union, List

from quotaclimat.data_processing.mediatree.i8n.brazil.channel_program import *
from quotaclimat.data_processing.mediatree.i8n.germany.channel_program import *
from quotaclimat.data_processing.mediatree.channel_program_data import channels_programs

# Define country codes as Literal types
# from https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
FranceCode = Literal["fra"]
GermanyCode = Literal["deu"]
BrazilCode = Literal["bra"]
BelgiumCode = Literal["bel"]
AllCode = Literal["all"]
CountryCode = Union[FranceCode, GermanyCode, BelgiumCode, BrazilCode, AllCode]

def get_country_name_from_code(code: CountryCode) -> str:
    match code:
        case "fra":
            return 'france'
        case "deu":
            return 'germany'
        case "bra":
            return 'brazil'
        case "bel":
            return 'belgium'
        # case SPAIN.code:
        #     return 'spain'
        # case PORTUGAL.code:
        #     return 'portugal'
        # case POLAND.code:
        #     return 'poland'
        # case DENMARK.code:
        #     return 'denmark'
        # case ITALY.code:
        #     return 'italy'
        # case ARAB_LEAGUE.code: 
        #     return 'arabic'
        # case GREECE.code:
        #     return 'greece'
        # case NETHERLANDS.code:
        #     return 'netherlands'
        # case LATVIA.code:
        #     return 'latvia'
        # case ENGLAND.code:  # If you use a generic 'EN' or 'UK'/'US' code
        #     return 'england'
        case _:
            raise ValueError(f"Unsupported country/language code: {code}")


class CountryMediaTree:   
    def __init__(self, code: CountryCode, language: str, channels: List[str], programs: Optional[List[str]], timezone: str):
        """
        Initialize a CountryMediaTree instance.
        
        Args:
            code (str): The country code.
            channels (list): List of media channels.
            timezone (str): The country's timezone.
        """
        self.code = code
        self.name = get_country_name_from_code(code)
        self.language = language
        self.channels = channels
        self.programs = programs
        self.timezone = timezone

    def __str__(self):
        """Return a string representation of the CountryMediaTree."""
        return f"CountryMediaTree(code='{self.code}', channels={self.channels}, programs={self.programs}, language={self.language}, name={self.name}, timezone='{self.timezone}')"


FRANCE_CODE : FranceCode = "fra"
FRANCE_CHANNELS= ["tf1", "france2", "fr3-idf", "m6", "arte", "bfmtv", "lci", "franceinfotv", "itele", "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]
FRANCE_TZ = "Europe/Paris"
FRANCE_LANGUAGE = "french"
FRANCE = CountryMediaTree(code=FRANCE_CODE,channels=FRANCE_CHANNELS, timezone=FRANCE_TZ, language=FRANCE_LANGUAGE, programs=channels_programs)


BELGIUM_CODE : BelgiumCode = "bel"
BELGIUM_CHANNELS= ["CANALZ","RTL","LAUNE","LN24","LATROIS"]
BELGIUM_TZ = "Europe/Bruxelles"
BELGIUM_LANGUAGE = "french" # TODO: flemish based on channel ?
channels_programs_belgium = None # TODO
BELGIUM = CountryMediaTree(code=BELGIUM_CODE,channels=BELGIUM_CHANNELS, timezone=BELGIUM_TZ, language=BELGIUM_LANGUAGE, programs=channels_programs_belgium)

GERMANY_CODE: GermanyCode ="deu"
GERMANY_CHANNELS= ["daserste" # from srt import
    ,"zdf-neo"
    ,"zdf" # from srt import
    ,"rtl-television"
    ,"sat1"
    ,"prosieben"
    ,"kabel-eins"]
GERMANY_CHANNELS_MEDIATREE= [ # imported via mediatree api only
    "zdf-neo"
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
    BELGIUM.code: BELGIUM,
}

ALL_COUNTRIES_CODE : AllCode = "all"
# belgium not included in mediatree
ALL_COUNTRIES = [GERMANY, FRANCE, BRAZIL, BELGIUM]

def get_all_countries(no_belgium = False):
    logging.info(f"Getting all countries : {ALL_COUNTRIES}")
    if no_belgium:
        logging.info(f"Removing belgium from all countries")
        return [country for country in ALL_COUNTRIES if country.code != BELGIUM_CODE]
    else:
        return ALL_COUNTRIES

def validate_country_code(code: str) -> CountryCode:
    """Validate that a string is a valid country code."""
    if code in (FRANCE_CODE, GERMANY_CODE, BRAZIL_CODE, BELGIUM_CODE, ALL_COUNTRIES_CODE):
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
        # TODO : channels belgium
        country = get_country_from_code(country_code)
        if country:
            return country.channels
        
        logging.error(f"Unknown country {country_code} - empty array - known array are {COUNTRIES}")
        return []
    
def get_channel_title_for_name(channel_name: str, country: CountryMediaTree = FRANCE) -> str:
    logging.debug(f"Getting channel title for {channel_name} in {country.code}")
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
            case "daserste":
                return "Das Erste"
            case "zdf-neo":
                return "ZDFneo"
            case "zdf":
                return "ZDF"
            case "rtl-television":
                return "RTL"
            case "sat1":
                return "Sat.1"
            case "prosieben":
                return "ProSieben"
            case "kabel-eins":
                return "Kabel Eins"
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
            case "redebandeirantes":
                return "Band"
            case "jovempan":
                return "Jovem Pan"
            case "cnnbrasil":
                return "CNN Brasil"
            case _:
                logging.error(f"Unknown channel name: {country.code} {channel_name}")
                return ""

    else:
        logging.error(f"Unsupported country code: {country.code}")
        return ""


def get_countries_array(country_code: str, no_belgium = True):
    if validate_country_code(country_code):
        if country_code == ALL_COUNTRIES_CODE:
            logging.info(f"Getting all countries {country_code} without belgium {no_belgium}")
            countries = get_all_countries(no_belgium)
        else:
            logging.info(f"Getting one country only {country_code}")
            countries = [get_country_from_code(country_code = country_code)]

    return countries