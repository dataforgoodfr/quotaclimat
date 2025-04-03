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
CountryCode = Union[FranceCode, GermanyCode, BrazilCode]

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

def validate_country_code(code: str) -> CountryCode:
    """Validate that a string is a valid country code."""
    if code in (FRANCE_CODE, GERMANY_CODE, BRAZIL_CODE):
        return code
    raise ValueError(f"Invalid country code: {code}")

def get_country_from_code(country_code: str) -> Optional[CountryMediaTree]:
    return COUNTRIES.get(validate_country_code(country_code))

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