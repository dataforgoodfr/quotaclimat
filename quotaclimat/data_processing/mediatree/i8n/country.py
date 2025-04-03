import logging
import os
from typing import Optional

class CountryMediaTree:   
    def __init__(self, code, language, channels, timezone):
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
        self.timezone = timezone
    

    def __str__(self):
        """Return a string representation of the CountryMediaTree."""
        return f"CountryMediaTree(code='{self.code}', channels={self.channels}, language={self.language}, timezone='{self.timezone}')"

FRANCE_CODE = "fra"
FRANCE_CHANNELS= ["tf1", "france2", "fr3-idf", "m6", "arte", "bfmtv", "lci", "franceinfotv", "itele"
            "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]
FRANCE_TZ = "Europe/Paris"
FRANCE_LANGUAGE = "french"
FRANCE = CountryMediaTree(code=FRANCE_CODE,channels=FRANCE_CHANNELS, timezone=FRANCE_TZ, language=FRANCE_LANGUAGE)

GERMANY_CODE="ger"
GERMANY_CHANNELS= ["daserste"
    ,"zdf-neo"
    ,"rtl-television"
    ,"sat1"
    ,"prosieben"
    ,"kabel-eins"]
GERMANY_TZ = "Europe/Berlin"
GERMANY_LANGUAGE = "german"
GERMANY = CountryMediaTree(code=GERMANY_CODE,channels=GERMANY_CHANNELS, timezone=GERMANY_TZ, language=GERMANY_LANGUAGE)

BRAZIL_CODE="bra"
BRAZIL_CHANNELS=["tvglobo"
    ,"tvrecord"
    ,"sbt"
    ,"redebandeirantes"
    ,"jovempan"
    ,"cnnbrasil"
]
BRAZIL_TZ = "America/Sao_Paulo"
BRAZIL_LANGUAGE = "portuguese"
BRAZIL = CountryMediaTree(code=BRAZIL_CODE,channels=BRAZIL_CHANNELS, timezone=BRAZIL_TZ, language=BRAZIL_LANGUAGE)

COUNTRIES = {
    FRANCE.code: FRANCE,
    GERMANY.code: GERMANY,
    BRAZIL.code: BRAZIL,
}

def get_country(country_code: str) -> Optional[CountryMediaTree]:
    return COUNTRIES.get(country_code)

def get_channels(country_code=FRANCE.code) -> List[str]:
    if(os.environ.get("ENV") == "docker" or os.environ.get("CHANNEL") is not None):
        default_channel = os.environ.get("CHANNEL") or "france2"
        logging.warning(f"Only one channel of env var CHANNEL {default_channel} (default to france2) is used")

        return [default_channel]
    else: #prod  - all channels
        logging.warning(f"All channels are used for {country_code}")
        country = get_country(country_code)
        if country:
            return country.channels
        
        logging.error(f"Unknown country {country_code} - empty array - known array are {COUNTRIES}")
        return []