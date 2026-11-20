"""The prompt's exclusions (mandatory messages, regulated CO2 displays), applied to claims
after the fact because the model records them anyway."""

import re

from quotaclimat.data_ingestion.advertising.tools.normalize import (
    normalize,
)

MANDATORY_MESSAGES = (
    "moinspolluer",
    "pensez a covoitur",
    "prenez les transports en commun",
    "privilegiez la marche ou le velo",
    "economisons-la",
)
# "A 0g CO2/km", "C126 gCO2/km", "ID.4 & ID.3 A 0 g CO₂/km"
CO2_DISPLAY = re.compile(r"\d\s*g\s*co2\s*/\s*km")


def exclusion_reason(quote: str) -> str | None:
    q = normalize(quote, keep_punctuation=True)
    if any(m in q for m in MANDATORY_MESSAGES):
        return "mandatory_message"
    if CO2_DISPLAY.search(q):
        return "co2_display"
    return None
