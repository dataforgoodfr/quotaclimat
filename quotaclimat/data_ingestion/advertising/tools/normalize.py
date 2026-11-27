import re
import unicodedata

_CHARACTERS = str.maketrans(
    {
        "’": "'",
        "‘": "'",
        "`": "'",
        "´": "'",
        "–": "-",
        "—": "-",
        "‐": "-",
        "‑": "-",
        " ": " ",
        " ": " ",
        " ": " ",
        "œ": "oe",
        "Œ": "oe",
        "æ": "ae",
        "Æ": "ae",
        **dict(zip("₀₁₂₃₄₅₆₇₈₉²³", "012345678923")),
    }
)
_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


# Without punctuation: brand keys, which the dbt macro name_key.sql also computes in SQL
# (change both together). With it: ad texts, before looking for phrases in them.
def normalize(text: str | None, keep_punctuation: bool = False) -> str:
    if not text:
        return ""
    text = strip_accents(str(text).translate(_CHARACTERS)).lower()
    if not keep_punctuation:
        text = _PUNCT_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def nospace(text: str) -> str:
    return text.replace(" ", "")
