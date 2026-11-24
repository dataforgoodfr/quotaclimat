"""A fixed list of generic green terms, looked for in an ad's texts"""

import re
import sys

import regex

from quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.review_schema import (
    evidence_items,
)
from quotaclimat.data_ingestion.advertising.tools.normalize import (
    normalize,
)

VERSION = "terms-1.2"  # bump whenever a pattern changes
CONTEXT = 40  # chars kept either side of a match
SEP = r"[\s'-]*"


def dehyphenate(text: str) -> str:
    """Line-wrapped end-card text: ``recy- clable`` -> ``recyclable``."""
    return re.sub(r"(?<=\w)-\s*(?=\w)", "", text)


def fz(word: str) -> str:
    """One typo allowed. For long, distinctive words only."""
    return rf"(?:{word}){{e<=1}}"


def phrase(*words: str) -> str:
    return SEP.join(f"(?:{w})" for w in words)


ENV = fz("environnement")
PLANETE = "planetes?"
RESPECTUEUX = fz("respectueu(?:x|se|ses)")
BON = "bon(?:ne)?s?"
MEILLEUR = "meilleure?s?"
CARBONE = fz("carbone")
COMPENSE = fz("compense(?:e|s|es)?")
ECOLOGIQUE = fz("ecologique(?:s|ment)?")

# {term: (pattern, excluded phrases)}
TERMS: dict[str, tuple[str, list[str]]] = {
    "respectueux de l'environnement": (phrase(RESPECTUEUX, "de", "l", ENV), []),
    "respectueux de la nature": (phrase(RESPECTUEUX, "de", "la", "nature"), []),
    "respectueux de la planète": (phrase(RESPECTUEUX, "de", "la", PLANETE), []),
    "vert": (
        "vert(?:e|s|es)?",
        [
            "feux? verts?",
            "numeros? verts?",
            "citrons? verts?",
            "the vert",
            "haricots? verts?",
            "espaces? verts?",
            "cap vert",
            "portes? ou vertes?",  # OCR splitting «portes ouvertes»
            "verte?s? (?:clair|fonce|pomme|sapin|olive|emeraude|anis|kaki|menthe|d eau)",
        ],
    ),
    "ami de la nature": (phrase("amie?s?", "de", "la", "nature"), []),
    "écologique": (
        ECOLOGIQUE,
        [f"bonus {ECOLOGIQUE}", f"malus {ECOLOGIQUE}", f"transition {ECOLOGIQUE}"],
    ),
    "écolo": ("ecolos?", []),
    "bon pour l'environnement": (phrase(BON, "pour", "l", ENV), []),
    "bon pour le climat": (phrase(BON, "pour", "le", "climat"), []),
    "bon pour la planète": (phrase(BON, "pour", "la", PLANETE), []),
    "favorable à l'environnement": (
        phrase(fz("favorables?"), "a", "l", ENV),
        [],
    ),
    "à faible intensité de carbone": (
        phrase("(?:a)?", "faibles?", fz("intensite"), "(?:de|en)?", CARBONE),
        [],
    ),
    "économe en énergie": (phrase(fz("economes?"), "en", fz("energie")), []),
    "biodégradable": (fz(f"bio{SEP}degradab(?:le|les|ilite)"), []),
    "biosourcé": (fz(f"bio{SEP}source(?:e|s|es)?"), []),
    "écoresponsable": (fz(f"eco{SEP}responsab(?:le|les|ilite)"), []),
    "bio-responsable": (fz(f"bio{SEP}responsab(?:le|les|ilite)"), []),
    "bio-compatible": (fz(f"bio{SEP}compatib(?:le|les|ilite)"), []),
    "se dégrade dans l'environnement": (
        phrase("se", fz("degrade(?:nt)?"), "dans", "l", ENV),
        [],
    ),
    "ne laisse pas de résidu en fin de vie": (
        # ne is often dropped in speech
        phrase(
            "(?:ne)?",
            "laisse(?:nt)?",
            "pas",
            "de",
            fz("residus?"),
            "en",
            "fin",
            "de",
            "vie",
        ),
        [],
    ),
    "s'élimine de manière naturelle": (
        phrase("s", fz("elimine(?:nt)?"), "de", fz("maniere"), fz("naturelle")),
        [],
    ),
    "meilleur pour l'environnement": (phrase(MEILLEUR, "pour", "l", ENV), []),
    "meilleur pour la planète": (phrase(MEILLEUR, "pour", "la", PLANETE), []),
    # The typo goes on the stem: «recycler», «recyclage» must not read as «recyclé».
    "recyclé": (fz("recycl") + "e(?:e|s|es)?", []),
    "recyclable": (fz("recyclab") + "(?:le|les|ilite)", []),
    "x% recyclé": (
        r"\d{1,3}\s?%\s*(?:de\s+\w+\s+)?" + fz("recycl") + "e(?:e|s|es)?",
        [],
    ),
    "zéro déchet": (phrase("(?:zero|0)", fz("dechets?")), []),
    "neutre en carbone": (
        phrase("neutres?", "en", CARBONE)
        + "|"
        + phrase(CARBONE, "neutres?")
        + "|"
        + phrase("neutralite", CARBONE),
        [],
    ),
    "compensé carbone": (
        phrase(COMPENSE, "(?:en)?", CARBONE) + "|" + phrase(CARBONE, COMPENSE),
        [],
    ),
}

# {noisy term: phrases in which its hits are not noisy}
NOISY: dict[str, list[str]] = {
    "vert": [
        r"(?:electricite|energie|gaz|hydrogene|carburant|courant|offre|economie"
        r"|croissance|finance|obligation|chimie|technologie|industrie|mobilite"
        r"|voiture|vehicule|acier|beton|plastique)s? (?:100\s?% )?verte?s?",
    ],
}

_BOUNDED = r"(?<![a-z0-9])(?:{})(?![a-z0-9])"


def _compile(pattern: str):
    # ENHANCEMATCH: prefer the fit with the fewest errors.
    return regex.compile(_BOUNDED.format(pattern.replace(" ", SEP)), regex.ENHANCEMATCH)


_COMPILED = {
    term: (
        _compile(pattern),
        [_compile(p) for p in excludes],
        [_compile(p) for p in NOISY.get(term, [])],
    )
    for term, (pattern, excludes) in TERMS.items()
}


def _spans(rx, text: str) -> list[tuple[int, int]]:
    return [m.span() for m in rx.finditer(text)]


def _inside(a: int, b: int, spans: list[tuple[int, int]]) -> bool:
    return any(x <= a and b <= y for x, y in spans)


def _match(text: str, source: str, brand: str) -> list[dict]:
    brand_spans = (
        _spans(regex.compile(regex.escape(brand)), text) if len(brand) >= 3 else []
    )
    hits = []
    for term, (rx, excludes, clear) in _COMPILED.items():
        blocked = brand_spans + [s for ex in excludes for s in _spans(ex, text)]
        sure = [s for c in clear for s in _spans(c, text)]
        for m in rx.finditer(text):
            a, b = m.span()
            if _inside(a, b, blocked):
                continue
            hits.append(
                {
                    "term": term,
                    "source": source,
                    "match": m.group(0),
                    "errors": sum(m.fuzzy_counts),
                    "noisy": term in NOISY and not _inside(a, b, sure),
                    "context": text[max(0, a - CONTEXT) : b + CONTEXT],
                }
            )
    return hits


def find_terms(text: str | None, source: str, brand: str | None = None) -> list[dict]:
    """Every term hit in ``text``; ``source`` is audio | ocr | claim | evidence."""
    norm = normalize(text, keep_punctuation=True)
    if not norm:
        return []
    brand_norm = normalize(brand, keep_punctuation=True)
    hits = _match(norm, source, brand_norm)
    rejoined = dehyphenate(norm)
    if rejoined != norm:
        seen = {h["term"] for h in hits}
        hits += [
            h for h in _match(rejoined, source, brand_norm) if h["term"] not in seen
        ]
    return hits


def scan_ad(
    transcript: str | None,
    ocr_text: str | None,
    record: dict,
    brand: str | None = None,
) -> dict:
    """The terms found in an ad's texts and in its final record's claims and evidence."""
    hits = find_terms(transcript, "audio", brand) + find_terms(ocr_text, "ocr", brand)
    for claim in record.get("I3_claims") or []:
        hits += find_terms(claim.get("quote"), "claim", brand)
    for _, item in evidence_items(record):
        hits += find_terms(item.get("text"), "evidence", brand)
    return {"terms": sorted({h["term"] for h in hits}), "hits": hits}


if __name__ == "__main__":
    for h in find_terms(" ".join(sys.argv[1:]) or sys.stdin.read(), "cli"):
        print(f"{h['term']:40} {h['errors']} {h['match']!r:35} …{h['context']}…")
