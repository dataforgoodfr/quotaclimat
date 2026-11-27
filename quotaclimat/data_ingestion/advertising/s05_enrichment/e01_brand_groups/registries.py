"""Company -> ultimate parent company and head office department, from public registries without API key:
- GLEIF (https://api.gleif.org/api/v1): LEI of a French company by its SIREN, and its ultimate accounting
  consolidating parent when published (many companies report NON_PUBLIC or NON_CONSOLIDATING instead),
  with the parent's registration number (its SIREN in France) and country;
- Wikidata: top of the chain of parent organizations (P749) of the item whose SIREN (P1616) is the
  company's, with the parent's SIREN and country (P17);
- API Recherche d'entreprises (https://recherche-entreprises.api.gouv.fr, Sirene data of the INSEE):
  department of the head office (siège) of a company by its SIREN.
"""

import logging
import time
from dataclasses import dataclass

import requests

GLEIF_URL = "https://api.gleif.org/api/v1"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
RECHERCHE_ENTREPRISES_URL = "https://recherche-entreprises.api.gouv.fr"
USER_AGENT = "QuotaClimat-brand-groups/1.0 (https://www.quotaclimat.org)"
TIMEOUT_SEC = 60
# answers of an overloaded service (e.g. HTTP 502 of the Wikidata query service), retried after a wait
RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3
RETRY_WAIT_SEC = 10


@dataclass
class Company:
    name: str
    identifier: str  # LEI or Wikidata QID
    siren: str = ""  # French companies only
    country: str = ""  # ISO 3166-1 alpha-2 code: FR, NL...


def siren_formats(siren: str) -> list[str]:
    """The ways GLEIF stores a SIREN in registeredAs: "552081317" and "552 081 317" (EDF)."""
    return [siren, f"{siren[:3]} {siren[3:6]} {siren[6:]}"]


def _company_from_lei_record(record: dict) -> Company:
    """Name, LEI, SIREN (registration number of a French company, spaces removed) and country of the legal
    address of a GLEIF LEI record."""
    entity = record["attributes"]["entity"]
    registered_as = "".join((entity.get("registeredAs") or "").split())
    french = (entity.get("jurisdiction") or "").startswith("FR")
    return Company(
        name=entity["legalName"]["name"],
        identifier=record["id"],
        siren=registered_as if french and registered_as.isdigit() and len(registered_as) == 9 else "",
        country=(entity.get("legalAddress") or {}).get("country") or "",
    )


def _get(url: str, delay_sec: float, **kwargs) -> requests.Response:
    """GET, tried again on an overloaded service answer or a network error, up to MAX_ATTEMPTS times."""
    headers = {"User-Agent": USER_AGENT, **kwargs.pop("headers", {})}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        time.sleep(delay_sec if attempt == 1 else RETRY_WAIT_SEC * (attempt - 1))
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT_SEC, **kwargs)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            if attempt == MAX_ATTEMPTS:
                raise
            logging.warning("%s: network error, attempt %s/%s", url.split("?")[0], attempt, MAX_ATTEMPTS)
            continue
        if response.status_code not in RETRY_STATUSES or attempt == MAX_ATTEMPTS:
            return response
        logging.warning("%s: HTTP %s, attempt %s/%s", url.split("?")[0], response.status_code, attempt, MAX_ATTEMPTS)
    return response


def _single_lei(params: dict, description: str, delay_sec: float) -> Company | None:
    """The only non-fund LEI record matching the filters, None when none or several."""
    response = _get(f"{GLEIF_URL}/lei-records", delay_sec, params=params)
    response.raise_for_status()
    records = [
        r for r in response.json().get("data", [])
        if r.get("attributes", {}).get("entity", {}).get("category") != "FUND"
    ]
    if len(records) != 1:
        if records:
            logging.warning("GLEIF: %s LEI records for %s, none kept", len(records), description)
        return None
    return _company_from_lei_record(records[0])


def gleif_lei(siren: str, delay_sec: float = 1.0) -> Company | None:
    """LEI record of the French company registered under this SIREN (registry Sirene), whether GLEIF
    stores it with or without spaces (a comma between two values is an OR in GLEIF filters)."""
    if not (siren.isdigit() and len(siren) == 9):
        return None
    return _single_lei(
        {"filter[entity.registeredAs]": ",".join(siren_formats(siren)), "filter[entity.jurisdiction]": "FR"},
        f"SIREN {siren}",
        delay_sec,
    )


def gleif_lei_by_name(name: str, country: str, delay_sec: float = 1.0) -> Company | None:
    """LEI record of a company registered abroad, by its exact legal name and the country of its legal
    address (companies without SIREN), when exactly one."""
    return _single_lei(
        {"filter[entity.legalName]": name, "filter[entity.legalAddress.country]": country},
        f"{name} ({country})",
        delay_sec,
    )


def _gleif_parent(lei: str, relation: str, delay_sec: float) -> Company | None:
    response = _get(f"{GLEIF_URL}/lei-records/{lei}/{relation}", delay_sec)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json().get("data")
    if not data:
        return None
    return _company_from_lei_record(data)


def gleif_lei_record(lei: str, delay_sec: float = 1.0) -> Company | None:
    """The LEI record itself (name, SIREN, country), None when unknown."""
    if not lei.isalnum():
        return None
    response = _get(f"{GLEIF_URL}/lei-records/{lei}", delay_sec)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json().get("data")
    return _company_from_lei_record(data) if data else None


def gleif_ultimate_parent(lei: str, delay_sec: float = 1.0) -> Company | None:
    """Ultimate accounting consolidating parent (the highest one), None when not published or when the
    company is its own ultimate parent."""
    return _gleif_parent(lei, "ultimate-parent", delay_sec)


def wikidata_ultimate_parent(siren: str, delay_sec: float = 1.0) -> Company | None:
    """Top of the chain of parent organizations (P749, followed up to an item without parent) of the
    Wikidata item with this SIREN (P1616), when exactly one. Its label in French, English or the
    multilingual one (mul); name empty when it has none (the label service then returns the QID)."""
    if not siren.isdigit():
        return None
    query = f"""
        SELECT DISTINCT ?parent ?parentLabel ?siren ?country WHERE {{
          ?item wdt:P1616 "{siren}" ;
                wdt:P749+ ?parent .
          FILTER NOT EXISTS {{ ?parent wdt:P749 ?above }}
          {_IDENTITY_PATTERN}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr,en,mul". }}
        }}
    """
    companies = _wikidata_companies(query, delay_sec)
    if len(companies) != 1:
        if companies:
            logging.warning("Wikidata: %s ultimate parents for SIREN %s, none kept", len(companies), siren)
        return None
    return companies[0]


# SIREN (P1616) and ISO country code (P297 of the country, P17) of ?parent, when it has them
_IDENTITY_PATTERN = (
    "OPTIONAL { ?parent wdt:P1616 ?siren } OPTIONAL { ?parent wdt:P17/wdt:P297 ?country }"
)


def _wikidata_companies(query: str, delay_sec: float) -> list[Company]:
    """One Company per ?parent of the answer (an item with several SIREN or countries gives several lines:
    the first 9-digit SIREN and the first country are kept)."""
    response = _get(
        WIKIDATA_SPARQL_URL, delay_sec,
        params={"query": query, "format": "json"}, headers={"Accept": "application/sparql-results+json"},
    )
    response.raise_for_status()
    companies: dict[str, Company] = {}
    for binding in response.json().get("results", {}).get("bindings", []):
        qid = binding["parent"]["value"].rsplit("/", 1)[-1]
        label = binding.get("parentLabel", {}).get("value", "")
        company = companies.setdefault(qid, Company(name="" if label == qid else label, identifier=qid))
        siren = "".join(binding.get("siren", {}).get("value", "").split())
        if not company.siren and siren.isdigit() and len(siren) == 9:
            company.siren = siren
        company.country = company.country or binding.get("country", {}).get("value", "")
    return list(companies.values())


def wikidata_item(qid: str, delay_sec: float = 1.0) -> Company | None:
    """Label, SIREN and country of a Wikidata item, None when unknown."""
    if not (qid.startswith("Q") and qid[1:].isdigit()):
        return None
    query = f"""
        SELECT ?parent ?parentLabel ?siren ?country WHERE {{
          BIND(wd:{qid} AS ?parent)
          ?parent wikibase:sitelinks ?sitelinks .
          {_IDENTITY_PATTERN}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr,en,mul". }}
        }}
    """
    companies = _wikidata_companies(query, delay_sec)
    return companies[0] if companies else None


def head_office_department(siren: str, delay_sec: float = 1.0) -> str | None:
    """Department code of the head office (siège) of the company registered under this SIREN, from the
    API Recherche d'entreprises ("75", "2A", "971"), None when the SIREN is not found or the head office
    has no French department (abroad)."""
    if not (siren.isdigit() and len(siren) == 9):
        return None
    response = _get(f"{RECHERCHE_ENTREPRISES_URL}/search", delay_sec, params={"q": siren, "per_page": 5})
    response.raise_for_status()
    # a full-text search: only the company with exactly this SIREN
    company = next((r for r in response.json().get("results", []) if r.get("siren") == siren), None)
    return ((company or {}).get("siege") or {}).get("departement") or None
