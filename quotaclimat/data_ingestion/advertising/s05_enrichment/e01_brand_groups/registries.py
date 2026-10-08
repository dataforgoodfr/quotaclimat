"""Company -> ultimate parent company, from public registries without API key:
- GLEIF (https://api.gleif.org/api/v1): LEI of a French company by its SIREN, and its ultimate accounting
  consolidating parent when published (many companies report NON_PUBLIC or NON_CONSOLIDATING instead);
- Wikidata: top of the chain of parent organizations (P749) of the item whose SIREN (P1616) is the
  company's.
"""

import logging
import time
from dataclasses import dataclass

import requests

GLEIF_URL = "https://api.gleif.org/api/v1"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = "QuotaClimat-brand-groups/1.0 (https://www.quotaclimat.org)"
TIMEOUT_SEC = 60


@dataclass
class Company:
    name: str
    identifier: str  # LEI or Wikidata QID


def _get(url: str, delay_sec: float, **kwargs) -> requests.Response:
    time.sleep(delay_sec)
    return requests.get(url, headers={"User-Agent": USER_AGENT, **kwargs.pop("headers", {})}, timeout=TIMEOUT_SEC, **kwargs)


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
    return Company(name=records[0]["attributes"]["entity"]["legalName"]["name"], identifier=records[0]["id"])


def gleif_lei(siren: str, delay_sec: float = 1.0) -> Company | None:
    """LEI record of the French company registered under this SIREN (registry Sirene)."""
    return _single_lei(
        {"filter[entity.registeredAs]": siren, "filter[entity.jurisdiction]": "FR"}, f"SIREN {siren}", delay_sec
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
    return Company(name=data["attributes"]["entity"]["legalName"]["name"], identifier=data["id"])


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
        SELECT DISTINCT ?parent ?parentLabel WHERE {{
          ?item wdt:P1616 "{siren}" ;
                wdt:P749+ ?parent .
          FILTER NOT EXISTS {{ ?parent wdt:P749 ?above }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr,en,mul". }}
        }}
    """
    response = _get(
        WIKIDATA_SPARQL_URL, delay_sec,
        params={"query": query, "format": "json"}, headers={"Accept": "application/sparql-results+json"},
    )
    response.raise_for_status()
    bindings = response.json().get("results", {}).get("bindings", [])
    if len(bindings) != 1:
        if bindings:
            logging.warning("Wikidata: %s ultimate parents for SIREN %s, none kept", len(bindings), siren)
        return None
    qid = bindings[0]["parent"]["value"].rsplit("/", 1)[-1]
    label = bindings[0].get("parentLabel", {}).get("value", "")
    return Company(name="" if label == qid else label, identifier=qid)
