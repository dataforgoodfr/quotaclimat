"""INPI "Diffusion PI" API (https://api-gateway.inpi.fr/services/apidiffusion): trademark search and
trademark notices (ST66 XML), to find the current holder (name, SIREN) and the Nice classes of a brand.

Only French trademarks (collection FR) are used: their notices give the SIREN of the holder, the
international (WO) and European (EU) ones do not.
"""

import logging
import os
import re
import time
import xml.etree.ElementTree as ET  # responses of the INPI API only (expat refuses entity expansion attacks)
from dataclasses import dataclass, field

import requests

from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.normalize import normalize, nospace

API_URL = "https://api-gateway.inpi.fr/services/apidiffusion/api/marques"
# login of the API gateway (checked with curl): GET AUTHENTICATE_URL sets the XSRF-TOKEN cookie (with a
# 401 answer), then POST LOGIN_URL with this token in the X-XSRF-TOKEN header sets the session cookies
AUTHENTICATE_URL = "https://api-gateway.inpi.fr/services/uaa/api/authenticate"
LOGIN_URL = os.environ.get("INPI_LOGIN_URL", "https://api-gateway.inpi.fr/auth/login")
XSRF_COOKIE = "XSRF-TOKEN"
XSRF_HEADER = "X-XSRF-TOKEN"
TIMEOUT_SEC = 60
MAX_RETRIES = 3
SEARCH_PAGE_SIZE = 100
# results read at most per brand: the search returns every trademark containing the brand's words, most
# recent first, and the trademark named exactly like the brand can be an old one (ALAIN AFFLELOU, 1986:
# after more than 200 other ones)
SEARCH_MAX_RESULTS = int(os.environ.get("INPI_SEARCH_MAX_RESULTS", "1000"))

# current status of a trademark still in force (MarkCurrentStatusCode, as name_key: the API returns either
# the label "Marque enregistrée" or the enum value "MARQUE_ENREGISTRÉE")
ALIVE_STATUSES = {
    "demandedeposee", "demandepubliee", "demandenonpubliee", "marqueenregistree",
    "renouvellementdemande", "marquerenouvelee",
}


def name_key(name: str | None) -> str:
    """Same key as the name_key dbt macro: 'L’Oréal Paris' -> 'lorealparis'."""
    return nospace(normalize(name))


def search_term(brand: str) -> str:
    """Brand as a term of the INPI Solr query: punctuation replaced by spaces ("Comme J'aime" ->
    "Comme J aime"), as apostrophes, brackets or quotes break the query (HTTP 500). The exact name is
    checked afterwards with name_key, which ignores punctuation too."""
    return " ".join(re.sub(r"[^\w\s]|_", " ", brand).split())


def is_alive(status: str | None) -> bool:
    return name_key((status or "").replace("_", " ")) in ALIVE_STATUSES


@dataclass
class SearchResult:
    application_number: str
    mark: str
    status: str | None
    applicant: str | None


@dataclass
class Notice:
    application_number: str
    mark: str | None
    feature: str | None  # Word, Figurative...
    status: str | None
    holder_name: str | None
    holder_siren: str | None
    holder_is_company: bool
    classes: set[int] = field(default_factory=set)
    # country of the holder's address (FR, NL...): the holders registered abroad have no SIREN
    holder_country: str | None = None


def _local(tag: str) -> str:
    """Tag without XML namespace."""
    return tag.rsplit("}", 1)[-1]


def _find(element, *path: str):
    """First descendant following the path of local tag names (namespace-agnostic)."""
    for name in path:
        if element is None:
            return None
        element = next((child for child in element.iter() if child is not element and _local(child.tag) == name), None)
    return element


def _text(element) -> str | None:
    if element is None or element.text is None:
        return None
    return element.text.strip() or None


def parse_search(xml: str | bytes) -> list[SearchResult]:
    """Results of POST /search (XML), one per trademark."""
    root = ET.fromstring(xml)
    results = []
    for result in (e for e in root.iter() if _local(e.tag) == "result"):
        values = {}
        for f in (e for e in result.iter() if _local(e.tag) == "field"):
            # fields are repeated in the response: keep the first value
            values.setdefault(f.get("name"), _text(_find(f, "value")))
        if values.get("ApplicationNumber"):
            results.append(SearchResult(
                application_number=values["ApplicationNumber"],
                mark=values.get("Mark") or "",
                status=values.get("MarkCurrentStatusCode"),
                applicant=values.get("DEPOSANT"),
            ))
    return results


def parse_search_count(xml: str | bytes) -> int:
    """Total number of results of POST /search (metadata/count), all pages."""
    count = _text(_find(ET.fromstring(xml), "count"))
    return int(count) if count and count.isdigit() else 0


def parse_notice(xml: str | bytes) -> Notice:
    """ST66 notice of GET /notice/{number}. The holder is the current holder (fr-CurrentHolder, which
    follows transfers), else the applicant."""
    root = ET.fromstring(xml)
    holder = _find(root, "fr-CurrentHolder")
    if holder is None:
        holder = _find(root, "Applicant")
    holder_name = holder_siren = holder_country = None
    holder_is_company = False
    if holder is not None:
        holder_is_company = holder.get("PersonType") == "PM"
        holder_name = _text(_find(holder, "OrganizationName"))
        holder_country = _text(_find(holder, "AddressCountryCode"))
        for e in holder.iter():
            if _local(e.tag) in ("fr-CurrentHolderIdentifier", "ApplicantIdentifier") and e.get("identifierKindCode") == "FR":
                holder_siren = _text(e)
                break
    classes = set()
    goods = _find(root, "GoodsServicesDetails")
    if goods is not None:
        for e in goods.iter():
            if _local(e.tag) == "ClassNumber" and (_text(e) or "").isdigit():
                classes.add(int(_text(e)))
    return Notice(
        application_number=_text(_find(root, "ApplicationNumber")) or "",
        mark=_text(_find(root, "MarkVerbalElementText")),
        feature=_text(_find(root, "MarkFeature")),
        status=_text(_find(root, "MarkCurrentStatusCode")),
        holder_name=holder_name,
        holder_siren=holder_siren,
        holder_is_company=holder_is_company,
        classes=classes,
        holder_country=holder_country,
    )


def _raise_for_status(response: requests.Response) -> None:
    """HTTP error with the status and the beginning of the INPI answer, for the logs."""
    if response.status_code >= 400:
        endpoint = (getattr(response, "url", "") or "").rsplit("/", 1)[-1]
        text = getattr(response, "text", "") or ""
        raise requests.HTTPError(f"INPI {endpoint}: HTTP {response.status_code} {text[:300]!r}", response=response)


class InpiClient:
    def __init__(self, username: str, password: str, delay_sec: float = 0.5, session: requests.Session | None = None):
        self.username = username
        self.password = password
        self.delay_sec = delay_sec
        self.session = session or requests.Session()
        self.logged_in = False

    def _set_xsrf_header(self) -> None:
        """The XSRF token of the cookie is sent back in the X-XSRF-TOKEN header (the cookie may change)."""
        token = self.session.cookies.get(XSRF_COOKIE)
        if not token:
            raise RuntimeError("INPI: no XSRF-TOKEN cookie")
        self.session.headers[XSRF_HEADER] = token

    def login(self) -> None:
        """XSRF cookie first (the 401 answer is expected), then login: HttpOnly session cookies."""
        self.session.get(AUTHENTICATE_URL, timeout=TIMEOUT_SEC)
        self._set_xsrf_header()
        response = self.session.post(
            LOGIN_URL,
            json={"username": self.username, "password": self.password, "rememberMe": True},
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()
        self._set_xsrf_header()
        self.logged_in = True

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        if not self.logged_in:
            self.login()
        for attempt in range(MAX_RETRIES):
            time.sleep(self.delay_sec)
            self._set_xsrf_header()
            response = self.session.request(method, url, timeout=TIMEOUT_SEC, **kwargs)
            if response.status_code == 401 and attempt == 0:
                self.login()  # session expired
                continue
            if response.status_code == 429:
                wait = 30 * 2 ** attempt
                logging.warning("INPI quota exceeded, waiting %s s", wait)
                time.sleep(wait)
                continue
            _raise_for_status(response)
            return response
        _raise_for_status(response)
        return response

    def search(
        self, brand: str, size: int = SEARCH_PAGE_SIZE, max_results: int = SEARCH_MAX_RESULTS
    ) -> list[SearchResult]:
        """French trademarks whose name contains the brand (Solr search of the API), all pages up to
        max_results."""
        term = search_term(brand)
        if not term:
            return []
        results: list[SearchResult] = []
        position = 0
        while position < max_results:
            response = self._request("POST", f"{API_URL}/search", headers={"Accept": "application/xml"}, json={
                "collections": ["FR"],
                "query": f"[Mark={term}]",
                "fields": ["ApplicationNumber", "Mark", "MarkCurrentStatusCode", "DEPOSANT"],
                "position": position,
                "size": size,
            })
            page = parse_search(response.content)
            results += page
            position += size
            count = parse_search_count(response.content)
            if not page or position >= count:
                break
        else:
            logging.warning("INPI search %r: more than %s results, the next ones are not read", term, max_results)
        return results

    def notice(self, application_number: str) -> Notice:
        number = application_number if application_number.startswith("FR") else f"FR{application_number}"
        return parse_notice(self._request("GET", f"{API_URL}/notice/{number}").content)

    def brand_notices(self, brand: str, max_notices: int = 10) -> list[Notice]:
        """Notices of the French trademarks in force named exactly like the brand (name_key), most recent
        first, whose holder is a company (natural persons are never kept). A company registered abroad
        (e.g. Inter IKEA Systems B.V.) has no SIREN: it is kept, identified by its name."""
        key = name_key(brand)
        matches = [r for r in self.search(brand) if name_key(r.mark) == key and is_alive(r.status)]
        notices = [self.notice(r.application_number) for r in matches[:max_notices]]
        return [n for n in notices if n.holder_is_company and (n.holder_siren or n.holder_name)]
