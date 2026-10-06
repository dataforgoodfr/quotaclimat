"""INPI "Diffusion PI" API (https://api-gateway.inpi.fr/services/apidiffusion): trademark search and
trademark notices (ST66 XML), to find the current holder (name, SIREN) and the Nice classes of a brand.

Only French trademarks (collection FR) are used: their notices give the SIREN of the holder, the
international (WO) and European (EU) ones do not.
"""

import logging
import os
import re
import time
from collections import Counter
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
# header of the 429 answers: seconds to wait before the quota allows requests again
RETRY_AFTER_HEADER = "x-rate-limit-retry-after-seconds"
RATE_LIMIT_HEADERS_PREFIX = "x-rate-limit"
# longest quota wait accepted during a run; longer, the run stops and the remaining brands are left for
# the next run
MAX_QUOTA_WAIT_SEC = int(os.environ.get("INPI_MAX_QUOTA_WAIT_SEC", "600"))
SEARCH_PAGE_SIZE = 100
# results read at most per brand: the search returns every trademark containing the brand's words, most
# recent first, and the trademark named exactly like the brand can be an old one (ALAIN AFFLELOU, 1986:
# after more than 200 other ones)
SEARCH_MAX_RESULTS = int(os.environ.get("INPI_SEARCH_MAX_RESULTS", "1000"))
# notices read at most per brand and collection group: one request each, they make most of the requests
# of a run and the quota is about 100 requests (x-rate-limit-remaining 87 at the start of a test run)
MAX_NOTICES = int(os.environ.get("INPI_MAX_NOTICES", "3"))
REMAINING_HEADER = "x-rate-limit-remaining"

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


# words of the statuses of the trademarks no longer in force, for the European (EU, in English) and
# international (WO) ones, whose statuses differ from the French ones
DEAD_STATUS_WORDS = (
    "withdrawn", "expired", "cancel", "refused", "rejected", "surrender", "lapsed", "removed", "invalid",
    "revoked", "expiree", "retrait", "renonciation", "annulee", "dechue", "rejetee",
)
# search order: the French trademarks first, which give the SIREN of French holders; the European and
# international ones (e.g. Volkswagen, Amazon) only when no French one is found
COLLECTION_GROUPS = (("FR",), ("EU", "WO"))


def is_alive(status: str | None) -> bool:
    """Trademark still in force. The international (WO) results have no status: kept."""
    if status is None:
        return True
    key = name_key(status.replace("_", " "))
    if key in ALIVE_STATUSES:
        return True
    return not any(word in key for word in DEAD_STATUS_WORDS)


@dataclass
class SearchResult:
    application_number: str
    mark: str
    status: str | None
    applicant: str | None
    # number of the notice, with its collection prefix: FR5189659, EU19197838, WO1892865
    notice_number: str = ""


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
    # number with its collection prefix (FR5189659, EU19197838, WO1892865), set from the search result
    notice_number: str = ""


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
            href = next((e.get("href") for e in result.iter() if _local(e.tag) == "xml"), None) or ""
            results.append(SearchResult(
                application_number=values["ApplicationNumber"],
                mark=values.get("Mark") or "",
                status=values.get("MarkCurrentStatusCode"),
                applicant=values.get("DEPOSANT"),
                notice_number=href.rsplit("/", 1)[-1] if "/notice/" in href else f"FR{values['ApplicationNumber']}",
            ))
    return results


def parse_search_count(xml: str | bytes) -> int:
    """Total number of results of POST /search (metadata/count), all pages."""
    count = _text(_find(ET.fromstring(xml), "count"))
    return int(count) if count and count.isdigit() else 0


# legal forms of companies, as words of a holder name: a holder without person type nor legal entity in
# its notice (EU) is kept only when its name looks like a company's (natural persons are never kept)
COMPANY_NAME_WORDS = {
    "sa", "sas", "sasu", "sarl", "eurl", "sca", "snc", "scs", "se", "ag", "gmbh", "kg", "kgaa", "inc", "llc",
    "ltd", "limited", "plc", "corp", "corporation", "company", "co", "bv", "nv", "spa", "srl", "ab", "as",
    "oy", "oyj", "aps", "sl", "aktiengesellschaft", "group", "groupe", "holding", "societe",
}
NATURAL_PERSON_WORDS = ("natural", "physique", "individual person")


def looks_like_company(name: str | None) -> bool:
    words = set(normalize(name).split())
    return bool(words & COMPANY_NAME_WORDS)


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
        # name: OrganizationName (FR), else the free format name lines (WO: "Volkswagen Aktiengesellschaft")
        organization = _text(_find(holder, "OrganizationName"))
        free_lines = [_text(e) for e in holder.iter() if _local(e.tag) == "FreeFormatNameLine" and _text(e)]
        holder_name = organization or " ".join(free_lines) or None
        # company: PersonType PM in the French notices (PP: natural person); else a legal entity that is not
        # a natural person (WO: "Joint Stock Company"), an organization name, or a company-like name
        person_type = (holder.get("PersonType") or "").strip().lower()
        legal_entity = next(
            (_text(e) for e in holder.iter() if _local(e.tag).endswith("LegalEntity") and _text(e)), None
        )
        is_natural = person_type == "pp" or any(w in (legal_entity or "").lower() for w in NATURAL_PERSON_WORDS)
        holder_is_company = person_type == "pm" or (
            not is_natural and (organization is not None or legal_entity is not None or looks_like_company(holder_name))
        )
        holder_country = _text(_find(holder, "AddressCountryCode")) or next(
            (_text(e) for e in holder.iter() if _local(e.tag).endswith(("IncorporationState", "IncorporationCountryCode")) and _text(e)),
            None,
        )
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


class InpiQuotaExceeded(RuntimeError):
    """HTTP 429 with a wait longer than MAX_QUOTA_WAIT_SEC, or still 429 after waiting: the following
    requests would fail too."""


def _rate_limit_headers(response) -> dict[str, str]:
    """x-rate-limit-* headers of an answer (quota information of the API gateway)."""
    return {k.lower(): v for k, v in (getattr(response, "headers", None) or {}).items() if k.lower().startswith(RATE_LIMIT_HEADERS_PREFIX)}


def _retry_after(response) -> int | None:
    value = str(_rate_limit_headers(response).get(RETRY_AFTER_HEADER, "")).strip()
    return int(float(value)) if value.replace(".", "", 1).isdigit() else None


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
        # HTTP requests sent to the INPI by kind (login, search, notice), to know how many the quota allows
        self.request_counts: Counter = Counter()
        self._rate_limit_headers_logged = False
        # requests left in the quota, from the last answer that gave it
        self.rate_limit_remaining: str | None = None

    @property
    def requests_summary(self) -> str:
        total = sum(self.request_counts.values())
        details = ", ".join(f"{kind} {n}" for kind, n in sorted(self.request_counts.items()))
        summary = f"{total} INPI requests ({details})" if total else "0 INPI requests"
        if self.rate_limit_remaining is not None:
            summary += f", {self.rate_limit_remaining} left in the quota"
        return summary

    def _count(self, url: str) -> None:
        kind = "search" if url.endswith("/search") else "notice" if "/notice/" in url else "login"
        self.request_counts[kind] += 1

    def _set_xsrf_header(self) -> None:
        """The XSRF token of the cookie is sent back in the X-XSRF-TOKEN header (the cookie may change)."""
        token = self.session.cookies.get(XSRF_COOKIE)
        if not token:
            raise RuntimeError("INPI: no XSRF-TOKEN cookie")
        self.session.headers[XSRF_HEADER] = token

    def login(self) -> None:
        """XSRF cookie first (the 401 answer is expected), then login: HttpOnly session cookies. The cookies
        of a previous session are dropped first."""
        self.session.cookies.clear()
        self.session.headers.pop(XSRF_HEADER, None)
        self._count(AUTHENTICATE_URL)
        self.session.get(AUTHENTICATE_URL, timeout=TIMEOUT_SEC)
        self._set_xsrf_header()
        self._count(LOGIN_URL)
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
            self._count(url)
            response = self.session.request(method, url, timeout=TIMEOUT_SEC, **kwargs)
            if not self._rate_limit_headers_logged and _rate_limit_headers(response):
                # once per run: the quota information the gateway sends, if any
                logging.info("INPI rate limit headers: %s", _rate_limit_headers(response))
                self._rate_limit_headers_logged = True
            remaining = _rate_limit_headers(response).get(REMAINING_HEADER)
            if remaining is not None:
                self.rate_limit_remaining = remaining
            if response.status_code == 401 and attempt == 0:
                self.login()  # session expired
                continue
            if response.status_code == 429:
                wait = _retry_after(response)
                logging.warning(
                    "INPI quota exceeded after %s, retry after %s s, headers %s",
                    self.requests_summary, wait, _rate_limit_headers(response),
                )
                if wait is None:
                    wait = 30 * 2 ** attempt
                if wait > MAX_QUOTA_WAIT_SEC:
                    raise InpiQuotaExceeded(
                        f"INPI quota exceeded after {self.requests_summary}: retry after {wait} s "
                        f"({wait / 3600:.1f} h), more than INPI_MAX_QUOTA_WAIT_SEC={MAX_QUOTA_WAIT_SEC}"
                    )
                time.sleep(wait + 1)
                continue
            _raise_for_status(response)
            return response
        if response.status_code == 429:
            raise InpiQuotaExceeded(f"INPI quota still exceeded after waiting, after {self.requests_summary}")
        _raise_for_status(response)
        return response

    def search(
        self,
        brand: str,
        collections: tuple[str, ...] = ("FR",),
        size: int = SEARCH_PAGE_SIZE,
        max_results: int = SEARCH_MAX_RESULTS,
    ) -> list[SearchResult]:
        """Trademarks whose name contains the brand as an exact phrase (Solr search of the API: without
        the quotes every word is searched separately, more than 1000 results for "Comme J'aime"), all
        pages up to max_results."""
        term = search_term(brand)
        if not term:
            return []
        results: list[SearchResult] = []
        position = 0
        while position < max_results:
            response = self._request("POST", f"{API_URL}/search", headers={"Accept": "application/xml"}, json={
                "collections": list(collections),
                "query": f'[Mark="{term}"]',
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

    def notice(self, number: str) -> Notice:
        """Notice by its number with its collection prefix (FR5189659, EU19197838, WO1892865); a number
        without prefix is a French one."""
        number = number if number[:2].isalpha() else f"FR{number}"
        return parse_notice(self._request("GET", f"{API_URL}/notice/{number}").content)

    def brand_notices(self, brand: str, max_notices: int = MAX_NOTICES) -> list[Notice]:
        """Notices of the French trademarks in force named exactly like the brand (name_key), most recent
        first, whose holder is a company (natural persons are never kept). A company registered abroad
        (e.g. Inter IKEA Systems B.V.) has no SIREN: it is kept, identified by its name."""
        key = name_key(brand)
        for collections in COLLECTION_GROUPS:
            results = self.search(brand, collections)
            exact = [r for r in results if name_key(r.mark) == key]
            alive = [r for r in exact if is_alive(r.status)]
            notices = []
            for r in alive[:max_notices]:
                notice = self.notice(r.notice_number)
                notice.notice_number = r.notice_number
                if not notice.holder_name and r.applicant:
                    # some EU notices are update records with the holder identifier only: the holder of
                    # the search result, kept only when its name looks like a company's
                    notice.holder_name = r.applicant
                    notice.holder_is_company = looks_like_company(r.applicant)
                notices.append(notice)
            kept = [n for n in notices if n.holder_is_company and (n.holder_siren or n.holder_name)]
            # how far each step goes, to understand the brands without result
            logging.info(
                "INPI %s %r: %s results, %s named exactly like it, %s in force, %s held by a company",
                "+".join(collections), brand, len(results), len(exact), len(alive), len(kept),
            )
            if kept:
                return kept
        return []
