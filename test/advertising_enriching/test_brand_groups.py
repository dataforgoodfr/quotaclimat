"""Brand group proposals (quotaclimat/data_ingestion/advertising/s05_enrichment/e01_brand_groups), without any network:
the INPI responses are real ones (assets/), shortened, plus two results added to the search to test the
exact-name and status filters (FR5189659 "DIOR", 1000001 expired)."""

import io
from datetime import date
from pathlib import Path

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import run as run_module

import requests

from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import (
    registries,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.inpi import (
    InpiClient,
    InpiQuotaExceeded,
    Notice,
    looks_like_company,
    is_alive,
    parse_notice,
    name_key,
    parse_search,
    search_term,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.propose import (
    choose_holder,
    parse_nice_classes,
    propose_row,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.registries import (
    Company,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.run import (
    brands_to_process,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups.sheet import (
    BrandInventorySheet,
)

ASSETS = Path(__file__).parent / "assets"
TODAY = date(2026, 10, 2)


def test_parse_search():
    results = parse_search((ASSETS / "inpi_search_dior.xml").read_bytes())
    assert [(r.application_number, r.mark, r.status, r.applicant) for r in results] == [
        (
            "5284729",
            "NOTHING BUT DIOR, J’ADORE",
            "Demande publiée",
            "PARFUMS CHRISTIAN DIOR",
        ),
        (
            "5237808",
            "DIOR ADDICT TOFFEE GLOW",
            "Marque enregistrée",
            "PARFUMS CHRISTIAN DIOR",
        ),
        ("5189659", "DIOR", "Marque enregistrée", "CHRISTIAN DIOR COUTURE"),
        ("1000001", "Dior", "Marque expirée", "OLD HOLDER"),
    ]


def test_parse_notice_current_holder_not_representative():
    notice = parse_notice((ASSETS / "inpi_notice_FR5189659.xml").read_bytes())
    assert notice == Notice(
        application_number="5189659",
        mark="DIOR",
        feature="Word",
        status="Marque enregistrée",
        holder_name="CHRISTIAN DIOR COUTURE",
        holder_siren="612035832",
        holder_is_company=True,
        classes={20, 24, 30, 40, 41, 42},
        holder_country="FR",
    )


def test_parse_notice_holder_registered_abroad():
    notice = parse_notice((ASSETS / "inpi_notice_foreign_holder.xml").read_bytes())
    assert (notice.holder_name, notice.holder_siren, notice.holder_country, notice.holder_is_company) == (
        "Inter IKEA Systems B.V.", None, "NL", True,
    )


def test_parse_notice_applicant_when_no_current_holder():
    xml = """<TradeMark><ApplicationNumber>1</ApplicationNumber>
        <ApplicantDetails><Applicant PersonType="PP">
          <ApplicantIdentifier identifierKindCode="FR">123456789</ApplicantIdentifier>
        </Applicant></ApplicantDetails></TradeMark>"""
    notice = parse_notice(xml)
    assert (notice.holder_siren, notice.holder_is_company, notice.classes) == (
        "123456789",
        False,
        set(),
    )


def test_search_term_without_punctuation():
    # an apostrophe in the Solr query makes the INPI answer HTTP 500
    assert search_term("Comme J'aime") == "Comme J aime"
    assert search_term("L’Oréal [Paris]") == "L Oréal Paris"
    assert search_term('Mc "Donald\'s"') == "Mc Donald s"
    assert search_term("!!!") == ""


def test_search_without_term_sends_nothing():
    session = FakeInpiSession()
    assert InpiClient("user", "password", delay_sec=0, session=session).search("?!") == []
    assert session.requests == []


def test_search_error_message(monkeypatch):
    session = FakeInpiSession()
    error = FakeResponse(status_code=500)
    error.url = "https://api-gateway.inpi.fr/services/apidiffusion/api/marques/search"
    error.text = "Erreur inattendue, requête SolR corrompue."
    monkeypatch.setattr(session, "request", lambda method, url, timeout, **kwargs: error)
    client = InpiClient("user", "password", delay_sec=0, session=session)
    try:
        client.search("Dior")
        raise AssertionError("no error raised")
    except requests.HTTPError as e:
        assert str(e) == "INPI search: HTTP 500 'Erreur inattendue, requête SolR corrompue.'"


def test_quota_wait_from_retry_after_header_and_request_counts(monkeypatch):
    from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import inpi

    waits = []
    monkeypatch.setattr(inpi.time, "sleep", waits.append)
    session = FakeInpiSession()
    answers = iter([
        FakeResponse(status_code=429, headers={"X-Rate-Limit-Retry-After-Seconds": "5"}),
        FakeResponse(search_page([("1", "ACME")], count=1), headers={"X-Rate-Limit-Remaining": "86"}),
    ])
    monkeypatch.setattr(session, "request", lambda method, url, timeout, **kwargs: next(answers))
    client = InpiClient("user", "password", delay_sec=0, session=session)
    assert [r.mark for r in client.search("Acme")] == ["ACME"]
    # the wait of the header (plus one second), then the same request again
    assert 6 in waits
    assert dict(client.request_counts) == {"login": 2, "search": 2}
    # with the quota left given by the last answer
    assert client.requests_summary == "4 INPI requests (login 2, search 2), 86 left in the quota"


def test_quota_wait_too_long_stops(monkeypatch):
    from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import inpi

    waits = []
    monkeypatch.setattr(inpi.time, "sleep", waits.append)
    session = FakeInpiSession()
    monkeypatch.setattr(
        session, "request",
        lambda method, url, timeout, **kwargs: FakeResponse(
            status_code=429, headers={"x-rate-limit-retry-after-seconds": "7200"}
        ),
    )
    try:
        InpiClient("user", "password", delay_sec=0, session=session).search("Acme")
        raise AssertionError("no error raised")
    except InpiQuotaExceeded as e:
        assert str(e).startswith("INPI quota exceeded after 3 INPI requests (login 2, search 1): retry after 7200 s (2.0 h)")
    # no useless wait
    assert 7201 not in waits


def test_quota_still_exceeded_after_waiting(monkeypatch):
    from quotaclimat.data_ingestion.advertising.s05_enrichment.e01_brand_groups import inpi

    monkeypatch.setattr(inpi.time, "sleep", lambda seconds: None)
    session = FakeInpiSession()
    monkeypatch.setattr(session, "request", lambda method, url, timeout, **kwargs: FakeResponse(status_code=429))
    try:
        InpiClient("user", "password", delay_sec=0, session=session).search("Acme")
        raise AssertionError("no error raised")
    except InpiQuotaExceeded as e:
        assert "still exceeded" in str(e)


def test_is_alive():
    assert is_alive("Marque enregistrée")
    assert is_alive("MARQUE_RENOUVELÉE")
    assert is_alive("Demande publiée")
    assert not is_alive("Marque expirée")
    assert not is_alive("MARQUE_DÉCHUE")
    # international (WO) results have no status; European (EU) statuses are in English
    assert is_alive(None)
    assert is_alive("Registered")
    assert is_alive("Application published")
    assert not is_alive("Application withdrawn")
    assert not is_alive("Registration expired")
    assert not is_alive("Registration cancelled")


class FakeResponse:
    def __init__(self, content=b"", status_code=200, json_data=None, headers=None):
        self.content = content
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class FakeInpiSession:
    """As the API gateway: GET authenticate sets the XSRF cookie (401), the login requires it in the header
    and rotates it, then search and notices from the assets."""

    def __init__(self):
        self.cookies = {}
        self.headers = {}
        self.requests = []
        self.login_body = None

    def get(self, url, timeout):
        assert url.endswith("/services/uaa/api/authenticate")
        self.cookies["XSRF-TOKEN"] = "token1"
        return FakeResponse(status_code=401)

    def post(self, url, json, timeout):
        assert url.endswith("/auth/login")
        if self.headers.get("X-XSRF-TOKEN") != "token1":
            return FakeResponse(status_code=403)
        self.login_body = json
        self.cookies["XSRF-TOKEN"] = "token2"
        return FakeResponse()

    def request(self, method, url, timeout, **kwargs):
        self.requests.append((method, url, kwargs, dict(self.headers)))
        if url.endswith("/search"):
            if kwargs["json"]["position"] > 0:
                # the asset is the first page of 451 results: the next ones are empty here
                return FakeResponse(search_page([], count=451))
            return FakeResponse((ASSETS / "inpi_search_dior.xml").read_bytes())
        if url.endswith("/notice/FR5189659"):
            return FakeResponse((ASSETS / "inpi_notice_FR5189659.xml").read_bytes())
        return FakeResponse(status_code=404)


def test_brand_notices_exact_name_in_force_only():
    session = FakeInpiSession()
    client = InpiClient("user", "password", delay_sec=0, session=session)
    notices = client.brand_notices("Dior")
    # partial matches ("DIOR ADDICT...") and the expired trademark are not fetched
    assert [n.application_number for n in notices] == ["5189659"]
    method, url, kwargs, headers = session.requests[0]
    assert (method, url.rsplit("/", 1)[-1]) == ("POST", "search")
    assert kwargs["json"]["collections"] == ["FR"]
    assert kwargs["json"]["query"] == '[Mark="Dior"]'
    assert session.login_body == {
        "username": "user",
        "password": "password",
        "rememberMe": True,
    }
    assert headers["X-XSRF-TOKEN"] == "token2"
    # first page, second (empty) page, then the notice
    assert [(r[1].rsplit("/", 1)[-1], r[2].get("json", {}).get("position")) for r in session.requests] == [
        ("search", 0), ("search", 100), ("FR5189659", None),
    ]


def test_parse_search_marks_escaped_twice():
    # as returned by the API for "Comme J'aime": the apostrophe escaped twice
    xml = search_page([("4365907", "COMME J&amp;apos;AIME"), ("5240557", "COMME J’AIME PAS CONFISERIE")], count=2)
    results = parse_search(xml)
    assert [r.mark for r in results] == ["COMME J'AIME", "COMME J’AIME PAS CONFISERIE"]
    assert name_key(results[0].mark) == name_key("Comme J'aime")


def test_brand_notices_reads_at_most_max_notices(monkeypatch):
    session = FakeInpiSession()
    urls = []

    def request(method, url, timeout, **kwargs):
        urls.append(url)
        if url.endswith("/search"):
            marks = [(str(n), "ACME") for n in range(5)] if kwargs["json"]["position"] == 0 else []
            return FakeResponse(search_page(marks, count=5))
        return FakeResponse((ASSETS / "inpi_notice_FR5189659.xml").read_bytes())

    monkeypatch.setattr(session, "request", request)
    notices = InpiClient("user", "password", delay_sec=0, session=session).brand_notices("Acme")
    # 5 trademarks in force named like the brand, only the first 3 notices read (INPI_MAX_NOTICES): the
    # notices make most of the requests of a run
    assert len(notices) == 3
    assert sum("/notice/" in url for url in urls) == 3


def search_page(marks: list[tuple[str, str]], count: int) -> bytes:
    """Search answer with these (application number, mark) results, out of count in total."""
    results = "".join(
        f'<result><fields><field name="ApplicationNumber"><value>{number}</value></field>'
        f'<field name="Mark"><value>{mark}</value></field>'
        f'<field name="MarkCurrentStatusCode"><value>Marque renouvelée</value></field></fields></result>'
        for number, mark in marks
    )
    return f"<trademarkSearch><metadata><count>{count}</count></metadata><results>{results}</results></trademarkSearch>".encode()


def test_brand_notices_european_and_international_when_no_french_one(monkeypatch):
    """VOLKSWAGEN: no French trademark, international (WO, without status) and European (EU) ones,
    the answer below is the real one, shortened."""
    wo_search = b"""<trademarkSearch><metadata><count>3</count></metadata><results>
      <result documentId="1892865"><xml href="https://api-gateway.inpi.fr/services/apidiffusion/api/marques/notice/WO1892865"/>
        <fields><field name="ApplicationNumber"><value>1892865</value></field>
        <field name="Mark"><value>VOLKSWAGEN</value></field>
        <field name="DEPOSANT"><value>Volkswagen Aktiengesellschaft</value></field></fields></result>
      <result documentId="19197838"><xml href="https://api-gateway.inpi.fr/services/apidiffusion/api/marques/notice/EU19197838"/>
        <fields><field name="ApplicationNumber"><value>19197838</value></field>
        <field name="Mark"><value>VOLKSWAGEN GROUP DIGITAL SOLUTIONS [ PORTUGAL ]</value></field>
        <field name="MarkCurrentStatusCode"><value>Application withdrawn</value></field></fields></result>
      <result documentId="1"><xml href="https://api-gateway.inpi.fr/services/apidiffusion/api/marques/notice/EU1"/>
        <fields><field name="ApplicationNumber"><value>1</value></field>
        <field name="Mark"><value>Volkswagen</value></field>
        <field name="MarkCurrentStatusCode"><value>Application withdrawn</value></field></fields></result>
    </results></trademarkSearch>"""
    # real WO notice, shortened: no PersonType, a legal entity, the name in free format lines
    wo_notice = b"""<TradeMark><RegistrationOfficeCode>WO</RegistrationOfficeCode><ApplicationNumber>1892865</ApplicationNumber>
      <MarkFeature>Word</MarkFeature>
      <GoodsServicesDetails><GoodsServices><ClassDescriptionDetails>
        <ClassDescription><ClassNumber>09</ClassNumber></ClassDescription>
      </ClassDescriptionDetails></GoodsServices></GoodsServicesDetails>
      <ApplicantDetails><Applicant>
        <ApplicantIdentifier>1742848</ApplicantIdentifier>
        <ApplicantLegalEntity>Joint Stock Company</ApplicantLegalEntity>
        <ApplicantIncorporationState>DE</ApplicantIncorporationState>
        <ApplicantAddressBook><FormattedNameAddress>
          <Name><FreeFormatName><FreeFormatNameDetails>
            <FreeFormatNameLine>Volkswagen Aktiengesellschaft</FreeFormatNameLine>
          </FreeFormatNameDetails></FreeFormatName></Name>
          <Address><AddressCountryCode>DE</AddressCountryCode></Address>
        </FormattedNameAddress></ApplicantAddressBook>
      </Applicant></ApplicantDetails></TradeMark>"""
    session = FakeInpiSession()
    requests_sent = []

    def request(method, url, timeout, **kwargs):
        collections = kwargs.get("json", {}).get("collections")
        requests_sent.append((url.rsplit("/", 1)[-1], collections))
        if url.endswith("/search"):
            return FakeResponse(search_page([], count=0) if collections == ["FR"] else wo_search)
        return FakeResponse(wo_notice)

    monkeypatch.setattr(session, "request", request)
    notices = InpiClient("user", "password", delay_sec=0, session=session).brand_notices("Volkswagen")
    assert requests_sent == [("search", ["FR"]), ("search", ["EU", "WO"]), ("WO1892865", None)]
    assert [(n.holder_name, n.holder_siren, n.holder_country, n.notice_number, n.classes) for n in notices] == [
        ("Volkswagen Aktiengesellschaft", None, "DE", "WO1892865", {9}),
    ]
    holder = choose_holder(notices, None)
    row = propose_row("Volkswagen", holder, None, None, None, {}, TODAY)
    assert (row["groupe"], row["numero_marque"]) == ("Volkswagen Aktiengesellschaft", "WO1892865")


def test_eu_update_record_holder_from_search_result(monkeypatch):
    """Real EU notice, shortened: an update record with the holder identifier only (no name)."""
    eu_notice = b"""<TradeMark operationCode="Insert"><RegistrationOfficeCode>EM</RegistrationOfficeCode>
      <ApplicationNumber>019197838</ApplicationNumber><MarkCurrentStatusCode>Registered</MarkCurrentStatusCode>
      <ApplicantDetails><Applicant operationCode="Delete"><ApplicantIdentifier>2349167</ApplicantIdentifier></Applicant></ApplicantDetails>
      <RepresentativeDetails><Representative><RepresentativeLegalEntity>Legal Person</RepresentativeLegalEntity>
        <RepresentativeAddressBook><FormattedNameAddress><Name><FormattedName><LastName>GULDE &amp; PARTNER</LastName>
        </FormattedName></Name></FormattedNameAddress></RepresentativeAddressBook></Representative></RepresentativeDetails>
    </TradeMark>"""
    notice = parse_notice(eu_notice)
    # the representative is never taken for the holder
    assert (notice.holder_name, notice.holder_is_company) == (None, False)

    def eu_search(applicant):
        return f"""<trademarkSearch><metadata><count>1</count></metadata><results>
          <result><xml href="https://api-gateway.inpi.fr/services/apidiffusion/api/marques/notice/EU19197838"/>
          <fields><field name="ApplicationNumber"><value>19197838</value></field>
          <field name="Mark"><value>ACME</value></field>
          <field name="MarkCurrentStatusCode"><value>Registered</value></field>
          <field name="DEPOSANT"><value>{applicant}</value></field></fields></result></results></trademarkSearch>""".encode()

    for applicant, kept in [("Acme Holding GmbH", True), ("Jean Dupont", False)]:
        session = FakeInpiSession()
        answers = {"search": [FakeResponse(search_page([], count=0)), FakeResponse(eu_search(applicant))]}

        def request(method, url, timeout, answers=answers, **kwargs):
            if url.endswith("/search"):
                return answers["search"].pop(0)
            return FakeResponse(eu_notice)

        monkeypatch.setattr(session, "request", request)
        notices = InpiClient("user", "password", delay_sec=0, session=session).brand_notices("Acme")
        # a natural person is never kept
        assert [n.holder_name for n in notices] == ([applicant] if kept else [])


def test_looks_like_company():
    assert looks_like_company("Volkswagen Aktiengesellschaft")
    assert looks_like_company("Amazon Technologies, Inc.")
    assert looks_like_company("CARREFOUR SA")
    assert not looks_like_company("Jean Dupont")
    assert not looks_like_company(None)


def test_search_reads_all_pages(monkeypatch):
    """ALAIN AFFLELOU: the trademarks named exactly like the brand are old ones, after 200 more recent
    trademarks containing its words (most recent first)."""
    pages = {
        0: search_page([(str(i), f"ALAIN AFFLELOU {i}") for i in range(100)], count=215),
        100: search_page([(str(i), f"AFFLELOU {i}") for i in range(100, 200)], count=215),
        200: search_page([("4125732", "ALAIN AFFLELOU")] + [(str(i), f"X {i}") for i in range(14)], count=215),
    }
    session = FakeInpiSession()
    positions = []

    def request(method, url, timeout, **kwargs):
        positions.append(kwargs["json"]["position"])
        return FakeResponse(pages[kwargs["json"]["position"]])

    monkeypatch.setattr(session, "request", request)
    client = InpiClient("user", "password", delay_sec=0, session=session)
    results = client.search("Alain Afflelou")
    assert positions == [0, 100, 200]
    assert len(results) == 215
    assert [r.application_number for r in results if r.mark == "ALAIN AFFLELOU"] == ["4125732"]
    # never more than max_results
    positions.clear()
    assert len(client.search("Alain Afflelou", max_results=100)) == 100
    assert positions == [0]


def notice(number, siren, name, classes):
    return Notice(
        number, "X", "Word", "Marque enregistrée", name, siren, True, set(classes)
    )


def test_holder_registered_abroad_without_siren(monkeypatch):
    """IKEA: the trademark holders are companies registered abroad, without SIREN."""
    foreign = parse_notice((ASSETS / "inpi_notice_foreign_holder.xml").read_bytes())
    old = Notice("1084780", "IKEA", "Word", "Marque renouvelée", "INTER-IKEA AG", None, True, {20}, "CH")
    holder = choose_holder([foreign, old], {20})
    assert (holder.name, holder.siren, holder.country, holder.other_holders) == (
        "Inter IKEA Systems B.V.", "", "NL", ["INTER-IKEA AG (CH)"],
    )

    lookups = []
    monkeypatch.setattr(registries, "wikidata_parent", lambda siren: lookups.append("wikidata"))
    monkeypatch.setattr(registries, "gleif_lei", lambda siren: lookups.append("gleif siren"))
    monkeypatch.setattr(
        registries, "gleif_lei_by_name", lambda name, country: Company(name, "LEI_IKEA") if country == "NL" else None
    )
    monkeypatch.setattr(registries, "gleif_direct_parent", lambda lei: Company("Inter IKEA Holding B.V.", "LEI_PARENT"))

    class FakeInpi:
        def brand_notices(self, brand):
            return [foreign, old]

    row = run_module.propose("IKEA", "FHI", FakeInpi(), {"FHI": {20, 21}}, {}, TODAY)
    # no SIREN: neither Wikidata nor GLEIF by SIREN
    assert lookups == []
    assert (row["groupe"], row["source"], row["siren"], row["lei"]) == ("Inter IKEA Holding B.V.", "inpi+gleif", "", "LEI_IKEA")
    assert row["commentaire"].startswith("job 2026-10-02 : titulaire Inter IKEA Systems B.V. (société étrangère, pays NL)")


def test_choose_holder_by_sector_classes():
    notices = [
        notice("3", "111", "COUTURE", {25, 30}),
        notice("2", "222", "PARFUMS", {3}),
        notice("1", "222", "PARFUMS", {3, 35}),
    ]
    holder = choose_holder(notices, {3})
    assert (holder.siren, holder.application_number, holder.other_holders) == (
        "222",
        "2",
        [],
    )
    # without the sector's classes: the holder of the most trademarks, others listed
    holder = choose_holder(notices, None)
    assert (holder.siren, holder.other_holders) == ("222", ["COUTURE (111)"])
    assert choose_holder(notices, {12}) is None


def test_parse_nice_classes():
    rows = [
        ("PCB", "3; 5; 8"),
        ("AUT", "12,37"),
        ("FSI", 36),
        ("PUB", None),
        (None, "3"),
    ]
    assert parse_nice_classes(rows) == {"PCB": {3, 5, 8}, "AUT": {12, 37}, "FSI": {36}}


def test_propose_row_known_group_wins():
    holder = choose_holder(
        [notice("5189659", "612035832", "CHRISTIAN DIOR COUTURE", {25})], None
    )
    row = propose_row(
        "Dior",
        holder,
        wikidata_parent=Company("LVMH Moët Hennessy Louis Vuitton", "Q504998"),
        gleif_lei=Company("CHRISTIAN DIOR COUTURE", "96950005T49LGF6G2042"),
        gleif_parent=None,
        known_groups={"lvmh": "LVMH", "lvmhmoethennessylouisvuitton": "LVMH"},
        today=TODAY,
    )
    assert row["groupe"] == "LVMH"
    assert row["source"] == "inpi+wikidata"
    assert row["statut"] == "non vérifié"
    assert (row["siren"], row["lei"], row["numero_marque"]) == (
        "612035832",
        "96950005T49LGF6G2042",
        "FR5189659",
    )
    assert row["commentaire"].startswith(
        "job 2026-10-02 : titulaire CHRISTIAN DIOR COUTURE (SIREN 612035832)"
    )


def test_propose_row_fallbacks():
    holder = choose_holder([notice("1", "123", "ACME SAS", {3})], None)
    gleif = propose_row(
        "Acme",
        holder,
        None,
        Company("ACME SAS", "LEI1"),
        Company("ACME HOLDING", "LEI2"),
        {},
        TODAY,
    )
    assert (gleif["groupe"], gleif["source"]) == ("ACME HOLDING", "inpi+gleif")
    alone = propose_row("Acme", holder, None, None, None, {}, TODAY)
    assert (alone["groupe"], alone["source"]) == ("ACME SAS", "inpi")
    assert "aucune société mère trouvée" in alone["commentaire"]
    nothing = propose_row("Acme", None, None, None, None, {}, TODAY)
    assert (nothing["marque"], nothing["groupe"], nothing["statut"]) == (
        "Acme",
        "",
        "non vérifié",
    )


def test_print_rows_tab_separated():
    out = io.StringIO()
    run_module.print_rows([{"marque": "Acme", "groupe": "ACME\tSAS", "commentaire": "ligne 1\nligne 2", "x": "ignored"}], out)
    lines = out.getvalue().splitlines()
    assert lines[0] == "----- BRAND GROUPS PROPOSALS (tab-separated) -----"
    assert lines[1].split("\t")[:5] == ["marque", "groupe", "source", "statut", "commentaire"]
    assert lines[2].split("\t")[:5] == ["Acme", "ACME SAS", "", "", "ligne 1 ligne 2"]
    assert lines[3] == "----- END OF BRAND GROUPS PROPOSALS -----"


def test_brands_to_process():
    rows = [
        ("Dior", "COSM", 100),
        ("DIOR", "COSM", 300),
        ("Dior", "LUXE", 50),
        ("Škoda", "AUTO", 200),
        ("Already Listed", "FOOD", 1000),
        ("", "FOOD", 10),
    ]
    assert brands_to_process(rows, {"alreadylisted"}, max_brands=10) == [
        ("DIOR", "COSM"),
        ("Škoda", "AUTO"),
    ]
    assert brands_to_process(rows, set(), max_brands=1) == [("Already Listed", "FOOD")]


def test_gleif_and_wikidata(monkeypatch):
    calls = []

    def fake_get(url, headers, timeout, params=None):
        calls.append((url, params, headers))
        if url.endswith("/lei-records"):
            return FakeResponse(
                json_data={
                    "data": [
                        {
                            "id": "FUNDLEI",
                            "attributes": {
                                "entity": {
                                    "category": "FUND",
                                    "legalName": {"name": "FONDS"},
                                }
                            },
                        },
                        {
                            "id": "96950005T49LGF6G2042",
                            "attributes": {
                                "entity": {
                                    "category": "GENERAL",
                                    "legalName": {"name": "CHRISTIAN DIOR COUTURE"},
                                }
                            },
                        },
                    ]
                }
            )
        if url.endswith("/direct-parent"):
            return FakeResponse(status_code=404)
        return FakeResponse(
            json_data={
                "results": {
                    "bindings": [
                        {
                            "parent": {
                                "value": "http://www.wikidata.org/entity/Q504998"
                            },
                            "parentLabel": {"value": "LVMH"},
                        },
                    ]
                }
            }
        )

    monkeypatch.setattr(registries.requests, "get", fake_get)
    assert registries.gleif_lei("612035832", delay_sec=0) == Company(
        "CHRISTIAN DIOR COUTURE", "96950005T49LGF6G2042"
    )
    assert calls[0][1] == {
        "filter[entity.registeredAs]": "612035832",
        "filter[entity.jurisdiction]": "FR",
    }
    assert registries.gleif_direct_parent("96950005T49LGF6G2042", delay_sec=0) is None
    assert registries.wikidata_parent("612035832", delay_sec=0) == Company(
        "LVMH", "Q504998"
    )
    assert 'wdt:P1616 "612035832"' in calls[2][1]["query"]
    assert all("QuotaClimat" in c[2]["User-Agent"] for c in calls)
    # company registered abroad: by exact legal name and country
    assert registries.gleif_lei_by_name("CHRISTIAN DIOR COUTURE", "FR", delay_sec=0) == Company(
        "CHRISTIAN DIOR COUTURE", "96950005T49LGF6G2042"
    )
    assert calls[-1][1] == {"filter[entity.legalName]": "CHRISTIAN DIOR COUTURE", "filter[entity.legalAddress.country]": "FR"}
    # never put anything else than a SIREN in the SPARQL query
    assert registries.wikidata_parent('1" } DROP', delay_sec=0) is None


class FakeSheetsSession:
    def __init__(self, tabs):
        self.tabs = tabs
        self.appended = []

    def get(self, url, timeout, params=None):
        tab = url.split("/values/")[1].split("!")[0].strip("'")
        if tab not in self.tabs:
            return FakeResponse(status_code=400)
        rows = self.tabs[tab][:1] if url.endswith("!1:1") else self.tabs[tab]
        return FakeResponse(json_data={"values": rows})

    def post(self, url, params, json, timeout):
        self.appended.append((url, params, json))
        return FakeResponse()


def test_sheet_read_and_append_by_column_name():
    session = FakeSheetsSession(
        {"Marques": [["marque", "groupe", "statut", "siren"], ["Dior", "LVMH"]]}
    )
    sheet = BrandInventorySheet(session, "SHEET_ID")
    assert sheet.read("Marques") == [
        {"marque": "Dior", "groupe": "LVMH", "statut": "", "siren": ""}
    ]
    assert sheet.read("Unknown tab") == []
    sheet.append(
        "Marques",
        [
            {
                "marque": "Acme",
                "groupe": "ACME",
                "statut": "non vérifié",
                "lei": "dropped",
            }
        ],
    )
    url, params, body = session.appended[0]
    assert url.endswith("/values/'Marques'!A1:append")
    assert params == {"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"}
    assert body == {"values": [["Acme", "ACME", "non vérifié", ""]]}
