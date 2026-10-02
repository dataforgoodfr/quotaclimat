"""Brand group proposals (quotaclimat/data_ingestion/advertising/s04_brand_groups), without any network:
the INPI responses are real ones (assets/), shortened, plus two results added to the search to test the
exact-name and status filters (FR5189659 "DIOR", 1000001 expired)."""

from datetime import date
from pathlib import Path

from quotaclimat.data_ingestion.advertising.s04_brand_groups import registries
from quotaclimat.data_ingestion.advertising.s04_brand_groups.inpi import (
    InpiClient, Notice, is_alive, parse_notice, parse_search)
from quotaclimat.data_ingestion.advertising.s04_brand_groups.propose import (
    choose_holder, parse_nice_classes, propose_row)
from quotaclimat.data_ingestion.advertising.s04_brand_groups.registries import Company
from quotaclimat.data_ingestion.advertising.s04_brand_groups.run import brands_to_process
from quotaclimat.data_ingestion.advertising.s04_brand_groups.sheet import BrandInventorySheet

ASSETS = Path(__file__).parent / "assets"
TODAY = date(2026, 10, 2)


def test_parse_search():
    results = parse_search((ASSETS / "inpi_search_dior.xml").read_bytes())
    assert [(r.application_number, r.mark, r.status, r.applicant) for r in results] == [
        ("5284729", "NOTHING BUT DIOR, J’ADORE", "Demande publiée", "PARFUMS CHRISTIAN DIOR"),
        ("5237808", "DIOR ADDICT TOFFEE GLOW", "Marque enregistrée", "PARFUMS CHRISTIAN DIOR"),
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
    )


def test_parse_notice_applicant_when_no_current_holder():
    xml = """<TradeMark><ApplicationNumber>1</ApplicationNumber>
        <ApplicantDetails><Applicant PersonType="PP">
          <ApplicantIdentifier identifierKindCode="FR">123456789</ApplicantIdentifier>
        </Applicant></ApplicantDetails></TradeMark>"""
    notice = parse_notice(xml)
    assert (notice.holder_siren, notice.holder_is_company, notice.classes) == ("123456789", False, set())


def test_is_alive():
    assert is_alive("Marque enregistrée")
    assert is_alive("MARQUE_RENOUVELÉE")
    assert is_alive("Demande publiée")
    assert not is_alive("Marque expirée")
    assert not is_alive("MARQUE_DÉCHUE")
    assert not is_alive(None)


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
    assert kwargs["json"]["query"] == "[Mark=Dior]"
    assert session.login_body == {"username": "user", "password": "password", "rememberMe": True}
    assert headers["X-XSRF-TOKEN"] == "token2"
    assert [r[1].rsplit("/", 1)[-1] for r in session.requests[1:]] == ["FR5189659"]


def notice(number, siren, name, classes):
    return Notice(number, "X", "Word", "Marque enregistrée", name, siren, True, set(classes))


def test_choose_holder_by_sector_classes():
    notices = [
        notice("3", "111", "COUTURE", {25, 30}),
        notice("2", "222", "PARFUMS", {3}),
        notice("1", "222", "PARFUMS", {3, 35}),
    ]
    holder = choose_holder(notices, {3})
    assert (holder.siren, holder.application_number, holder.other_holders) == ("222", "2", [])
    # without the sector's classes: the holder of the most trademarks, others listed
    holder = choose_holder(notices, None)
    assert (holder.siren, holder.other_holders) == ("222", ["COUTURE (111)"])
    assert choose_holder(notices, {12}) is None


def test_parse_nice_classes():
    rows = [("PCB", "3; 5; 8"), ("AUT", "12,37"), ("FSI", 36), ("PUB", None), (None, "3")]
    assert parse_nice_classes(rows) == {"PCB": {3, 5, 8}, "AUT": {12, 37}, "FSI": {36}}


def test_propose_row_known_group_wins():
    holder = choose_holder([notice("5189659", "612035832", "CHRISTIAN DIOR COUTURE", {25})], None)
    row = propose_row(
        "Dior", holder,
        wikidata_parent=Company("LVMH Moët Hennessy Louis Vuitton", "Q504998"),
        gleif_lei=Company("CHRISTIAN DIOR COUTURE", "96950005T49LGF6G2042"),
        gleif_parent=None,
        known_groups={"lvmh": "LVMH", "lvmhmoethennessylouisvuitton": "LVMH"},
        today=TODAY,
    )
    assert row["groupe"] == "LVMH"
    assert row["source"] == "inpi+wikidata"
    assert row["statut"] == "non vérifié"
    assert (row["siren"], row["lei"], row["numero_marque"]) == ("612035832", "96950005T49LGF6G2042", "FR5189659")
    assert row["commentaire"].startswith("job 2026-10-02 : titulaire CHRISTIAN DIOR COUTURE (SIREN 612035832)")


def test_propose_row_fallbacks():
    holder = choose_holder([notice("1", "123", "ACME SAS", {3})], None)
    gleif = propose_row("Acme", holder, None, Company("ACME SAS", "LEI1"), Company("ACME HOLDING", "LEI2"), {}, TODAY)
    assert (gleif["groupe"], gleif["source"]) == ("ACME HOLDING", "inpi+gleif")
    alone = propose_row("Acme", holder, None, None, None, {}, TODAY)
    assert (alone["groupe"], alone["source"]) == ("ACME SAS", "inpi")
    assert "aucune société mère trouvée" in alone["commentaire"]
    nothing = propose_row("Acme", None, None, None, None, {}, TODAY)
    assert (nothing["marque"], nothing["groupe"], nothing["statut"]) == ("Acme", "", "non vérifié")


def test_brands_to_process():
    rows = [
        ("Dior", "COSM", 100), ("DIOR", "COSM", 300), ("Dior", "LUXE", 50),
        ("Škoda", "AUTO", 200),
        ("Already Listed", "FOOD", 1000),
        ("", "FOOD", 10),
    ]
    assert brands_to_process(rows, {"alreadylisted"}, max_brands=10) == [("DIOR", "COSM"), ("Škoda", "AUTO")]
    assert brands_to_process(rows, set(), max_brands=1) == [("Already Listed", "FOOD")]


def test_gleif_and_wikidata(monkeypatch):
    calls = []

    def fake_get(url, headers, timeout, params=None):
        calls.append((url, params, headers))
        if url.endswith("/lei-records"):
            return FakeResponse(json_data={"data": [
                {"id": "FUNDLEI", "attributes": {"entity": {"category": "FUND", "legalName": {"name": "FONDS"}}}},
                {"id": "96950005T49LGF6G2042", "attributes": {"entity": {"category": "GENERAL", "legalName": {"name": "CHRISTIAN DIOR COUTURE"}}}},
            ]})
        if url.endswith("/direct-parent"):
            return FakeResponse(status_code=404)
        return FakeResponse(json_data={"results": {"bindings": [
            {"parent": {"value": "http://www.wikidata.org/entity/Q504998"}, "parentLabel": {"value": "LVMH"}},
        ]}})

    monkeypatch.setattr(registries.requests, "get", fake_get)
    assert registries.gleif_lei("612035832", delay_sec=0) == Company("CHRISTIAN DIOR COUTURE", "96950005T49LGF6G2042")
    assert calls[0][1] == {"filter[entity.registeredAs]": "612035832", "filter[entity.jurisdiction]": "FR"}
    assert registries.gleif_direct_parent("96950005T49LGF6G2042", delay_sec=0) is None
    assert registries.wikidata_parent("612035832", delay_sec=0) == Company("LVMH", "Q504998")
    assert 'wdt:P1616 "612035832"' in calls[2][1]["query"]
    assert all("QuotaClimat" in c[2]["User-Agent"] for c in calls)
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
    session = FakeSheetsSession({"Marques": [["marque", "groupe", "statut", "siren"], ["Dior", "LVMH"]]})
    sheet = BrandInventorySheet(session, "SHEET_ID")
    assert sheet.read("Marques") == [{"marque": "Dior", "groupe": "LVMH", "statut": "", "siren": ""}]
    assert sheet.read("Unknown tab") == []
    sheet.append("Marques", [{"marque": "Acme", "groupe": "ACME", "statut": "non vérifié", "lei": "dropped"}])
    url, params, body = session.appended[0]
    assert url.endswith("/values/'Marques'!A1:append")
    assert params == {"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"}
    assert body == {"values": [["Acme", "ACME", "non vérifié", ""]]}
