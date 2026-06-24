"""Download the Pulsar TRAC "Themes" export (XLSX) by driving the web dashboard,
and capture the session Bearer token so we can fetch per-theme posts via GraphQL.

Pulsar offers no API/server export for themes, so we drive the dashboard headlessly:
log in, open the themes page, use the Export → XLS button for the workbook (one row
per theme + a Topics Breakdown sheet), and grab the `authorization` + `x-pulsar-team`
headers the dashboard itself uses to call the data GraphQL endpoint.

Credentials come from the environment (PULSAR_EMAIL / PULSAR_PASSWORD). Logging in
mints a fresh session each run, so there is no API token to store or refresh.
"""

from dataclasses import dataclass
from pathlib import Path

from playwright.sync_api import sync_playwright

DEFAULT_BASE_URL = "https://lessurligneurs.pulsarplatform.com"

# Login is a standard Rails/Devise form reached by redirect when unauthenticated.
_EMAIL_SELECTOR = "#session_email"
_PASSWORD_SELECTOR = "#session_password"
_SUBMIT_SELECTOR = "button[type=submit]"
_THEMES_READY_SELECTOR = "text=main themes"
_EXPORT_BUTTON_SELECTOR = "[aria-label=Export]"
_XLS_MENUITEM_SELECTOR = "[role=menuitem]:has-text('XLS')"


@dataclass
class DownloadResult:
    xlsx_path: Path
    authorization: str | None  # verbatim header value, e.g. "Bearer eyJ…"
    team: str | None  # x-pulsar-team header value


def themes_url(base_url: str, folder_id: str, search_id: str) -> str:
    return f"{base_url}/trac/folders/{folder_id}/gl/{search_id}/overview/themes"


def download_themes_xlsx(
    *,
    email: str,
    password: str,
    folder_id: str,
    search_id: str,
    output_path,
    base_url: str = DEFAULT_BASE_URL,
    headless: bool = True,
    nav_timeout_ms: int = 30000,
    download_timeout_ms: int = 60000,
) -> DownloadResult:
    """Log in, export the XLS, and capture the data-endpoint auth headers."""
    url = themes_url(base_url, folder_id, search_id)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    captured = {"authorization": None, "team": None}

    def _capture(request):
        if "pulsarplatform.com/graphql" in request.url and captured["authorization"] is None:
            h = request.headers
            if h.get("authorization"):
                captured["authorization"] = h.get("authorization")
                captured["team"] = h.get("x-pulsar-team")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_context(accept_downloads=True).new_page()
        page.on("request", _capture)  # grab the session token from the dashboard's own calls
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_selector(_EMAIL_SELECTOR, timeout=nav_timeout_ms)
            page.fill(_EMAIL_SELECTOR, email)
            page.fill(_PASSWORD_SELECTOR, password)
            page.click(_SUBMIT_SELECTOR)
            page.wait_for_load_state("networkidle", timeout=nav_timeout_ms)

            if "overview/themes" not in page.url:
                page.goto(url, wait_until="domcontentloaded")
            page.wait_for_selector(_THEMES_READY_SELECTOR, timeout=nav_timeout_ms)

            page.click(_EXPORT_BUTTON_SELECTOR)
            xls_item = page.wait_for_selector(_XLS_MENUITEM_SELECTOR, timeout=nav_timeout_ms)
            with page.expect_download(timeout=download_timeout_ms) as download_info:
                xls_item.click()
            download_info.value.save_as(output_path)
        finally:
            browser.close()

    print(f"  Downloaded themes export → {output_path}; token captured: {captured['authorization'] is not None}")
    return DownloadResult(output_path, captured["authorization"], captured["team"])
