"""Fetch a theme's posts from Pulsar's TRAC data GraphQL endpoint.

A theme is scoped by its topics: `TopContent(filter)` with `filter.topics =
<the theme's topics>` returns the top posts/articles for that theme. Auth is the
session Bearer token + x-pulsar-team header captured during the dashboard login.
"""

import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta

import requests
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_random_exponential

logger = logging.getLogger(__name__)

DATA_ENDPOINT = "https://data.pulsarplatform.com/graphql/trac"
_MIN_REQUEST_INTERVAL = 0.2  # ≤5 req/s
_TIMEOUT = 60

_TOP_CONTENT_QUERY = """
query TopContent($options: ResultsOptionsInput, $filter: FilterInput!) {
  topContent(filter: $filter, options: $options) {
    nextCursor
    total
    results {
      content credibilityLabel credibilityScore domainName identifier images
      newsArticleLink publishedAt pulsarId source thumbnail title url userBio
      userCountryCode userFollowersCount userFriendsCount userGender userInfo
      userName userScreenName videos visibility
      identifier userIdentifier topics subtype parentIdentifier pulsarParentId quotedPostId
      likesCount sharesCount commentsCount viewsCount engagement
    }
  }
}
"""

# Base FilterInput (wide-open ranges + every source), captured from the dashboard.
# Only searchIds / dateFrom / dateTo / topics are overridden per call.
_BASE_FILTER = {
    "authors": [], "ave": {"max": 1000000, "min": 0}, "bioEmojis": [], "bioKeywords": [],
    "circulation": {"max": 100000, "min": 0}, "cities": [], "communityTags": [], "companies": [],
    "contentSubtypes": [], "contentType": [], "countries": [], "countryOrLanguage": "AND",
    "creatorCodeCompliance": [], "creatorCodes": [], "credibilityLabel": [],
    "credibilityScore": {"max": 100, "min": 0}, "dataTypes": [], "discordChannels": [],
    "discordServers": [], "domains": [], "duration": {"max": 10000, "min": 0},
    "engagements": {"max": 100, "min": 0}, "entitiesCategories": [], "entitiesNames": [],
    "excludeAuthors": False, "excludeCities": False, "excludeCountries": False,
    "excludeDataTypes": False, "excludeDomains": False, "excludeEntitiesCategories": False,
    "excludeEntitiesNames": False, "excludeIdentifiers": False, "excludeLanguages": False,
    "excludeLicenses": False, "excludePublicIdentifiers": False, "excludeRegions": False,
    "excludeTags": False, "excludeTopics": False, "followers": {"max": 100000, "min": 0},
    "genders": [], "hashtags": "", "identifiers": [], "imageCaption": "", "imageTags": [],
    "imageTexts": [], "impressions": {"max": 100000, "min": 0}, "industries": [], "intensities": [],
    "jobTitles": [], "keywords": "", "languages": [], "licenses": [], "likes": {"max": 10000, "min": 0},
    "linksDomain": [], "mainEmotion": [], "mediaImpressions": {"max": 100000, "min": 0},
    "mediaReach": {"max": 100000, "min": 0}, "mediaType": [], "mentionTags": [], "newsSubtypes": [],
    "onlyRelevantCsr": False, "onlyRelevantReputation": False, "publicIdentifiers": [], "regions": [],
    "relevantModules": [], "reputation": {"max": 100, "min": 0}, "sentiments": [],
    "socialImpressions": {"max": 100000, "min": 0},
    "sources": [
        "ALIEXPRESS", "AMAZON", "BING", "BLOG", "EXPEDIA", "FACEBOOK_PUBLIC", "FIRST_PARTY_DATA",
        "FORUM", "INSTAGRAM", "INSTAGRAM_PUBLIC", "LINKEDIN", "NAVER", "ONLINE_NEWS", "PODCAST",
        "PRINT_NEWS", "RADIO", "REDDIT", "REVIEW", "SEA_FACEBOOK_PAGE", "TAOBAO", "THREADS", "TIKTOK",
        "TRIPADVISOR", "TRUSTPILOT", "TUMBLR", "TV", "TWITCH", "TWITTER", "VKONTAKTE", "X", "YOUTUBE",
        "CH_BAIDU", "BILIBILI", "DOUYIN", "KUAISHOU", "WEIBO", "WECHAT", "LITTLE_RED_BOOK", "ZHIHU",
        "CHINESE_ONLINE_NEWS",
    ],
    "syndicationGrouping": False, "tags": [], "transcribedStatus": [], "userBio": "",
    "visibility": {"max": 1000, "min": 0},
}


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat(timespec="milliseconds") if dt is not None else None


class PulsarPostsClient:
    def __init__(self, authorization: str, team: str | None = None):
        # `authorization` is the verbatim header value (e.g. "Bearer eyJ…").
        # `team` is the x-pulsar-team header, required for session tokens but not for API keys.
        self._headers = {"authorization": authorization, "content-type": "application/json"}
        if team:
            self._headers["x-pulsar-team"] = team
        self._last_request_at = 0.0

    @classmethod
    def from_api_key(cls, api_key: str) -> "PulsarPostsClient":
        """Create a client authenticated with a permanent Pulsar API key."""
        return cls(authorization=f"Bearer {api_key}")

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at = time.monotonic()

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(5),
           before_sleep=before_sleep_log(logger, logging.WARNING))
    def _post(self, variables: dict) -> dict:
        self._throttle()
        started = time.monotonic()
        resp = requests.post(
            DATA_ENDPOINT,
            json={"query": _TOP_CONTENT_QUERY, "variables": variables},
            headers=self._headers,
            timeout=_TIMEOUT,
        )
        logger.debug("POST %s -> %s in %.2fs", DATA_ENDPOINT, resp.status_code, time.monotonic() - started)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise RuntimeError(f"Pulsar GraphQL errors: {payload['errors']}")
        return payload["data"]

    def _filter(self, search_id: str, date_from: datetime, date_to: datetime, **overrides) -> dict:
        return {**_BASE_FILTER, "searchIds": [search_id], "dateFrom": _iso(date_from),
                "dateTo": _iso(date_to), **overrides}

    def _page(self, filter_: dict, limit: int, cursor: str = "*", sort_by: str = "VISIBILITY") -> dict:
        options = {"limit": limit, "sort": "DESC", "sortBy": sort_by, "cursor": cursor,
                   "forceCacheRefresh": True}
        page = self._post({"filter": filter_, "options": options}).get("topContent") or {}
        logger.debug("topContent %s → %s → %d result(s) (limit %d)",
                     filter_["dateFrom"], filter_["dateTo"], len(page.get("results") or []), limit)
        return page

    def fetch_theme_posts(
        self, search_id: str, topics: list[str], date_from: datetime, date_to: datetime, limit: int = 30
    ) -> list[dict]:
        """Return the top `limit` posts for a theme (its topics), newest/most-visible first."""
        if not topics:
            return []
        filter_ = self._filter(search_id, date_from, date_to, topics=topics)
        return self._page(filter_, limit).get("results") or []

    def fetch_all_posts(
        self, search_id: str, topics: list[str] | None, date_from: datetime, date_to: datetime,
        page_size: int = 100, min_window: timedelta = timedelta(seconds=1),
        on_window_done: Callable[[timedelta], None] | None = None,
    ) -> list[dict]:
        """Return every post matching `topics` in [date_from, date_to].

        `topics=None` means no topic filter: every post of the search (each result carries its own
        `topics` list, so callers can filter locally); an empty list returns nothing.

        Pulsar caps a topContent query at 100 results (`total` is capped too) and `nextCursor`
        always comes back as "*", so the cursor can't be used to page. Instead, whenever a window
        returns a full page it is bisected in time and both halves are fetched again, until every
        window returns fewer than `page_size` posts (i.e. is complete). Deduped by pulsarId.
        `on_window_done(span)` is called with the time span of each completed window (for progress).
        """
        page_size = min(page_size, 100)
        if topics is not None and not topics:
            return []
        overrides = {"topics": topics} if topics else {}
        found: dict[str, dict] = {}

        def walk(start: datetime, end: datetime) -> None:
            results = self._page(self._filter(search_id, start, end, **overrides),
                                 page_size, sort_by="TIME").get("results") or []
            if len(results) >= page_size:
                if (end - start) > min_window:
                    mid = start + (end - start) / 2
                    logger.debug("window %s → %s full (%d), splitting", start, end, len(results))
                    walk(start, mid)
                    walk(mid, end)
                    return
                logger.warning(">%d posts within %s at %s; some are dropped", page_size, min_window, start)
            for p in results:
                if p.get("pulsarId"):
                    found[p["pulsarId"]] = p
            if on_window_done:
                on_window_done(end - start)

        walk(date_from, date_to)
        return list(found.values())

    def fetch_by_identifiers(
        self, search_id: str, identifiers: list[str], date_from: datetime, date_to: datetime,
        batch_size: int = 50,
    ) -> list[dict]:
        """Look up posts by their id on the original platform (used to resolve a repost's parent).

        `batch_size` is capped at 50 — Pulsar's API rejects a filter with more identifiers.
        """
        batch_size = min(batch_size, 50)
        out: list[dict] = []
        logger.info("looking up %d parent post(s) in %d batch(es)", len(identifiers),
                    -(-len(identifiers) // batch_size))
        for i in range(0, len(identifiers), batch_size):
            batch = identifiers[i:i + batch_size]
            filter_ = self._filter(search_id, date_from, date_to, identifiers=batch)
            out.extend(self._page(filter_, len(batch)).get("results") or [])
        return out
