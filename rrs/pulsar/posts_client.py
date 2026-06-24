"""Fetch a theme's posts from Pulsar's TRAC data GraphQL endpoint.

A theme is scoped by its topics: `TopContent(filter)` with `filter.topics =
<the theme's topics>` returns the top posts/articles for that theme. Auth is the
session Bearer token + x-pulsar-team header captured during the dashboard login.
"""

import time
from datetime import datetime

import requests
from tenacity import retry, stop_after_attempt, wait_random_exponential

DATA_ENDPOINT = "https://data.pulsarplatform.com/graphql/trac"
_MIN_REQUEST_INTERVAL = 0.2  # ≤5 req/s
_TIMEOUT = 60

_TOP_CONTENT_QUERY = """
query TopContent($options: ResultsOptionsInput, $filter: FilterInput!) {
  topContent(filter: $filter, options: $options) {
    results {
      content credibilityLabel credibilityScore domainName identifier images
      newsArticleLink publishedAt pulsarId source thumbnail title url userBio
      userCountryCode userFollowersCount userFriendsCount userGender userInfo
      userName userScreenName videos visibility
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
    def __init__(self, authorization: str, team: str):
        # `authorization` is the verbatim header value captured from the session (e.g. "Bearer eyJ…").
        self._headers = {
            "authorization": authorization,
            "x-pulsar-team": team,
            "content-type": "application/json",
        }
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at = time.monotonic()

    @retry(wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def _post(self, variables: dict) -> dict:
        self._throttle()
        resp = requests.post(
            DATA_ENDPOINT,
            json={"query": _TOP_CONTENT_QUERY, "variables": variables},
            headers=self._headers,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise RuntimeError(f"Pulsar GraphQL errors: {payload['errors']}")
        return payload["data"]

    def fetch_theme_posts(
        self, search_id: str, topics: list[str], date_from: datetime, date_to: datetime, limit: int = 30
    ) -> list[dict]:
        """Return the top `limit` posts for a theme (its topics), newest/most-visible first."""
        if not topics:
            return []
        filter_ = {**_BASE_FILTER, "searchIds": [search_id], "topics": topics,
                   "dateFrom": _iso(date_from), "dateTo": _iso(date_to)}
        options = {"limit": limit, "sort": "DESC", "sortBy": "VISIBILITY", "cursor": "*",
                   "forceCacheRefresh": True, "engine": "SOLR"}
        data = self._post({"filter": filter_, "options": options})
        return (data.get("topContent") or {}).get("results") or []
