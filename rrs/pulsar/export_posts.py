"""Export every Pulsar post behind the themes/topics saved for a subject, as CSV + JSON.

1. Read the themes saved in `pulsar_themes` for a subject between two dates, with their topics.
2. For each (theme, topic), pull ALL matching posts from Pulsar's TopContent endpoint
   (following the API's nextCursor, see PulsarPostsClient.fetch_all_posts).
3. Write one row per (theme, topic, post): the pulsar_posts columns + theme + topic +
   `original_poster` (account that first posted, when the post is a repost) and the engagements
   (likes / shares-retweets / comments / views).

Config (environment):
  PULSAR_API_KEY            Pulsar API key (required).
  SUBJECT                   Subject name, e.g. "climate" (required).
  PULSAR_START_DATE / PULSAR_END_DATE
                            YYYY-MM-DD, themes saved within the window (required). Also the
                            window posts are fetched over.
  PULSAR_EXPORT_DIR         Output directory (default: .). Files: pulsar_posts_<subject>_<start>_<end>.{csv,json}
  PULSAR_PAGE_LIMIT         Posts per API page (default: 100, Pulsar's API max).
  PULSAR_PARENT_LOOKBACK_DAYS
                            How far before the window to look for a repost's original post (default: 365).

Run with: python -m rrs.pulsar.export_posts
"""

import csv
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from rrs.pulsar import store
from rrs.pulsar.posts_client import PulsarPostsClient
from rrs.utils.generate_id import get_consistent_hash

_DATE_FMT = "%Y-%m-%d"
_RT_RE = re.compile(r"^\s*RT\s+@(\w+)", re.IGNORECASE)

POST_COLUMNS = [
    "pulsar_id", "search_id", "title", "content", "url", "news_article_link", "domain_name", "source",
    "published_at", "visibility", "credibility_label", "credibility_score", "images", "videos",
    "thumbnail", "user_name", "user_screen_name", "user_country_code", "user_followers_count",
    "user_friends_count", "user_gender", "user_bio",
]
EXTRA_COLUMNS = ["identifier", "subtype", "parent_identifier", "likes_count", "shares_count",
                 "comments_count", "views_count", "engagement"]
COLUMNS = ["theme_id", "theme_title", "theme_date_from", "theme_date_to", "topic", *POST_COLUMNS,
           *EXTRA_COLUMNS, "original_poster"]

# pulsar_posts column -> TopContent result key
_API_KEYS = {
    "pulsar_id": "pulsarId", "title": "title", "content": "content", "url": "url",
    "news_article_link": "newsArticleLink", "domain_name": "domainName", "source": "source",
    "published_at": "publishedAt", "visibility": "visibility", "credibility_label": "credibilityLabel",
    "credibility_score": "credibilityScore", "images": "images", "videos": "videos",
    "thumbnail": "thumbnail", "user_name": "userName", "user_screen_name": "userScreenName",
    "user_country_code": "userCountryCode", "user_followers_count": "userFollowersCount",
    "user_friends_count": "userFriendsCount", "user_gender": "userGender", "user_bio": "userBio",
    "identifier": "identifier", "subtype": "subtype", "parent_identifier": "parentIdentifier",
    "likes_count": "likesCount", "shares_count": "sharesCount", "comments_count": "commentsCount",
    "views_count": "viewsCount", "engagement": "engagement",
}


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, _DATE_FMT).replace(tzinfo=timezone.utc)


def load_themes(conn, subject_id: str, start: datetime, end: datetime) -> list[dict]:
    """Themes of the subject saved in the window, each with its topic labels."""
    rows = conn.execute(
        """
        SELECT t.theme_id, t.search_id, t.title, t.date_from, t.date_to,
               COALESCE(array_agg(tp.label ORDER BY tp.label) FILTER (WHERE tp.label IS NOT NULL), '{}')
        FROM pulsar_themes t
        LEFT JOIN pulsar_topics tp ON tp.theme_id = t.theme_id
        WHERE t.subject_id = %s AND t.date_from >= %s AND t.date_to <= %s
        GROUP BY t.theme_id, t.search_id, t.title, t.date_from, t.date_to
        ORDER BY t.date_from, t.title
        """,
        (subject_id, start, end),
    ).fetchall()
    return [
        {"theme_id": r[0], "search_id": r[1], "title": r[2], "date_from": r[3], "date_to": r[4], "topics": r[5]}
        for r in rows
    ]


def resolve_original_posters(client, search_id: str, posts: list[dict], date_from: datetime,
                             date_to: datetime) -> dict[str, str]:
    """Map a repost's parentIdentifier -> screen name of the account that first posted.

    Parents already among `posts` are reused; the rest are looked up by identifier (best effort:
    the original may be outside the search, in which case it is simply missing).
    """
    wanted = {p["parentIdentifier"] for p in posts if p.get("parentIdentifier") and p.get("pulsarParentId") != "0"}
    known = {p["identifier"]: p for p in posts if p.get("identifier")}
    authors = {i: known[i].get("userScreenName") for i in wanted if i in known}
    missing = sorted(wanted - authors.keys())
    for p in client.fetch_by_identifiers(search_id, missing, date_from, date_to):
        if p.get("identifier"):
            authors[p["identifier"]] = p.get("userScreenName")
    return {k: v for k, v in authors.items() if v}


def original_poster(post: dict, parent_authors: dict[str, str]) -> str | None:
    """Account that first posted: the resolved parent's author, else the 'RT @user' prefix."""
    return parent_authors.get(post.get("parentIdentifier")) or (
        m.group(1) if (m := _RT_RE.match(post.get("content") or "")) else None
    )


def build_row(theme: dict, topic: str, post: dict, search_id: str, parent_authors: dict[str, str]) -> dict:
    row = {
        "theme_id": theme["theme_id"], "theme_title": theme["title"],
        "theme_date_from": theme["date_from"].isoformat() if theme["date_from"] else None,
        "theme_date_to": theme["date_to"].isoformat() if theme["date_to"] else None,
        "topic": topic, "search_id": search_id,
    }
    for col, key in _API_KEYS.items():
        row[col] = post.get(key)
    row["original_poster"] = original_poster(post, parent_authors)
    return row


def write_outputs(rows: list[dict], base: Path) -> tuple[Path, Path]:
    csv_path, json_path = base.with_suffix(".csv"), base.with_suffix(".json")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                        for k, v in ((c, r.get(c)) for c in COLUMNS)})
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([{c: r.get(c) for c in COLUMNS} for r in rows], f, ensure_ascii=False, indent=2)
    return csv_path, json_path


def run() -> None:
    load_dotenv()
    api_key = os.getenv("PULSAR_API_KEY")
    subject = os.getenv("SUBJECT")
    raw_start, raw_end = os.getenv("PULSAR_START_DATE"), os.getenv("PULSAR_END_DATE")
    missing = [n for n, v in (("PULSAR_API_KEY", api_key), ("SUBJECT", subject),
                              ("PULSAR_START_DATE", raw_start), ("PULSAR_END_DATE", raw_end)) if not v]
    if missing:
        raise EnvironmentError(f"Missing config: {', '.join(missing)}")
    start, end = _parse_date(raw_start), _parse_date(raw_end)
    page_limit = int(os.getenv("PULSAR_PAGE_LIMIT", "100"))
    lookback = timedelta(days=int(os.getenv("PULSAR_PARENT_LOOKBACK_DAYS", "365")))
    out_dir = Path(os.getenv("PULSAR_EXPORT_DIR", "."))
    out_dir.mkdir(parents=True, exist_ok=True)

    with store.connect() as conn:
        themes = load_themes(conn, get_consistent_hash(subject), start, end)
    print(f"{len(themes)} theme(s) for subject '{subject}' between {raw_start} and {raw_end}")

    client = PulsarPostsClient.from_api_key(api_key)
    rows: list[dict] = []
    for theme in themes:
        for topic in theme["topics"]:
            posts = client.fetch_all_posts(theme["search_id"], [topic], start, end, page_size=page_limit)
            authors = resolve_original_posters(client, theme["search_id"], posts, start - lookback, end)
            print(f"  [{theme['title']}] {topic}: {len(posts)} post(s)")
            rows.extend(build_row(theme, topic, p, theme["search_id"], authors) for p in posts)

    base = out_dir / f"pulsar_posts_{subject}_{raw_start}_{raw_end}"
    csv_path, json_path = write_outputs(rows, base)
    print(f"Wrote {len(rows)} row(s) to {csv_path} and {json_path}")


if __name__ == "__main__":
    run()
