"""Export every Pulsar post behind the themes/topics saved for a subject, as CSV + JSON.

1. Read the themes saved in `pulsar_themes` for a subject between two dates, with their topics.
2. Pull ALL the posts of the search over the window from Pulsar's TopContent endpoint in one pass
   (time-window bisection, see PulsarPostsClient.fetch_all_posts), then keep the posts whose own
   `topics` contain one of the saved topics and link them to every matching (theme, topic).
3. Write one row per (theme, topic, post) — a post matching several topics appears once per
   topic — plus a `_unique` pair of files with one row per distinct post and the lists of
   themes / topics it matched. Row columns: the pulsar_posts columns + theme + topic +
   `original_poster` (account that first posted, when the post is a repost) and the engagements
   (likes / shares-retweets / comments / views).

Config (environment):
  PULSAR_API_KEY            Pulsar API key (required).
  SUBJECT                   Subject name, e.g. "climate" (required).
  PULSAR_START_DATE / PULSAR_END_DATE
                            YYYY-MM-DD, themes saved within the window (required). Also the
                            window posts are fetched over.
  PULSAR_EXPORT_DIR         Output directory (default: .). Files: pulsar_posts_<subject>_<start>_<end>[_unique].{csv,json}
  LOG_LEVEL                 INFO (default) or DEBUG to log every API call.
  PULSAR_PAGE_LIMIT         Posts per API page (default: 100, Pulsar's API max).
  PULSAR_PARENT_LOOKBACK_DAYS
                            How far before the window to look for a repost's original post (default: 365).

Run with: python -m rrs.pulsar.export_posts
"""

import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from rrs.pulsar import store
from rrs.pulsar.posts_client import PulsarPostsClient
from rrs.utils.generate_id import get_consistent_hash

logger = logging.getLogger(__name__)

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
# One row per distinct post: the post columns, then every theme / topic it matched.
UNIQUE_LIST_COLUMNS = ["theme_ids", "theme_titles", "topics"]
UNIQUE_COLUMNS = [*POST_COLUMNS, *EXTRA_COLUMNS, "original_poster", *UNIQUE_LIST_COLUMNS]

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


def build_unique_rows(rows: list[dict]) -> list[dict]:
    """One row per distinct post, listing every theme (id + title) and topic it matched."""
    unique: dict[str, dict] = {}
    for r in rows:
        u = unique.setdefault(r["pulsar_id"], {
            **{c: r.get(c) for c in UNIQUE_COLUMNS if c not in UNIQUE_LIST_COLUMNS},
            **{c: [] for c in UNIQUE_LIST_COLUMNS},
        })
        u["original_poster"] = u["original_poster"] or r.get("original_poster")
        for col, key in (("theme_ids", "theme_id"), ("theme_titles", "theme_title"), ("topics", "topic")):
            if r[key] not in u[col]:
                u[col].append(r[key])
    return list(unique.values())


def write_outputs(rows: list[dict], base: Path, columns: list[str]) -> tuple[Path, Path]:
    csv_path, json_path = base.with_suffix(".csv"), base.with_suffix(".json")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                        for k, v in ((c, r.get(c)) for c in columns)})
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([{c: r.get(c) for c in columns} for r in rows], f, ensure_ascii=False, indent=2)
    return csv_path, json_path


def _norm(label: str) -> str:
    return " ".join(label.casefold().split())


def run() -> None:
    load_dotenv()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
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
    # label -> every (theme, topic) of the db that uses it, per search
    by_search: dict[str, dict[str, list[tuple[dict, str]]]] = {}
    for theme in themes:
        index = by_search.setdefault(theme["search_id"], {})
        for topic in theme["topics"]:
            index.setdefault(_norm(topic), []).append((theme, topic))
    logger.info("%d theme(s), %d distinct topic(s) for subject '%s' between %s and %s",
                len(themes), sum(len(i) for i in by_search.values()), subject, raw_start, raw_end)

    client = PulsarPostsClient.from_api_key(api_key)
    rows: list[dict] = []
    span = (end - start).total_seconds()
    with logging_redirect_tqdm():
        for search_id, index in by_search.items():
            with tqdm(total=span, desc=f"search {search_id}", unit="s", mininterval=1,
                      dynamic_ncols=True, file=sys.stderr) as bar:
                step = {"logged": 0.0}

                def on_done(d: timedelta) -> None:
                    bar.update(d.total_seconds())
                    # no TTY (docker compose up/logs, CI): the bar can't redraw, so log every 5%
                    if not sys.stderr.isatty() and bar.n / span - step["logged"] >= 0.05:
                        step["logged"] = bar.n / span
                        logger.info("search %s: %.0f%% of the window fetched", search_id, 100 * bar.n / span)

                posts = client.fetch_all_posts(search_id, None, start, end, page_size=page_limit,
                                               on_window_done=on_done)
            matches = [(p, [m for t in p.get("topics") or [] for m in index.get(_norm(t), [])])
                       for p in posts]
            matches = [(p, m) for p, m in matches if m]
            logger.info("search %s: %d post(s) fetched, %d match a saved topic",
                        search_id, len(posts), len(matches))
            authors = resolve_original_posters(client, search_id, [p for p, _ in matches],
                                               start - lookback, end)
            for post, pairs in matches:
                unique_pairs = {(t["theme_id"], tp): (t, tp) for t, tp in pairs}
                rows.extend(build_row(t, tp, post, search_id, authors) for t, tp in unique_pairs.values())

    base = out_dir / f"pulsar_posts_{subject}_{raw_start}_{raw_end}"
    csv_path, json_path = write_outputs(rows, base, COLUMNS)
    logger.info("Wrote %d row(s) to %s and %s", len(rows), csv_path, json_path)
    unique = build_unique_rows(rows)
    u_csv, u_json = write_outputs(unique, base.with_name(base.name + "_unique"), UNIQUE_COLUMNS)
    logger.info("Wrote %d distinct post(s) to %s and %s", len(unique), u_csv, u_json)


if __name__ == "__main__":
    run()
