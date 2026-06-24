"""Write Pulsar themes/topics/posts into the pulsar_* tables (psycopg, no ORM).

Self-contained (no rrs.clustering import) so it stays light — the whole pipeline
runs in the lean downloader image, not rrs-base.
"""

import os
from datetime import datetime

import psycopg
from psycopg.types.json import Json

from rrs.pulsar.parse_themes import Theme
from rrs.utils.generate_id import get_consistent_hash


def _conninfo() -> str:
    return (
        f"host={os.getenv('RRS_PG_HOST', 'localhost')} "
        f"port={os.getenv('RRS_PG_PORT', 5432)} "
        f"dbname={os.getenv('RRS_PG_DATABASE', 'rrs_db')} "
        f"user={os.getenv('RRS_PG_USER', 'user')} "
        f"password={os.getenv('RRS_PG_PASSWORD', 'supersecret')}"
    )


def upsert_search(conn, search_id: str, name: str, folder_id: str, base_url: str) -> None:
    conn.execute(
        """
        INSERT INTO pulsar_searches (search_id, name, folder_id, base_url, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (search_id) DO UPDATE
            SET name = EXCLUDED.name, folder_id = EXCLUDED.folder_id,
                base_url = EXCLUDED.base_url, updated_at = now()
        """,
        (search_id, name, folder_id, base_url),
    )


def insert_theme(conn, search_id: str, theme: Theme, date_from, date_to) -> str:
    """Insert/refresh a theme + its topics for this pull window; return the theme_id."""
    theme_id = get_consistent_hash(f"pulsar-theme:{search_id}:{date_from}:{date_to}:{theme.title}")
    conn.execute(
        """
        INSERT INTO pulsar_themes
            (theme_id, search_id, title, summary, sentiment, volume, date_from, date_to)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (theme_id) DO UPDATE
            SET summary = EXCLUDED.summary, sentiment = EXCLUDED.sentiment, volume = EXCLUDED.volume
        """,
        (theme_id, search_id, theme.title, theme.summary, theme.sentiment, theme.volume, date_from, date_to),
    )
    for t in theme.topics:
        topic_id = get_consistent_hash(f"pulsar-topic:{theme_id}:{t.label}")
        conn.execute(
            """
            INSERT INTO pulsar_topics
                (topic_id, search_id, theme_id, label, volume, date_from, date_to)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (topic_id) DO UPDATE SET volume = EXCLUDED.volume
            """,
            (topic_id, search_id, theme_id, t.label, t.volume, date_from, date_to),
        )
    return theme_id


def _f(value):
    """None-safe float (Pulsar returns nulls)."""
    try:
        return float(value) if value not in (None, "", "None") else None
    except (TypeError, ValueError):
        return None


def _i(value):
    try:
        return int(value) if value not in (None, "", "None") else None
    except (TypeError, ValueError):
        return None


def upsert_posts(conn, search_id: str, theme_id: str, posts: list[dict]) -> int:
    """Upsert posts (dedup by pulsar_id) and link them to the theme. Returns count."""
    n = 0
    for p in posts:
        pid = p.get("pulsarId")
        if not pid:
            continue
        conn.execute(
            """
            INSERT INTO pulsar_posts (
                pulsar_id, search_id, title, content, url, news_article_link, domain_name, source,
                published_at, visibility, credibility_label, credibility_score, images, videos,
                thumbnail, user_name, user_screen_name, user_country_code, user_followers_count,
                user_friends_count, user_gender, user_bio, last_seen_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (pulsar_id) DO UPDATE SET
                title = EXCLUDED.title, content = EXCLUDED.content, visibility = EXCLUDED.visibility,
                credibility_label = EXCLUDED.credibility_label,
                credibility_score = EXCLUDED.credibility_score, last_seen_at = now()
            """,
            (
                pid, search_id, p.get("title"), p.get("content"), p.get("url"),
                p.get("newsArticleLink"), p.get("domainName"), p.get("source"), p.get("publishedAt"),
                _f(p.get("visibility")), p.get("credibilityLabel"), _f(p.get("credibilityScore")),
                Json(p.get("images")), Json(p.get("videos")), p.get("thumbnail"), p.get("userName"),
                p.get("userScreenName"), p.get("userCountryCode"), _i(p.get("userFollowersCount")),
                _i(p.get("userFriendsCount")), p.get("userGender"), p.get("userBio"),
            ),
        )
        conn.execute(
            "INSERT INTO pulsar_theme_posts (theme_id, pulsar_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (theme_id, pid),
        )
        n += 1
    return n


def connect():
    return psycopg.connect(_conninfo())
