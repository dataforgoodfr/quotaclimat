"""Standalone storage for Pulsar TRAC data (themes, topics, posts).

Kept separate from the RRS clusters tables (all `pulsar_` prefixed). Grain:
  - searches: the saved Pulsar searches we pull.
  - themes / topics: time-windowed snapshots (one set per pull) — kept for trends.
  - posts: stable entities (deduped by Pulsar's `pulsar_id`), upserted.
  - theme↔post: per-pull membership.
"""

from datetime import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text as sql_text

from rrs.schemas.base import RRSBase

_UTC_NOW = sql_text("(now() at time zone 'utc')")


class PulsarSearch(RRSBase):
    __tablename__ = "pulsar_searches"

    search_id = Column(String, primary_key=True)  # Pulsar search id, e.g. "201830"
    name = Column(String, nullable=True)
    folder_id = Column(String, nullable=True)
    base_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)
    updated_at = Column(DateTime(), default=datetime.now, onupdate=datetime.now, nullable=True)

    themes = relationship("PulsarTheme", back_populates="search")


class PulsarTheme(RRSBase):
    """One narrative theme for a given search + pull window (snapshot)."""

    __tablename__ = "pulsar_themes"
    __table_args__ = (
        UniqueConstraint("search_id", "date_from", "date_to", "title", name="uq_pulsar_theme_pull"),
    )

    theme_id = Column(String, primary_key=True)  # surrogate (hash of search+window+title)
    search_id = Column(String, ForeignKey("pulsar_searches.search_id"), nullable=False)
    title = Column(String, nullable=True)
    summary = Column(Text, nullable=True)
    sentiment = Column(String, nullable=True)
    volume = Column(Integer, nullable=True)  # "Volume of Posts"
    date_from = Column(DateTime(timezone=True), nullable=True)
    date_to = Column(DateTime(timezone=True), nullable=True)
    pulled_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)
    created_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)

    search = relationship("PulsarSearch", back_populates="themes")
    topics = relationship("PulsarTopic", back_populates="theme")
    theme_posts = relationship("PulsarThemePost", back_populates="theme")


class PulsarTopic(RRSBase):
    """A constituent topic of a theme (with its volume), for a pull window."""

    __tablename__ = "pulsar_topics"

    topic_id = Column(String, primary_key=True)  # surrogate per (theme, label)
    search_id = Column(String, ForeignKey("pulsar_searches.search_id"), nullable=False)
    theme_id = Column(String, ForeignKey("pulsar_themes.theme_id"), nullable=False)
    label = Column(String, nullable=False)  # e.g. "nucléaire"
    volume = Column(Integer, nullable=True)
    date_from = Column(DateTime(timezone=True), nullable=True)
    date_to = Column(DateTime(timezone=True), nullable=True)
    pulled_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)
    created_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)

    theme = relationship("PulsarTheme", back_populates="topics")


class PulsarPost(RRSBase):
    """A post/article (stable entity, deduped by pulsar_id, upserted)."""

    __tablename__ = "pulsar_posts"

    pulsar_id = Column(String, primary_key=True)  # e.g. "0_201823_16_d95f..."
    search_id = Column(String, ForeignKey("pulsar_searches.search_id"), nullable=True)
    title = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    url = Column(String, nullable=True)
    news_article_link = Column(String, nullable=True)
    domain_name = Column(String, nullable=True)
    source = Column(String, nullable=True)  # e.g. ONLINE_NEWS
    published_at = Column(DateTime(timezone=True), nullable=True)
    visibility = Column(Float, nullable=True)
    credibility_label = Column(String, nullable=True)
    credibility_score = Column(Float, nullable=True)
    images = Column(JSON, nullable=True)
    videos = Column(JSON, nullable=True)
    thumbnail = Column(String, nullable=True)
    user_name = Column(String, nullable=True)
    user_screen_name = Column(String, nullable=True)
    user_country_code = Column(String, nullable=True)
    user_followers_count = Column(Integer, nullable=True)
    user_friends_count = Column(Integer, nullable=True)
    user_gender = Column(String, nullable=True)
    user_bio = Column(Text, nullable=True)
    first_seen_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)
    last_seen_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)

    theme_posts = relationship("PulsarThemePost", back_populates="post")


class PulsarThemePost(RRSBase):
    """Per-pull membership: which posts belong to which theme."""

    __tablename__ = "pulsar_theme_posts"

    theme_id = Column(String, ForeignKey("pulsar_themes.theme_id"), primary_key=True)
    pulsar_id = Column(String, ForeignKey("pulsar_posts.pulsar_id"), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=_UTC_NOW)

    theme = relationship("PulsarTheme", back_populates="theme_posts")
    post = relationship("PulsarPost", back_populates="theme_posts")
