"""add pulsar tables

Revision ID: a7c3f1e9b2d4
Revises: 937031dc7d99
Create Date: 2026-06-19 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a7c3f1e9b2d4"
down_revision: Union[str, None] = "937031dc7d99"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_UTC = sa.text("(now() at time zone 'utc')")


def upgrade() -> None:
    op.create_table(
        "pulsar_searches",
        sa.Column("search_id", sa.String(), primary_key=True),
        sa.Column("name", sa.String()),
        sa.Column("folder_id", sa.String()),
        sa.Column("base_url", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_UTC),
        sa.Column("updated_at", sa.DateTime()),
    )

    op.create_table(
        "pulsar_themes",
        sa.Column("theme_id", sa.String(), primary_key=True),
        sa.Column("search_id", sa.String(), sa.ForeignKey("pulsar_searches.search_id"), nullable=False),
        sa.Column("title", sa.String()),
        sa.Column("summary", sa.Text()),
        sa.Column("sentiment", sa.String()),
        sa.Column("volume", sa.Integer()),
        sa.Column("date_from", sa.DateTime(timezone=True)),
        sa.Column("date_to", sa.DateTime(timezone=True)),
        sa.Column("pulled_at", sa.DateTime(timezone=True), server_default=_UTC),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_UTC),
        sa.UniqueConstraint("search_id", "date_from", "date_to", "title", name="uq_pulsar_theme_pull"),
    )

    op.create_table(
        "pulsar_topics",
        sa.Column("topic_id", sa.String(), primary_key=True),
        sa.Column("search_id", sa.String(), sa.ForeignKey("pulsar_searches.search_id"), nullable=False),
        sa.Column("theme_id", sa.String(), sa.ForeignKey("pulsar_themes.theme_id"), nullable=False),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("volume", sa.Integer()),
        sa.Column("date_from", sa.DateTime(timezone=True)),
        sa.Column("date_to", sa.DateTime(timezone=True)),
        sa.Column("pulled_at", sa.DateTime(timezone=True), server_default=_UTC),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_UTC),
    )

    op.create_table(
        "pulsar_posts",
        sa.Column("pulsar_id", sa.String(), primary_key=True),
        sa.Column("search_id", sa.String(), sa.ForeignKey("pulsar_searches.search_id")),
        sa.Column("title", sa.Text()),
        sa.Column("content", sa.Text()),
        sa.Column("url", sa.String()),
        sa.Column("news_article_link", sa.String()),
        sa.Column("domain_name", sa.String()),
        sa.Column("source", sa.String()),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("visibility", sa.Float()),
        sa.Column("credibility_label", sa.String()),
        sa.Column("credibility_score", sa.Float()),
        sa.Column("images", sa.JSON()),
        sa.Column("videos", sa.JSON()),
        sa.Column("thumbnail", sa.String()),
        sa.Column("user_name", sa.String()),
        sa.Column("user_screen_name", sa.String()),
        sa.Column("user_country_code", sa.String()),
        sa.Column("user_followers_count", sa.Integer()),
        sa.Column("user_friends_count", sa.Integer()),
        sa.Column("user_gender", sa.String()),
        sa.Column("user_bio", sa.Text()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=_UTC),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=_UTC),
    )

    op.create_table(
        "pulsar_theme_posts",
        sa.Column("theme_id", sa.String(), sa.ForeignKey("pulsar_themes.theme_id"), primary_key=True),
        sa.Column("pulsar_id", sa.String(), sa.ForeignKey("pulsar_posts.pulsar_id"), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_UTC),
    )


def downgrade() -> None:
    op.drop_table("pulsar_theme_posts")
    op.drop_table("pulsar_posts")
    op.drop_table("pulsar_topics")
    op.drop_table("pulsar_themes")
    op.drop_table("pulsar_searches")
