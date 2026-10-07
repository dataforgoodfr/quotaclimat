"""initial base tables (sitemap, keywords, channel_metadata)

These tables predate Alembic: they were originally created via
Base.metadata.create_all() (see postgres/schemas/models.py::create_tables)
rather than through a tracked migration, so a fresh/empty database has no
migration that lays down their initial shape. This revision recreates them
as they existed right before 2c48f626a749 (the first tracked migration,
which assumes `keywords` already exists and only adds columns to it).
Column set reconstructed from git history of postgres/schemas/models.py at
commit c081dc6656a749 (2024-04-05), the last commit before the
channel_program / channel_program_type / category columns were added
alongside migration 2c48f626a749.

Revision ID: cd649aa751f4
Revises:
Create Date: 2026-10-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cd649aa751f4'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sitemap_table',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('publication_name', sa.String(), nullable=False),
        sa.Column('news_title', sa.Text(), nullable=False),
        sa.Column('download_date', sa.DateTime(), nullable=True),
        sa.Column('news_publication_date', sa.DateTime(), nullable=True),
        sa.Column('news_keywords', sa.Text(), nullable=True),
        sa.Column('section', sa.Text(), nullable=True),
        sa.Column('image_caption', sa.Text(), nullable=True),
        sa.Column('media_type', sa.Text(), nullable=True),
        sa.Column('url', sa.Text(), nullable=True),
        sa.Column('news_description', sa.Text(), nullable=True),
        sa.Column('updated_on', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        if_not_exists=True,
    )

    op.create_table(
        'channel_metadata',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('channel_name', sa.String(), nullable=False),
        sa.Column('channel_title', sa.String(), nullable=False),
        sa.Column('duration_minutes', sa.Integer(), nullable=True),
        sa.Column('weekday', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        if_not_exists=True,
    )

    op.create_table(
        'keywords',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('channel_name', sa.String(), nullable=False),
        sa.Column('channel_radio', sa.Boolean(), nullable=True),
        sa.Column('start', sa.DateTime(), nullable=False),
        sa.Column('plaintext', sa.Text(), nullable=True),
        sa.Column('theme', sa.JSON(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text("(now() at time zone 'utc')"),
            nullable=True,
        ),
        sa.Column('keywords_with_timestamp', sa.JSON(), nullable=True),
        sa.Column('number_of_keywords', sa.Integer(), nullable=True),
        sa.Column('srt', sa.JSON(), nullable=True),
        sa.Column('number_of_changement_climatique_constat', sa.Integer(), nullable=True),
        sa.Column('number_of_changement_climatique_causes_directes', sa.Integer(), nullable=True),
        sa.Column('number_of_changement_climatique_consequences', sa.Integer(), nullable=True),
        sa.Column('number_of_attenuation_climatique_solutions_directes', sa.Integer(), nullable=True),
        sa.Column('number_of_adaptation_climatique_solutions_directes', sa.Integer(), nullable=True),
        sa.Column('number_of_ressources_naturelles_concepts_generaux', sa.Integer(), nullable=True),
        sa.Column('number_of_ressources_naturelles_causes', sa.Integer(), nullable=True),
        sa.Column('number_of_ressources_naturelles_solutions', sa.Integer(), nullable=True),
        sa.Column('number_of_biodiversite_concepts_generaux', sa.Integer(), nullable=True),
        sa.Column('number_of_biodiversite_causes_directes', sa.Integer(), nullable=True),
        sa.Column('number_of_biodiversite_consequences', sa.Integer(), nullable=True),
        sa.Column('number_of_biodiversite_solutions_directes', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id', 'start'),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table('keywords')
    op.drop_table('channel_metadata')
    op.drop_table('sitemap_table')
