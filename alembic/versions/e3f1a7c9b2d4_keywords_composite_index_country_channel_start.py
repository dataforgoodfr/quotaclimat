"""Replace the keywords country index by a composite (country, channel_name, start) index

The single-column index on keywords.country was created by hand against the
production DB (never through a migration), so its name is not known here: we
look up every plain btree index on exactly (country) and drop it once the
composite index, which covers the same queries through its leftmost column,
is built.

Both operations run CONCURRENTLY (outside of a transaction) so that ingestion
jobs can keep writing to keywords while the index is being built. If the build
fails, Postgres leaves an INVALID index behind: drop it by hand before
re-running the migration, otherwise IF NOT EXISTS will skip it.

Revision ID: e3f1a7c9b2d4
Revises: b2c4d6e8f0a1
Create Date: 2026-10-12

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e3f1a7c9b2d4"
down_revision: Union[str, None] = "b2c4d6e8f0a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

COMPOSITE_INDEX = "ix_keywords_country_channel_name_start"
COUNTRY_INDEX = "ix_keywords_country"

FIND_COUNTRY_INDEXES = sa.text(
    """
    SELECT i.relname
    FROM pg_index x
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_class t ON t.oid = x.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_am am ON am.oid = i.relam
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.indkey[0]
    WHERE n.nspname = 'public'
      AND t.relname = 'keywords'
      AND am.amname = 'btree'
      AND x.indnatts = 1
      AND a.attname = 'country'
      AND x.indpred IS NULL
      AND x.indexprs IS NULL
      AND NOT x.indisunique
    """
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            COMPOSITE_INDEX,
            "keywords",
            ["country", "channel_name", "start"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )
        country_indexes = op.get_bind().execute(FIND_COUNTRY_INDEXES).scalars().all()
        for index_name in country_indexes:
            op.drop_index(
                index_name,
                table_name="keywords",
                postgresql_concurrently=True,
                if_exists=True,
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            COUNTRY_INDEX,
            "keywords",
            ["country"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )
        op.drop_index(
            COMPOSITE_INDEX,
            table_name="keywords",
            postgresql_concurrently=True,
            if_exists=True,
        )
