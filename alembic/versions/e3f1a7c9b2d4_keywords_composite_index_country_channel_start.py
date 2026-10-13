"""Replace the keywords country index by a composite (country, channel_name, start) index

In production, keywords is partitioned by RANGE (start) and its idx_country
index was created by hand (never through a migration). On a partitioned table,
CREATE INDEX builds the index on every partition (and on partitions created
later), but blocks writes to keywords for the whole build: CONCURRENTLY is not
supported on partitioned tables.

Run it by hand in production before deploying, while no ingestion job is
running; the migration is then a no-op:

    CREATE INDEX IF NOT EXISTS ix_keywords_country_channel_name_start
        ON public.keywords (country, channel_name, start);
    DROP INDEX IF EXISTS public.idx_country;

Revision ID: e3f1a7c9b2d4
Revises: b2c4d6e8f0a1
Create Date: 2026-10-12

"""

from typing import Sequence, Union

from alembic import op

revision: str = "e3f1a7c9b2d4"
down_revision: Union[str, None] = "b2c4d6e8f0a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_keywords_country_channel_name_start",
        "keywords",
        ["country", "channel_name", "start"],
        unique=False,
        if_not_exists=True,
    )
    op.drop_index("idx_country", table_name="keywords", if_exists=True)


def downgrade() -> None:
    op.create_index(
        "idx_country", "keywords", ["country"], unique=False, if_not_exists=True
    )
    op.drop_index(
        "ix_keywords_country_channel_name_start", table_name="keywords", if_exists=True
    )
