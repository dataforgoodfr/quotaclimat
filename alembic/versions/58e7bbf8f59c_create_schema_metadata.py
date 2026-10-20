"""Create schema metadata (dbt_objects, the list of the relations built by dbt)

dbt builds metadata.dbt_objects (my_dbt_project/models/metadata/dbt_objects.sql), formerly
analytics.dbt_objects: the old table is dropped, else it would be listed as a table that dbt no
longer builds. IF NOT EXISTS: a dbt run before this migration creates the schema itself.

Revision ID: 58e7bbf8f59c
Revises: 09709fb55a7e
Create Date: 2026-10-20 15:18:31

"""

from typing import Sequence, Union

from alembic import op

revision: str = "58e7bbf8f59c"
down_revision: Union[str, None] = "09709fb55a7e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("create schema if not exists metadata")
    op.execute("drop table if exists analytics.dbt_objects")


def downgrade() -> None:
    # only holds dbt_objects, rebuilt by the next dbt run
    op.execute("drop schema if exists metadata cascade")
