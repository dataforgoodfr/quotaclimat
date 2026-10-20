"""Merge the two heads: create schema thematic_extracts and ad_merge tables

Both revisions were added on top of b2c4d6e8f0a1, so `alembic upgrade head` refused to run
("Multiple head revisions are present").

Revision ID: 09709fb55a7e
Revises: ca688f8b3a18, d3e5f7a9b1c2
Create Date: 2026-10-20 15:18:31

"""

from typing import Sequence, Union

revision: str = "09709fb55a7e"
down_revision: Union[str, Sequence[str], None] = ("ca688f8b3a18", "d3e5f7a9b1c2")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
