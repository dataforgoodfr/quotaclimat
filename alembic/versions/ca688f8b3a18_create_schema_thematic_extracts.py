"""create schema thematic_extracts

Revision ID: ca688f8b3a18
Revises: b2c4d6e8f0a1
Create Date: 2026-10-20 16:14:37.559190

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ca688f8b3a18'
down_revision: Union[str, None] = 'b2c4d6e8f0a1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("create schema thematic_extracts")


def downgrade() -> None:
    op.execute("drop schema thematic_extracts")

