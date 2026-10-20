"""create schema thematic_extracts

Revision ID: 63656acfa7e6
Revises: 060edb2b9305
Create Date: 2026-10-20 16:26:13.537202

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '63656acfa7e6'
down_revision = '060edb2b9305'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("create schema thematic_extracts")


def downgrade() -> None:
    op.execute("drop schema thematic_extracts")


