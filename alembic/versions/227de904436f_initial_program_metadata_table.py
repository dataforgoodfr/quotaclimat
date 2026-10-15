"""initial program_metadata table

Like `keywords`/`sitemap_table`/`channel_metadata`, program_metadata was
created via Base.metadata.create_all() (see
postgres/schemas/models.py::update_program_metadata), not through a
tracked migration. The first migration that touches it (5bff4dceda53)
only ALTERs it, so a fresh database has nothing to create it with. This
revision recreates it as it existed right before 5bff4dceda53, based on
git history of postgres/schemas/models.py at commit 39a28c17cd17
(2024-04-30, "feat: add program metadata table").

Revision ID: 227de904436f
Revises: 356882459cec
Create Date: 2026-10-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '227de904436f'
down_revision: Union[str, None] = '356882459cec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'program_metadata',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('channel_name', sa.String(), nullable=False),
        sa.Column('channel_title', sa.String(), nullable=False),
        sa.Column('duration_minutes', sa.Integer(), nullable=True),
        sa.Column('weekday', sa.Integer(), nullable=True),
        sa.Column('start', sa.String(), nullable=False),
        sa.Column('end', sa.String(), nullable=False),
        sa.Column('channel_program', sa.String(), nullable=False),
        sa.Column('channel_program_type', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table('program_metadata')
