"""time monitored new table

Revision ID: 827fb6dde3bb
Revises: c08231a9eb37
Create Date: 2025-04-29 13:29:54.299095

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '827fb6dde3bb'
down_revision: Union[str, None] = 'c08231a9eb37'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create the time_monitored table
    op.create_table(
        'time_monitored',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('channel_name', sa.String(), nullable=False),
        sa.Column('start', sa.DateTime(), nullable=False),
        sa.Column('duration_minutes', sa.Integer(), nullable=True),
        sa.Column('country', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    # Drop the time_monitored table
    op.drop_table('time_monitored')