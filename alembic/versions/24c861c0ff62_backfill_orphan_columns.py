"""backfill columns that were never added by a tracked migration

Two more columns exist in production/postgres/schemas/models.py but were
never actually added by any Alembic migration (same orphan-column issue
as the orphan tables fixed in cd649aa751f4 / 227de904436f / c14476f987a0):

- keywords.country / program_metadata.country: added directly against
  the DB alongside commit 3966ffcbb6a1 (2025-04-29, "feat/i8n: handle
  multiple countries"), never via a migration.
- keywords.number_of_keywords_20/30/40: migration 4ccd746ee291 uses
  op.alter_column (a no-op on a non-existent column) where it should
  have used op.add_column, so these were never actually created by the
  migration chain either - they only exist in prod because they were
  added manually.

Revision ID: 24c861c0ff62
Revises: 227de904436f
Create Date: 2026-10-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '24c861c0ff62'
down_revision: Union[str, None] = '227de904436f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # if_not_exists guards environments (e.g. production) where these
    # columns were already added manually outside of any migration.
    op.add_column('keywords', sa.Column('country', sa.Text(), nullable=True), if_not_exists=True)
    op.add_column('program_metadata', sa.Column('country', sa.Text(), nullable=True), if_not_exists=True)
    op.add_column('keywords', sa.Column('number_of_keywords_20', sa.Integer(), nullable=True), if_not_exists=True)
    op.add_column('keywords', sa.Column('number_of_keywords_30', sa.Integer(), nullable=True), if_not_exists=True)
    op.add_column('keywords', sa.Column('number_of_keywords_40', sa.Integer(), nullable=True), if_not_exists=True)


def downgrade() -> None:
    # if_exists guards against 4ccd746ee291's downgrade() already having
    # dropped number_of_keywords_20/30/40 further down the chain.
    op.drop_column('keywords', 'number_of_keywords_40', if_exists=True)
    op.drop_column('keywords', 'number_of_keywords_30', if_exists=True)
    op.drop_column('keywords', 'number_of_keywords_20', if_exists=True)
    op.drop_column('program_metadata', 'country', if_exists=True)
    op.drop_column('keywords', 'country', if_exists=True)
