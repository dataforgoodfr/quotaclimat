"""add_duplicate_status_column

Add duplicate_status column to factiva_articles table for duplicate detection.

Revision ID: g1a2b3c4d5e7
Revises: f9a2b3c4d5e6
Create Date: 2025-12-18 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "g1a2b3c4d5e7"
down_revision: Union[str, None] = "f9a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add duplicate_status column with default value "NOT_DUP"
    op.add_column(
        "factiva_articles",
        sa.Column("duplicate_status", sa.String(), nullable=True, server_default="NOT_DUP"),
    )
    
    # Update existing rows to set default value
    op.execute(
        """
        UPDATE factiva_articles 
        SET duplicate_status = 'NOT_DUP' 
        WHERE duplicate_status IS NULL
        """
    )


def downgrade() -> None:
    # Remove duplicate_status column
    op.drop_column("factiva_articles", "duplicate_status")
