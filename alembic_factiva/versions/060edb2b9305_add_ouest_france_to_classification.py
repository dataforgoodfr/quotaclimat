"""Add ouest France to classification

Revision ID: 060edb2b9305
Revises: 5833799c8abc
Create Date: 2026-04-07 13:26:27.363471

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "060edb2b9305"
down_revision: Union[str, None] = "5833799c8abc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            INSERT INTO source_classification (source_type, source_name, source_code, source_owner, media_all, source_region)
            VALUES ('Presse Régionale', 'Ouest-France', 'OUESTFRANCE', 'SIPA Ouest-France', 'Ouest-France', 'Bretagne')
            ON CONFLICT (source_code) DO NOTHING
            """
        )
    )
    op.execute(
        sa.text(
            """
            INSERT INTO source_classification (source_type, source_name, source_code, source_owner, media_all, source_region)
            VALUES ('Presse Régionale', 'Ouest-France.fr', 'OUESTFRAFR', 'SIPA Ouest-France', 'Ouest-France', 'Bretagne')
            ON CONFLICT (source_code) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    # Remove OuestFrance sources from source_classification
    op.execute(
        sa.text(
            "DELETE FROM source_classification WHERE source_code IN ('OUESTFRANCE', 'OUESTFRAFR')"
        )
    )
