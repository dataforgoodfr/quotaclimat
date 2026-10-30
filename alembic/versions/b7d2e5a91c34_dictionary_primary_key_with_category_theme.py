"""Dictionary primary key: (keyword, language, category, theme)

The model declares pk_keyword_language_category_theme (since #389, May 2025), but no
migration changed the primary key created by c14476f987a0 (pk_keyword_language on
(keyword, language)), so a keyword listed under several themes made update_dictionary fail
with a UniqueViolation.

Inserted in the history right after 44f13b7eebd4 (which added category and theme). Databases
already past this point do not run it: their key is fixed by hand.

Revision ID: b7d2e5a91c34
Revises: 44f13b7eebd4
Create Date: 2025-05-26 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

PK_COLUMNS = ["keyword", "language", "category", "theme"]

revision: str = "b7d2e5a91c34"
down_revision: Union[str, None] = "44f13b7eebd4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pk = sa.inspect(op.get_bind()).get_pk_constraint("dictionary")
    if pk["constrained_columns"] == PK_COLUMNS:
        return

    # category and theme were added nullable by 44f13b7eebd4, a primary key column cannot be
    op.execute("UPDATE dictionary SET category = '' WHERE category IS NULL")
    op.execute("UPDATE dictionary SET theme = '' WHERE theme IS NULL")
    op.execute("ALTER TABLE dictionary DROP CONSTRAINT IF EXISTS pk_keyword_language")
    op.create_primary_key("pk_keyword_language_category_theme", "dictionary", PK_COLUMNS)


def downgrade() -> None:
    # Deliberately a no-op: (keyword, language) was a bug, and restoring it would break
    # databases where the key was fixed by hand. Reverting past 44f13b7eebd4 drops the
    # category/theme columns, and the key with them.
    pass
