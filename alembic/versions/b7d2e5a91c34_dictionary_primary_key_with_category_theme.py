"""Dictionary primary key: (keyword, language, category, theme)

The model declares pk_keyword_language_category_theme, but the database still had the
initial pk_keyword_language (keyword, language), so a keyword listed under several themes
made update_dictionary fail with a UniqueViolation.

Revision ID: b7d2e5a91c34
Revises: f4a8c2e6d1b3
Create Date: 2026-10-30

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

PK_COLUMNS = ["keyword", "language", "category", "theme"]

revision: str = "b7d2e5a91c34"
down_revision: Union[str, None] = "f4a8c2e6d1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pk = sa.inspect(op.get_bind()).get_pk_constraint("dictionary")
    if pk["constrained_columns"] == PK_COLUMNS:
        # already fixed (other databases, or by hand): nothing to do
        return

    # category and theme were added as nullable, a primary key column cannot be
    op.execute("UPDATE dictionary SET category = '' WHERE category IS NULL")
    op.execute("UPDATE dictionary SET theme = '' WHERE theme IS NULL")
    op.execute("ALTER TABLE dictionary DROP CONSTRAINT IF EXISTS pk_keyword_language")
    op.execute(
        "ALTER TABLE dictionary DROP CONSTRAINT IF EXISTS pk_keyword_language_category_theme"
    )
    op.create_primary_key(
        "pk_keyword_language_category_theme",
        "dictionary",
        PK_COLUMNS,
    )


def downgrade() -> None:
    # The old key is stricter: this fails if a keyword exists under several themes.
    op.drop_constraint("pk_keyword_language_category_theme", "dictionary", type_="primary")
    op.create_primary_key("pk_keyword_language", "dictionary", ["keyword", "language"])
