"""keywords: program_id foreign key

Revision ID: 4333bc46985d
Revises: ac96222af6fe
Create Date: 2025-03-21 14:25:06.180296

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '4333bc46985d'
down_revision: Union[str, None] = 'ac96222af6fe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('keywords', sa.Column('program_metadata_id', sa.Text(), nullable=True))
    op.create_foreign_key(None, 'keywords', 'program_metadata', ['program_metadata_id'], ['id'])
    op.alter_column('sitemap_table', 'download_date',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               type_=sa.DateTime(),
               existing_nullable=True)
    op.alter_column('sitemap_table', 'news_publication_date',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               type_=sa.DateTime(),
               existing_nullable=True)
    op.alter_column('sitemap_table', 'updated_on',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               type_=sa.DateTime(),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('sitemap_table', 'updated_on',
               existing_type=sa.DateTime(),
               type_=postgresql.TIMESTAMP(timezone=True),
               existing_nullable=True)
    op.alter_column('sitemap_table', 'news_publication_date',
               existing_type=sa.DateTime(),
               type_=postgresql.TIMESTAMP(timezone=True),
               existing_nullable=True)
    op.alter_column('sitemap_table', 'download_date',
               existing_type=sa.DateTime(),
               type_=postgresql.TIMESTAMP(timezone=True),
               existing_nullable=True)
    op.drop_constraint(None, 'keywords', type_='foreignkey')
    op.drop_column('keywords', 'program_metadata_id')
    # ### end Alembic commands ###
