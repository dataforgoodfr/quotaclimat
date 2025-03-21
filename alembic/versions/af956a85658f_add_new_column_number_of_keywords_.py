"""Add new column number_of_keywords climat/biod/r

Revision ID: af956a85658f
Revises: a5c39db3c8e9
Create Date: 2024-09-12 14:15:12.049367

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'af956a85658f'
down_revision: Union[str, None] = 'a5c39db3c8e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('keywords', sa.Column('number_of_keywords_climat', sa.Integer(), nullable=True))
    op.add_column('keywords', sa.Column('number_of_keywords_biodiversite', sa.Integer(), nullable=True))
    op.add_column('keywords', sa.Column('number_of_keywords_ressources', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('keywords', 'number_of_keywords_ressources')
    op.drop_column('keywords', 'number_of_keywords_biodiversite')
    op.drop_column('keywords', 'number_of_keywords_climat')
    # ### end Alembic commands ###
