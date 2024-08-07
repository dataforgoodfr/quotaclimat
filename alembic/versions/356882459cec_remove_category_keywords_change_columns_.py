"""Remove: category keywords / change columns names

Revision ID: 356882459cec
Revises: 2c48f626a749
Create Date: 2024-04-29 10:14:27.240887

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '356882459cec'
down_revision: Union[str, None] = '2c48f626a749'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('keywords', sa.Column('number_of_ressources', sa.Integer(), nullable=True))
    op.add_column('keywords', sa.Column('number_of_ressources_solutions', sa.Integer(), nullable=True))
    op.drop_column('keywords', 'number_of_ressources_naturelles_causes')
    op.drop_column('keywords', 'number_of_ressources_naturelles_concepts_generaux')
    op.drop_column('keywords', 'category')
    op.drop_column('keywords', 'number_of_ressources_naturelles_solutions')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('keywords', sa.Column('number_of_ressources_naturelles_solutions', sa.INTEGER(), autoincrement=False, nullable=True))
    op.add_column('keywords', sa.Column('category', postgresql.JSON(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('keywords', sa.Column('number_of_ressources_naturelles_concepts_generaux', sa.INTEGER(), autoincrement=False, nullable=True))
    op.add_column('keywords', sa.Column('number_of_ressources_naturelles_causes', sa.INTEGER(), autoincrement=False, nullable=True))
    op.drop_column('keywords', 'number_of_ressources_solutions')
    op.drop_column('keywords', 'number_of_ressources')
    # ### end Alembic commands ###
