"""Add new column test for table keywords

Revision ID: a5c39db3c8e9
Revises: 5ccd746ee292
Create Date: 2024-09-12 14:10:26.305593

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a5c39db3c8e9'
down_revision: Union[str, None] = '5ccd746ee292'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
