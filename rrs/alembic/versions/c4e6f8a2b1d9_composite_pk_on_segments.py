"""composite pk on segments (segment_id, subject_id)

Revision ID: c4e6f8a2b1d9
Revises: b9d7e2f1a3c8
Create Date: 2026-07-09

"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "c4e6f8a2b1d9"
down_revision: Union[str, None] = "b9d7e2f1a3c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop FK from cases.segment_id → segments.segment_id
    op.drop_constraint("cases_segment_id_fkey", "cases", type_="foreignkey")

    # Drop existing single-column PK on segments
    op.drop_constraint("segments_pkey", "segments", type_="primary")

    # The unique constraint (segment_id, subject_id) is now the PK — drop it
    op.drop_constraint("uq_segments_segment_subject", "segments", type_="unique")

    # Add composite PK
    op.create_primary_key("segments_pkey", "segments", ["segment_id", "subject_id"])

    # Add composite FK from cases(segment_id, subject_id) → segments(segment_id, subject_id)
    op.create_foreign_key(
        "cases_segment_id_subject_id_fkey",
        "cases",
        "segments",
        ["segment_id", "subject_id"],
        ["segment_id", "subject_id"],
    )


def downgrade() -> None:
    op.drop_constraint("cases_segment_id_subject_id_fkey", "cases", type_="foreignkey")
    op.drop_constraint("segments_pkey", "segments", type_="primary")
    op.create_primary_key("segments_pkey", "segments", ["segment_id"])
    op.create_unique_constraint(
        "uq_segments_segment_subject", "segments", ["segment_id", "subject_id"]
    )
    op.create_foreign_key(
        "cases_segment_id_fkey", "cases", "segments", ["segment_id"], ["segment_id"]
    )
