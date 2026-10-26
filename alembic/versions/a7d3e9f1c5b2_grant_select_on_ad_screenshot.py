"""Grant select on advertising.ad_screenshot to the readers (Metabase), and on the next tables

The default privileges of the schema advertising only cover the tables created by _rdb_superadmin:
ad_screenshot, created by the migrations' user, was not readable by Metabase. The readers are those
of the advertising dbt models (my_dbt_project/dbt_project.yml), granted only where the role exists
(prod and dev don't have the same roles). The default privileges added here make the next tables
created in the schema by the migrations' user readable by them too.

Revision ID: a7d3e9f1c5b2
Revises: f4a8c2e6d1b3
Create Date: 2026-10-26

"""

from typing import Sequence, Union

from alembic import op

revision: str = "a7d3e9f1c5b2"
down_revision: Union[str, None] = "f4a8c2e6d1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

READERS = (
    "metabase-analytics-prod",
    "metabase-analytics-dev",
    "rrs-read-prod",
    "rrs-read-dev",
    "climateguard-reader-user",
)


def _for_each_existing_reader(statements: list[str]) -> None:
    """Run the statements (with %I for the role) for each reader role that exists."""
    roles = ", ".join(f"'{role}'" for role in READERS)
    executes = "\n".join(f"EXECUTE format('{s}', role);" for s in statements)
    op.execute(
        f"""
        DO $$
        DECLARE role text;
        BEGIN
            FOR role IN SELECT rolname FROM pg_roles WHERE rolname IN ({roles}) LOOP
                {executes}
            END LOOP;
        END $$;
    """
    )


def upgrade() -> None:
    _for_each_existing_reader(
        [
            "GRANT SELECT ON advertising.ad_screenshot TO %I",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA advertising GRANT SELECT ON TABLES TO %I",
        ]
    )


def downgrade() -> None:
    _for_each_existing_reader(
        [
            "ALTER DEFAULT PRIVILEGES IN SCHEMA advertising REVOKE SELECT ON TABLES FROM %I",
            "REVOKE SELECT ON advertising.ad_screenshot FROM %I",
        ]
    )
