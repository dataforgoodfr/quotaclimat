"""Replace the keywords country index by a composite (country, channel_name, start) index

In production, keywords is a partitioned table and its idx_country index was
created by hand (never through a migration). Postgres refuses
CREATE/DROP INDEX CONCURRENTLY on a partitioned table, so on a partitioned
table we follow the documented procedure: create the parent index ON ONLY
(instant, INVALID), build each partition's index CONCURRENTLY (ingestion keeps
writing), then ATTACH it to the parent, which becomes valid once every
partition is attached. On a plain table (local / test DB) a single
CREATE INDEX CONCURRENTLY is enough.

Every step is idempotent, so the migration can run after the index has been
built by hand. To build it by hand in production before deploying (in psql,
outside of any transaction, at off-peak hours):

    -- 1. Parent index: instant, stays INVALID until every partition is attached
    CREATE INDEX IF NOT EXISTS ix_keywords_country_channel_name_start
        ON ONLY public.keywords (country, channel_name, start);

    -- 2. Each partition's index, built without blocking writes, then attached
    --    to the parent. \\gexec runs each generated statement separately.
    --    Assumes a single level of partitions: check with
    --    SELECT * FROM pg_partition_tree('public.keywords');
    SELECT format(
               'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (country, channel_name, start)',
               c.relname || '_country_channel_name_start_idx', n.nspname, c.relname
           ),
           format(
               'ALTER INDEX public.ix_keywords_country_channel_name_start ATTACH PARTITION %I.%I',
               n.nspname, c.relname || '_country_channel_name_start_idx'
           )
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.inhparent = 'public.keywords'::regclass
    ORDER BY c.relname
    \\gexec

    -- 3. Must return true before going further
    SELECT indisvalid FROM pg_index
    WHERE indexrelid = 'public.ix_keywords_country_channel_name_start'::regclass;

    -- 4. Drop the old index. No CONCURRENTLY on a partitioned table: this takes
    --    a short ACCESS EXCLUSIVE lock, lock_timeout avoids queueing behind a
    --    long query (and blocking everything else behind it). Retry if it times out.
    SET lock_timeout = '10s';
    DROP INDEX IF EXISTS public.idx_country;
    RESET lock_timeout;

If a CONCURRENTLY build fails, Postgres leaves an INVALID index behind that
IF NOT EXISTS will then skip: drop it by hand before re-running.

Revision ID: e3f1a7c9b2d4
Revises: b2c4d6e8f0a1
Create Date: 2026-10-12

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e3f1a7c9b2d4"
down_revision: Union[str, None] = "b2c4d6e8f0a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

COMPOSITE_INDEX = "ix_keywords_country_channel_name_start"
COMPOSITE_COLUMNS = ["country", "channel_name", "start"]
COUNTRY_INDEX = "idx_country"  # created by hand in production
COUNTRY_COLUMNS = ["country"]


def _is_partitioned(schema: str, table: str) -> bool:
    return (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT c.relkind = 'p' FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :table"
            ),
            {"schema": schema, "table": table},
        )
        .scalar()
    )


def _partitions(schema: str, table: str) -> list:
    return (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT n.nspname, c.relname FROM pg_inherits i "
                "JOIN pg_class c ON c.oid = i.inhrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE i.inhparent = format('%I.%I', :schema, :table)::regclass "
                "ORDER BY c.relname"
            ),
            {"schema": schema, "table": table},
        )
        .all()
    )


def _create_index(schema: str, table: str, index_name: str, columns: list) -> None:
    """Build an index without blocking writes, partitioned tables included."""
    cols = ", ".join(columns)
    if not _is_partitioned(schema, table):
        op.execute(
            f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{index_name}" '
            f'ON "{schema}"."{table}" ({cols})'
        )
        return

    op.execute(
        f'CREATE INDEX IF NOT EXISTS "{index_name}" ON ONLY "{schema}"."{table}" ({cols})'
    )
    for partition_schema, partition in _partitions(schema, table):
        partition_index = f"{partition}_{'_'.join(columns)}_idx"
        _create_index(partition_schema, partition, partition_index, columns)
        op.execute(
            f'ALTER INDEX "{schema}"."{index_name}" '
            f'ATTACH PARTITION "{partition_schema}"."{partition_index}"'
        )


def _drop_index(index_name: str) -> None:
    # No CONCURRENTLY on a partitioned table: keep the ACCESS EXCLUSIVE lock
    # wait short rather than queueing every other query behind it.
    op.execute("SET lock_timeout = '10s'")
    op.execute(f'DROP INDEX IF EXISTS public."{index_name}"')
    op.execute("RESET lock_timeout")


def upgrade() -> None:
    with op.get_context().autocommit_block():
        _create_index("public", "keywords", COMPOSITE_INDEX, COMPOSITE_COLUMNS)
        _drop_index(COUNTRY_INDEX)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        _create_index("public", "keywords", COUNTRY_INDEX, COUNTRY_COLUMNS)
        _drop_index(COMPOSITE_INDEX)
