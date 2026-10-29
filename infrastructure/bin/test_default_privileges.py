"""Test, on the dev database, the default privileges proposed in PR #730 (reverted in #736).

Question: after `ALTER DEFAULT PRIVILEGES` (without IN SCHEMA), run once as the database admin, can the
readonly users (Metabase, RRS) read the tables and schemas this admin creates afterwards, without a new
`tf apply`?

Commands, to run in this order:

    python infrastructure/bin/test_default_privileges.py show    # owners and current default privileges
    python infrastructure/bin/test_default_privileges.py check   # expected before apply: new objects NOT readable
    python infrastructure/bin/test_default_privileges.py apply   # the SQL of PR #730 (persistent change)
    python infrastructure/bin/test_default_privileges.py check   # expected after apply: new objects readable
    # optional: re-apply the Scaleway readonly privilege (tf apply / console), then `show` and `check`
    #           again to see whether Scaleway keeps these default privileges
    python infrastructure/bin/test_default_privileges.py revert  # back to the initial state

`check` creates, as the admin, a table in an existing schema (--existing-schema, public by default;
advertising reproduces the case of ad_screenshot), a test schema and a table in it, then reads the
privileges of each readonly user, and drops everything it created. It changes nothing else.

Environment variables:
    PGHOST, PGPORT (5432), PGDATABASE (barometre)
    ADMIN_USER, ADMIN_PASSWORD: the database admin, i.e. the user the Alembic migrations run as
        (barometreclimat in prod, the instance admin in dev): default privileges only apply to the
        objects created by the user who sets them
    READERS: comma separated readonly users (metabase-analytics-dev,rrs-read-dev)
    READER_PASSWORD: optional, password of the first reader, to also run a real SELECT as this user
    PGSSLMODE: require by default (Scaleway)
"""

import argparse
import os
import sys
import uuid

import psycopg2
from psycopg2 import sql

READERS = [
    r.strip()
    for r in os.environ.get("READERS", "metabase-analytics-dev,rrs-read-dev").split(",")
    if r.strip()
]
TEST_PREFIX = "test_droits_"


def connect(user: str, password: str):
    """Autocommit connection: each statement is visible at once to the other connections."""
    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", 5432),
        dbname=os.environ.get("PGDATABASE", "barometre"),
        user=user,
        password=password,
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )
    conn.autocommit = True
    return conn


def admin_connection():
    return connect(os.environ["ADMIN_USER"], os.environ["ADMIN_PASSWORD"])


def readers_sql():
    return sql.SQL(", ").join(sql.Identifier(r) for r in READERS)


def existing_readers(cur) -> list[str]:
    cur.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (READERS,))
    found = {row[0] for row in cur.fetchall()}
    missing = [r for r in READERS if r not in found]
    if missing:
        sys.exit(f"Unknown users in this database: {', '.join(missing)} (set READERS)")
    return READERS


def print_rows(title: str, header: list[str], rows: list[tuple]) -> None:
    print(f"\n{title}")
    widths = [
        max(len(str(x)) for x in [h] + [row[i] for row in rows])
        for i, h in enumerate(header)
    ]
    print("  " + "  ".join(h.ljust(w) for h, w in zip(header, widths)))
    for row in rows:
        print("  " + "  ".join(str(x).ljust(w) for x, w in zip(row, widths)))
    if not rows:
        print("  (none)")


def show(cur) -> None:
    cur.execute("SELECT current_user")
    print(f"Connected as {cur.fetchone()[0]}")
    cur.execute(
        """
        SELECT schemaname, tableowner, count(*)
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            AND schemaname NOT LIKE 'metabase_cache%%'
        GROUP BY 1, 2 ORDER BY 1, 2
    """
    )
    print_rows(
        "Table owners (default privileges only cover the objects created by their user)",
        ["schema", "owner", "tables"],
        cur.fetchall(),
    )
    cur.execute(
        """
        SELECT pg_get_userbyid(defaclrole),
            coalesce(defaclnamespace::regnamespace::text, '(all schemas)'),
            CASE defaclobjtype WHEN 'r' THEN 'tables' WHEN 'S' THEN 'sequences'
                WHEN 'f' THEN 'functions' WHEN 'n' THEN 'schemas' ELSE defaclobjtype::text END,
            defaclacl::text
        FROM pg_default_acl ORDER BY 1, 2, 3
    """
    )
    print_rows(
        "Default privileges",
        ["created by", "schema", "objects", "granted"],
        cur.fetchall(),
    )


def _privileges(cur, schema: str, table: str) -> list[tuple]:
    rows = []
    for reader in READERS:
        cur.execute(
            "SELECT has_schema_privilege(%s, %s, 'USAGE'), has_table_privilege(%s, %s, 'SELECT')",
            (reader, schema, reader, f'"{schema}"."{table}"'),
        )
        usage, select = cur.fetchone()
        rows.append((reader, f"{schema}.{table}", usage, select))
    return rows


def _real_select(schema: str, table: str) -> str:
    password = os.environ.get("READER_PASSWORD")
    if not password:
        return "not run (no READER_PASSWORD)"
    try:
        conn = connect(READERS[0], password)
    except psycopg2.Error as e:
        return f"connection FAILED: {e}".strip()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            return f"OK ({cur.fetchone()[0]} rows)"
    except psycopg2.Error as e:
        return f"FAILED: {(e.pgerror or str(e)).splitlines()[0]}"
    finally:
        conn.close()


def check(cur, existing_schema: str) -> bool:
    existing_readers(cur)
    suffix = uuid.uuid4().hex[:8]
    new_schema = f"{TEST_PREFIX}{suffix}"
    created_existing_schema = False
    rows = []
    try:
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (existing_schema,))
        if cur.fetchone() is None:
            # simulates a schema created before the default privileges: created now, but before
            # this run's tables, which is what matters for the table privileges
            cur.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(existing_schema))
            )
            created_existing_schema = True

        # 1. new table in an existing schema (the case of advertising.ad_screenshot)
        table_in_existing = f"{TEST_PREFIX}table_{suffix}"
        cur.execute(
            sql.SQL("CREATE TABLE {}.{} (x int)").format(
                sql.Identifier(existing_schema), sql.Identifier(table_in_existing)
            )
        )
        rows += _privileges(cur, existing_schema, table_in_existing)
        select_existing = _real_select(existing_schema, table_in_existing)

        # 2. new schema and a table in it
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(new_schema)))
        cur.execute(
            sql.SQL("CREATE TABLE {}.t (x int)").format(sql.Identifier(new_schema))
        )
        rows += _privileges(cur, new_schema, "t")
        select_new = _real_select(new_schema, "t")
    finally:
        cur.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(new_schema)
            )
        )
        if created_existing_schema:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(existing_schema)
                )
            )
        else:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(existing_schema),
                    sql.Identifier(f"{TEST_PREFIX}table_{suffix}"),
                )
            )

    print_rows(
        "Privileges of the readonly users on objects just created by the admin",
        ["reader", "object", "schema USAGE", "table SELECT"],
        rows,
    )
    print(f"\nReal SELECT as {READERS[0]}:")
    print(f"  table in existing schema: {select_existing}")
    print(f"  table in new schema:      {select_new}")
    ok = all(usage and select for _, _, usage, select in rows)
    print(
        "\nResult: every reader can read the new objects"
        if ok
        else "\nResult: some new objects are NOT readable by the readers"
    )
    print("Test objects dropped.")
    return ok


def apply(cur) -> None:
    existing_readers(cur)
    statements = [
        sql.SQL("ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {}").format(
            readers_sql()
        ),
        sql.SQL("ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO {}").format(
            readers_sql()
        ),
    ]
    for statement in statements:
        print(statement.as_string(cur))
        cur.execute(statement)
    print("Default privileges set for the objects created by the current user.")


def revert(cur) -> None:
    existing_readers(cur)
    statements = [
        sql.SQL("ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM {}").format(
            readers_sql()
        ),
        sql.SQL("ALTER DEFAULT PRIVILEGES REVOKE USAGE ON SCHEMAS FROM {}").format(
            readers_sql()
        ),
    ]
    for statement in statements:
        print(statement.as_string(cur))
        cur.execute(statement)
    print("Default privileges removed.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=["show", "check", "apply", "revert"])
    parser.add_argument(
        "--existing-schema",
        default="public",
        help="existing schema for the 'new table in an existing schema' case (public by default; "
        "advertising reproduces ad_screenshot): only the test table is created there, then dropped",
    )
    args = parser.parse_args()

    # not `with conn`: since psycopg2 2.9 it opens a transaction even in autocommit, and the test
    # objects would not be visible to the reader's connection
    conn = admin_connection()
    try:
        with conn.cursor() as cur:
            if args.command == "show":
                show(cur)
            elif args.command == "check":
                ok = check(cur, args.existing_schema)
            elif args.command == "apply":
                apply(cur)
            elif args.command == "revert":
                revert(cur)
    finally:
        conn.close()
    if args.command == "check" and not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
