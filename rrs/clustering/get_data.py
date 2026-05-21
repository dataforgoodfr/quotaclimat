import os
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import psycopg
from dotenv import load_dotenv

from rrs.utils.generate_id import get_consistent_hash

CLIMATE_SUBJECT_ID = get_consistent_hash("climate")

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def _conninfo() -> str:
    return (
        f"host={os.getenv('RRS_PG_HOST', 'localhost')} "
        f"port={os.getenv('RRS_PG_PORT', 5432)} "
        f"dbname={os.getenv('RRS_PG_DATABASE', 'rrs_db')} "
        f"user={os.getenv('RRS_PG_USER', 'user')} "
        f"password={os.getenv('RRS_PG_PASSWORD', 'supersecret')}"
    )


def get_data_from_db(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    conditions = []
    params: list = []
    if start_date is not None:
        conditions.append("c.start >= %s")
        params.append(start_date)
    if end_date is not None:
        conditions.append("c.start < %s")
        params.append(end_date + timedelta(days=1))
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT
            c.case_id,
            c.segment_id,
            c.start,
            c.text
        FROM cases c
        {where}
    """
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)

def get_clusters_from_db(
    active_since: Optional[date] = None,
) -> pd.DataFrame:
    """Return clusters from the database.

    If active_since is given, only clusters that have had at least one case
    assigned on or after that date are returned.
    Columns: cluster_id, cluster_text.
    """
    if active_since is not None:
        query = """
            SELECT cl.cluster_id, cl.cluster_text
            FROM clusters cl
            JOIN case_to_clusters ctc ON ctc.cluster_id = cl.cluster_id
            JOIN cases c ON c.case_id = ctc.case_id
            GROUP BY cl.cluster_id, cl.cluster_text
            HAVING MAX(c.start) >= %s
        """
        params: list = [active_since]
    else:
        query = "SELECT cluster_id, cluster_text FROM clusters"
        params = []
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)


def write_clusters_to_db(assignments_df: pd.DataFrame) -> None:
    """Upsert clusters and case→cluster mappings into the database.

    Expects assignments_df to have at least: case_id, cluster_id, cluster_text.
    """
    if assignments_df.empty:
        print("  No assignments to write to DB.")
        return

    clusters = (
        assignments_df[["cluster_id", "cluster_text"]]
        .dropna(subset=["cluster_id"])
        .drop_duplicates(subset=["cluster_id"])
    )
    mappings = (
        assignments_df[["case_id", "cluster_id"]]
        .dropna(subset=["case_id", "cluster_id"])
        .drop_duplicates()
    )

    case_ids = mappings["case_id"].unique().tolist()

    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO clusters (cluster_id, subject_id, cluster_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (cluster_id) DO UPDATE
                    SET subject_id = EXCLUDED.subject_id,
                        cluster_text = EXCLUDED.cluster_text
                """,
                [(row.cluster_id, CLIMATE_SUBJECT_ID, row.cluster_text) for row in clusters.itertuples()],
            )
            cur.execute(
                "DELETE FROM case_to_clusters WHERE case_id = ANY(%s)",
                (case_ids,),
            )
            cur.executemany(
                """
                INSERT INTO case_to_clusters (case_id, cluster_id)
                VALUES (%s, %s)
                """,
                [(row.case_id, row.cluster_id) for row in mappings.itertuples()],
            )
        conn.commit()

    print(f"  Written {len(clusters)} cluster(s) and {len(mappings)} case→cluster mapping(s) to DB.")


if __name__ == "__main__":
    get_data_from_db()
