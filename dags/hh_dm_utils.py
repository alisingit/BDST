import logging
from typing import Iterable, Sequence

from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_ods_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_ods")


def get_dm_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_dm")


def ensure_dm_schema(cur, schema: str = "dm") -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def full_refresh_table(
    *,
    table_fqn: str,
    create_ddl: str,
    select_sql_ods: str,
    insert_sql_dm: str,
    batch_size: int = 2000,
) -> None:
    """
    Полный рефреш витрины в postgres_dm:
    - CREATE TABLE IF NOT EXISTS
    - TRUNCATE
    - SELECT в postgres_ods
    - INSERT батчами в postgres_dm
    """
    ods_hook = get_ods_hook()
    dm_hook = get_dm_hook()

    dm_conn = dm_hook.get_conn()
    dm_cur = dm_conn.cursor()

    ods_conn = ods_hook.get_conn()
    ods_cur = ods_conn.cursor()

    try:
        logging.info("Ensuring DM table exists: %s", table_fqn)
        ensure_dm_schema(dm_cur, schema=table_fqn.split(".")[0])
        dm_cur.execute(create_ddl)
        dm_conn.commit()

        logging.info("Truncating DM table: %s", table_fqn)
        dm_cur.execute(f"TRUNCATE TABLE {table_fqn};")
        dm_conn.commit()

        logging.info("Selecting ODS/DDS aggregates for %s", table_fqn)
        ods_cur.execute(select_sql_ods)

        total = 0
        while True:
            rows: Sequence[tuple] = ods_cur.fetchmany(batch_size)
            if not rows:
                break
            dm_cur.executemany(insert_sql_dm, rows)
            dm_conn.commit()
            total += len(rows)
            logging.info("Inserted %d rows into %s (total=%d)", len(rows), table_fqn, total)

        logging.info("Full refresh done for %s: total rows=%d", table_fqn, total)
    finally:
        try:
            ods_cur.close()
        finally:
            ods_conn.close()

        try:
            dm_cur.close()
        finally:
            dm_conn.close()


