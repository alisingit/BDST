from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DDS_SCHEMA = "dds"


def _get_postgres_conn() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_ods")


def prepare_dds_schema_employers(**_context):
    """
    Создание таблиц DDS для работодателей на основе ODS-таблицы ods_hh_employers_pg.
    Здесь мы переиспользуем измерение работодателей dim_employer (SCD2),
    а также создаём факт-таблицу снимков показателей работодателя.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {DDS_SCHEMA};

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.dim_employer (
            employer_sk BIGSERIAL PRIMARY KEY,
            employer_id BIGINT NOT NULL,
            employer_name TEXT NOT NULL,
            alternate_url TEXT,
            trusted BOOLEAN,
            valid_from TIMESTAMPTZ NOT NULL,
            valid_to TIMESTAMPTZ NOT NULL,
            is_current BOOLEAN NOT NULL,
            CONSTRAINT uq_dim_employer_business UNIQUE (employer_id, is_current)
        );

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.fact_employer_snapshot (
            employer_id BIGINT NOT NULL,
            snapshot_date DATE NOT NULL,
            employer_sk BIGINT NOT NULL REFERENCES {DDS_SCHEMA}.dim_employer (employer_sk),
            area_name TEXT,
            open_vacancies INTEGER,
            site_url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT pk_fact_employer_snapshot PRIMARY KEY (employer_id, snapshot_date)
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("DDS tables for employers (dim_employer, fact_employer_snapshot) are ready")


def load_dim_employer_from_employers(**_context):
    """
    Дополнительное наполнение измерения работодателей dim_employer
    по данным из ODS-таблицы ods_hh_employers_pg (SCD2).
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TEMP TABLE tmp_employer_ods AS
        SELECT DISTINCT
               id::bigint         AS employer_id,
               name               AS employer_name,
               site_url           AS alternate_url,
               NULL::boolean      AS trusted
        FROM ods_hh_employers_pg
        WHERE id IS NOT NULL;
        """
    )

    now_ts = datetime.utcnow()

    cur.execute(
        f"""
        UPDATE {DDS_SCHEMA}.dim_employer d
        SET valid_to = %s,
            is_current = FALSE
        FROM tmp_employer_ods s
        WHERE d.employer_id = s.employer_id
          AND d.is_current = TRUE
          AND (
                d.employer_name IS DISTINCT FROM s.employer_name
            OR  d.alternate_url IS DISTINCT FROM s.alternate_url
          );
        """,
        (now_ts,),
    )

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.dim_employer
            (employer_id, employer_name, alternate_url, trusted, valid_from, valid_to, is_current)
        SELECT s.employer_id,
               s.employer_name,
               s.alternate_url,
               s.trusted,
               %s AS valid_from,
               '9999-12-31'::timestamptz AS valid_to,
               TRUE AS is_current
        FROM tmp_employer_ods s
        LEFT JOIN {DDS_SCHEMA}.dim_employer d
          ON d.employer_id = s.employer_id
         AND d.is_current = TRUE
        WHERE d.employer_id IS NULL;
        """,
        (now_ts,),
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("dim_employer (SCD2) updated from ods_hh_employers_pg")


def load_fact_employer_snapshot(**_context):
    """
    Загрузка факт-таблицы снимков работодателей из ODS-таблицы ods_hh_employers_pg.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.fact_employer_snapshot (
            employer_id,
            snapshot_date,
            employer_sk,
            area_name,
            open_vacancies,
            site_url,
            created_at
        )
        SELECT
            e.id::bigint AS employer_id,
            CURRENT_DATE AS snapshot_date,
            d.employer_sk,
            e.area_name,
            e.open_vacancies,
            e.site_url,
            now()        AS created_at
        FROM ods_hh_employers_pg e
        JOIN {DDS_SCHEMA}.dim_employer d
          ON d.employer_id = e.id::bigint
         AND d.is_current = TRUE
        LEFT JOIN {DDS_SCHEMA}.fact_employer_snapshot f
          ON f.employer_id = e.id::bigint
         AND f.snapshot_date = CURRENT_DATE
        WHERE f.employer_id IS NULL;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("fact_employer_snapshot loaded/updated for CURRENT_DATE")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_employers_dds_postgres",
    default_args=default_args,
    description="DDS-слой по работодателям HH (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dds", "employers", "postgres"],
) as dds_employers_dag:
    # Учебный режим: убираем реальный ExternalTaskSensor и
    # заменяем его пустым оператором, чтобы DDS можно было запускать
    # независимо от текущего состояния ODS-DAG.
    wait_ods_employers = EmptyOperator(
        task_id="wait_for_ods_employers_postgres",
    )

    prepare_schema_employers = PythonOperator(
        task_id="prepare_dds_schema_employers",
        python_callable=prepare_dds_schema_employers,
    )

    dim_employer_from_ods_task = PythonOperator(
        task_id="load_dim_employer_from_employers",
        python_callable=load_dim_employer_from_employers,
    )

    fact_employer_snapshot_task = PythonOperator(
        task_id="load_fact_employer_snapshot",
        python_callable=load_fact_employer_snapshot,
    )

    wait_ods_employers >> prepare_schema_employers >> dim_employer_from_ods_task >> fact_employer_snapshot_task


