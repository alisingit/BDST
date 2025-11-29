from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


DDS_SCHEMA = "dds"


def _get_postgres_conn() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_ods")


def prepare_dds_schema(**_context):
    """
    Создание схемы DDS и базовых таблиц измерений/фактов.
    Таблицы созданы с упором на PostgreSQL как основное аналитическое хранилище.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {DDS_SCHEMA};

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.dim_date (
            date_key DATE PRIMARY KEY,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            week INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.dim_area (
            area_id BIGINT PRIMARY KEY,
            area_name TEXT NOT NULL
        );

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

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.fact_vacancy (
            vacancy_id BIGINT PRIMARY KEY,
            name TEXT,
            employer_sk BIGINT NOT NULL REFERENCES {DDS_SCHEMA}.dim_employer (employer_sk),
            area_id BIGINT REFERENCES {DDS_SCHEMA}.dim_area (area_id),
            published_date DATE NOT NULL REFERENCES {DDS_SCHEMA}.dim_date (date_key),
            salary_from NUMERIC,
            salary_to NUMERIC,
            salary_currency TEXT,
            salary_gross BOOLEAN,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("DDS schema and base tables are ready")


def load_dim_date(**_context):
    """
    Заполняет измерение дат на основе диапазона published_at в ODS.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("SELECT MIN(published_at)::date, MAX(published_at)::date FROM ods_hh_vacancies_pg;")
    min_date, max_date = cur.fetchone()

    if min_date is None or max_date is None:
        logging.info("No data in ods_hh_vacancies_pg to build dim_date")
        cur.close()
        conn.close()
        return

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.dim_date (date_key, year, month, day, day_of_week, week)
        SELECT d::date                                                   AS date_key,
               EXTRACT(YEAR FROM d)::int                                AS year,
               EXTRACT(MONTH FROM d)::int                               AS month,
               EXTRACT(DAY FROM d)::int                                 AS day,
               EXTRACT(DOW FROM d)::int                                 AS day_of_week,
               EXTRACT(WEEK FROM d)::int                                AS week
        FROM generate_series(%s::date, %s::date, interval '1 day') AS d
        ON CONFLICT (date_key) DO NOTHING;
        """,
        (min_date, max_date),
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("dim_date loaded/updated")


def load_dim_area(**_context):
    """
    Загружает справочник регионов (area) из ODS.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.dim_area (area_id, area_name)
        SELECT DISTINCT
               (raw->'area'->>'id')::bigint      AS area_id,
               COALESCE(raw->'area'->>'name', area_name) AS area_name
        FROM ods_hh_vacancies_pg
        WHERE raw->'area'->>'id' IS NOT NULL
        ON CONFLICT (area_id) DO UPDATE
            SET area_name = EXCLUDED.area_name;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("dim_area loaded/updated")


def load_dim_employer(**_context):
    """
    Загружает измерение работодателей (SCD2: хранение истории изменений атрибутов).
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("CREATE TEMP TABLE tmp_employer AS "
                "SELECT DISTINCT "
                "       (raw->'employer'->>'id')::bigint AS employer_id, "
                "       COALESCE(raw->'employer'->>'name', employer_name) AS employer_name, "
                "       raw->'employer'->>'alternate_url' AS alternate_url, "
                "       NULLIF(raw->'employer'->>'trusted', '')::boolean AS trusted "
                "FROM ods_hh_vacancies_pg "
                "WHERE raw->'employer'->>'id' IS NOT NULL;")

    now_ts = datetime.utcnow()

    cur.execute(
        f"""
        UPDATE {DDS_SCHEMA}.dim_employer d
        SET valid_to = %s,
            is_current = FALSE
        FROM tmp_employer s
        WHERE d.employer_id = s.employer_id
          AND d.is_current = TRUE
          AND (
                d.employer_name IS DISTINCT FROM s.employer_name
            OR  d.alternate_url IS DISTINCT FROM s.alternate_url
            OR  d.trusted IS DISTINCT FROM s.trusted
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
        FROM tmp_employer s
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
    logging.info("dim_employer (SCD2) loaded/updated")


def load_fact_vacancy(**_context):
    """
    Загружает фактовую таблицу вакансий из ODS в DDS.
    Используются измерения dim_date, dim_area, dim_employer.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.fact_vacancy (
            vacancy_id,
            name,
            employer_sk,
            area_id,
            published_date,
            salary_from,
            salary_to,
            salary_currency,
            salary_gross,
            created_at
        )
        SELECT
            v.id AS vacancy_id,
            v.name,
            de.employer_sk,
            (v.raw->'area'->>'id')::bigint AS area_id,
            v.published_at::date AS published_date,
            NULLIF(v.raw->'salary'->>'from', '')::numeric AS salary_from,
            NULLIF(v.raw->'salary'->>'to', '')::numeric AS salary_to,
            v.raw->'salary'->>'currency' AS salary_currency,
            NULLIF(v.raw->'salary'->>'gross', '')::boolean AS salary_gross,
            now() AS created_at
        FROM ods_hh_vacancies_pg v
        JOIN {DDS_SCHEMA}.dim_employer de
          ON de.employer_id = (v.raw->'employer'->>'id')::bigint
         AND de.is_current = TRUE
        LEFT JOIN {DDS_SCHEMA}.fact_vacancy f
          ON f.vacancy_id = v.id
        WHERE f.vacancy_id IS NULL;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("fact_vacancy loaded/updated")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_vacancies_dds_postgres",
    default_args=default_args,
    description="Формирование DDS по вакансиям HH (PostgreSQL, звёздная схема)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dds", "postgres"],
) as dds_dag:
    # В прод-режиме DDS ждёт завершения ODS-DAG по вакансиям в PostgreSQL.
    wait_ods = ExternalTaskSensor(
        task_id="wait_for_ods_vacancies_postgres",
        external_dag_id="hh_vacancies_to_postgres",
        external_task_id="load_to_postgres",
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    prepare_schema = PythonOperator(
        task_id="prepare_dds_schema",
        python_callable=prepare_dds_schema,
    )

    dim_date_task = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date,
    )

    dim_area_task = PythonOperator(
        task_id="load_dim_area",
        python_callable=load_dim_area,
    )

    dim_employer_task = PythonOperator(
        task_id="load_dim_employer",
        python_callable=load_dim_employer,
    )

    fact_vacancy_task = PythonOperator(
        task_id="load_fact_vacancy",
        python_callable=load_fact_vacancy,
    )

    wait_ods >> prepare_schema >> [dim_date_task, dim_area_task, dim_employer_task] >> fact_vacancy_task


