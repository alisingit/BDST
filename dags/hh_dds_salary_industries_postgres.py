from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


DDS_SCHEMA = "dds"


def same_execution_date(execution_date):
    return execution_date


def _get_postgres_conn() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_ods")


def prepare_dds_schema_salary_industries(**_context):
    """
    Создание таблиц измерений для справочника отраслей/сфер (industries)
    в слое DDS на основе ODS-таблицы ods_hh_salary_industries_pg.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {DDS_SCHEMA};

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.dim_industry (
            industry_id TEXT PRIMARY KEY,
            industry_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS {DDS_SCHEMA}.dim_sphere (
            sphere_id TEXT PRIMARY KEY,
            sphere_name TEXT NOT NULL,
            industry_id TEXT NOT NULL REFERENCES {DDS_SCHEMA}.dim_industry (industry_id)
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("DDS tables for salary industries (dim_industry, dim_sphere) are ready")


def load_dim_industry(**_context):
    """
    Загрузка измерения отраслей (industry) из ODS-таблицы ods_hh_salary_industries_pg.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.dim_industry (industry_id, industry_name)
        SELECT DISTINCT
               industry_id,
               industry_name
        FROM ods_hh_salary_industries_pg
        WHERE industry_id IS NOT NULL
        ON CONFLICT (industry_id) DO UPDATE
            SET industry_name = EXCLUDED.industry_name;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("dim_industry loaded/updated")


def load_dim_sphere(**_context):
    """
    Загрузка измерения сфер (подотраслей) из ODS-таблицы ods_hh_salary_industries_pg.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        INSERT INTO {DDS_SCHEMA}.dim_sphere (sphere_id, sphere_name, industry_id)
        SELECT DISTINCT
               sphere_id,
               sphere_name,
               industry_id
        FROM ods_hh_salary_industries_pg
        WHERE sphere_id IS NOT NULL
        ON CONFLICT (sphere_id) DO UPDATE
            SET sphere_name = EXCLUDED.sphere_name,
                industry_id = EXCLUDED.industry_id;
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("dim_sphere loaded/updated")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_salary_industries_dds_postgres",
    default_args=default_args,
    description="DDS-измерения для справочника отраслей/сфер HH (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dds", "salary_industries", "postgres"],
) as dds_salary_dag:
    # В прод-режиме DDS ждёт завершения ODS-DAG по справочнику отраслей/сфер.
    wait_ods_salary = ExternalTaskSensor(
        task_id="wait_for_ods_salary_industries_postgres",
        external_dag_id="hh_salary_industries_to_postgres",
        external_task_id="load_salary_industries_to_postgres",
        execution_date_fn=same_execution_date,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    prepare_schema_salary = PythonOperator(
        task_id="prepare_dds_schema_salary_industries",
        python_callable=prepare_dds_schema_salary_industries,
    )

    dim_industry_task = PythonOperator(
        task_id="load_dim_industry",
        python_callable=load_dim_industry,
    )

    dim_sphere_task = PythonOperator(
        task_id="load_dim_sphere",
        python_callable=load_dim_sphere,
    )

    wait_ods_salary >> prepare_schema_salary >> dim_industry_task >> dim_sphere_task


