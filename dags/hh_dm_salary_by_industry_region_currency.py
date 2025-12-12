import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from hh_dm_utils import full_refresh_table


DM_SCHEMA = "dm"
TABLE_FQN = f"{DM_SCHEMA}.hh_salary_by_industry_region_currency_month"


def same_execution_date(execution_date):
    return execution_date


def build_mart(**_context) -> None:
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FQN} (
        month DATE NOT NULL,
        industry_id TEXT NOT NULL,
        industry_name TEXT NOT NULL,
        area_id BIGINT,
        area_name TEXT,
        currency TEXT NOT NULL,
        vacancies_cnt INTEGER NOT NULL,
        vacancies_with_salary_cnt INTEGER NOT NULL,
        salary_min NUMERIC,
        salary_avg NUMERIC,
        salary_max NUMERIC,
        PRIMARY KEY (month, industry_id, area_id, currency)
    );
    """

    select_sql_ods = """
    WITH vmap AS (
        SELECT
            b.vacancy_id,
            MIN(r.role_id) AS industry_id
        FROM dds.bridge_vacancy_professional_role b
        JOIN dds.dim_professional_role r
          ON r.role_id = b.role_id
        GROUP BY b.vacancy_id
    ),
    base AS (
        SELECT
            date_trunc('month', f.published_date)::date AS month,
            vmap.industry_id AS industry_id,
            r.role_name AS industry_name,
            f.area_id AS area_id,
            da.area_name AS area_name,
            COALESCE(NULLIF(f.salary_currency, ''), 'UNKNOWN') AS currency,
            CASE
                WHEN f.salary_from IS NOT NULL AND f.salary_to IS NOT NULL THEN (f.salary_from + f.salary_to) / 2
                ELSE COALESCE(f.salary_from, f.salary_to)
            END AS salary_point
        FROM dds.fact_vacancy f
        JOIN vmap
          ON vmap.vacancy_id = f.vacancy_id
        JOIN dds.dim_professional_role r
          ON r.role_id = vmap.industry_id
        LEFT JOIN dds.dim_area da
          ON da.area_id = f.area_id
    )
    SELECT
        month,
        industry_id,
        industry_name,
        area_id,
        area_name,
        currency,
        COUNT(*)::int AS vacancies_cnt,
        COUNT(*) FILTER (WHERE salary_point IS NOT NULL)::int AS vacancies_with_salary_cnt,
        MIN(salary_point) AS salary_min,
        AVG(salary_point) AS salary_avg,
        MAX(salary_point) AS salary_max
    FROM base
    GROUP BY
        month,
        industry_id,
        industry_name,
        area_id,
        area_name,
        currency;
    """

    insert_sql_dm = f"""
    INSERT INTO {TABLE_FQN} (
        month,
        industry_id,
        industry_name,
        area_id,
        area_name,
        currency,
        vacancies_cnt,
        vacancies_with_salary_cnt,
        salary_min,
        salary_avg,
        salary_max
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    logging.info("Building mart %s", TABLE_FQN)
    full_refresh_table(
        table_fqn=TABLE_FQN,
        create_ddl=create_ddl,
        select_sql_ods=select_sql_ods,
        insert_sql_dm=insert_sql_dm,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_dm_salary_by_industry_region_currency",
    default_args=default_args,
    description="DM: зарплаты по индустриям/регионам/валютам (месяц)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dm", "postgres_dm", "salary", "industry", "region", "currency"],
) as dag:
    wait_vacancies_dds = ExternalTaskSensor(
        task_id="wait_for_dds_vacancies_bridge",
        external_dag_id="hh_vacancies_dds_postgres",
        external_task_id="load_bridge_vacancy_professional_role",
        execution_date_fn=same_execution_date,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    build = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart,
    )

    wait_vacancies_dds >> build


