import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from hh_dm_utils import full_refresh_table


DM_SCHEMA = "dm"
TABLE_FQN = f"{DM_SCHEMA}.hh_currency_structure_by_industry_region"


def same_execution_date(execution_date):
    return execution_date


def build_mart(**_context) -> None:
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FQN} (
        industry_id TEXT NOT NULL,
        industry_name TEXT NOT NULL,
        area_id BIGINT,
        area_name TEXT,
        currency TEXT NOT NULL,
        vacancies_cnt INTEGER NOT NULL,
        currency_share NUMERIC,
        PRIMARY KEY (industry_id, area_id, currency)
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
            vmap.industry_id AS industry_id,
            r.role_name AS industry_name,
            f.area_id AS area_id,
            da.area_name AS area_name,
            COALESCE(NULLIF(f.salary_currency, ''), 'UNKNOWN') AS currency
        FROM dds.fact_vacancy f
        JOIN vmap
          ON vmap.vacancy_id = f.vacancy_id
        JOIN dds.dim_professional_role r
          ON r.role_id = vmap.industry_id
        LEFT JOIN dds.dim_area da
          ON da.area_id = f.area_id
    ),
    agg AS (
        SELECT
            industry_id,
            industry_name,
            area_id,
            area_name,
            currency,
            COUNT(*)::int AS vacancies_cnt
        FROM base
        GROUP BY industry_id, industry_name, area_id, area_name, currency
    ),
    totals AS (
        SELECT industry_id, area_id, SUM(vacancies_cnt)::numeric AS total_cnt
        FROM agg
        GROUP BY industry_id, area_id
    )
    SELECT
        a.industry_id,
        a.industry_name,
        a.area_id,
        a.area_name,
        a.currency,
        a.vacancies_cnt,
        CASE
            WHEN t.total_cnt = 0 THEN NULL
            ELSE a.vacancies_cnt::numeric / t.total_cnt
        END AS currency_share
    FROM agg a
    JOIN totals t
      ON t.industry_id = a.industry_id
     AND t.area_id IS NOT DISTINCT FROM a.area_id;
    """

    insert_sql_dm = f"""
    INSERT INTO {TABLE_FQN} (
        industry_id,
        industry_name,
        area_id,
        area_name,
        currency,
        vacancies_cnt,
        currency_share
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s);
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
    dag_id="hh_dm_currency_structure_by_industry_region",
    default_args=default_args,
    description="DM: структура валют по индустриям и регионам (доли)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dm", "postgres_dm", "industry", "region", "currency"],
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


