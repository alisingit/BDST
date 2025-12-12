import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from hh_dm_utils import full_refresh_table


DM_SCHEMA = "dm"
TABLE_FQN = f"{DM_SCHEMA}.hh_industry_demand_trends_month"


def same_execution_date(execution_date):
    return execution_date


def build_mart(**_context) -> None:
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FQN} (
        month DATE NOT NULL,
        industry_id TEXT NOT NULL,
        industry_name TEXT NOT NULL,
        vacancies_cnt INTEGER NOT NULL,
        mom_abs INTEGER,
        mom_pct NUMERIC,
        PRIMARY KEY (month, industry_id)
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
            r.role_name AS industry_name
        FROM dds.fact_vacancy f
        JOIN vmap
          ON vmap.vacancy_id = f.vacancy_id
        JOIN dds.dim_professional_role r
          ON r.role_id = vmap.industry_id
    ),
    agg AS (
        SELECT
            month,
            industry_id,
            industry_name,
            COUNT(*)::int AS vacancies_cnt
        FROM base
        GROUP BY month, industry_id, industry_name
    )
    SELECT
        month,
        industry_id,
        industry_name,
        vacancies_cnt,
        (vacancies_cnt - LAG(vacancies_cnt) OVER (PARTITION BY industry_id ORDER BY month))::int AS mom_abs,
        CASE
            WHEN LAG(vacancies_cnt) OVER (PARTITION BY industry_id ORDER BY month) IS NULL THEN NULL
            WHEN LAG(vacancies_cnt) OVER (PARTITION BY industry_id ORDER BY month) = 0 THEN NULL
            ELSE vacancies_cnt::numeric / LAG(vacancies_cnt) OVER (PARTITION BY industry_id ORDER BY month) - 1
        END AS mom_pct
    FROM agg;
    """

    insert_sql_dm = f"""
    INSERT INTO {TABLE_FQN} (
        month,
        industry_id,
        industry_name,
        vacancies_cnt,
        mom_abs,
        mom_pct
    )
    VALUES (%s, %s, %s, %s, %s, %s);
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
    dag_id="hh_dm_industry_demand_trends",
    default_args=default_args,
    description="DM: тренды спроса по индустриям (месяц, MoM)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dm", "postgres_dm", "industry", "trends"],
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


