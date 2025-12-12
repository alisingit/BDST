from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def _trigger(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    """
    Триггерит DAG и ждёт его завершения, чтобы обеспечить строгую последовательность:
    ODS → DDS → DM.
    """
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        execution_date="{{ execution_date }}",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )


with DAG(
    dag_id="hh_pipeline_orchestrator",
    default_args=default_args,
    description="Оркестрация: ODS → DDS → DM для HH (последовательно)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "orchestrator"],
) as dag:
    start = EmptyOperator(task_id="start")

    # ODS (Postgres)
    ods_salary_industries = _trigger(
        task_id="ods_salary_industries_to_postgres",
        dag_id="hh_salary_industries_to_postgres",
    )
    ods_vacancies = _trigger(
        task_id="ods_vacancies_to_postgres",
        dag_id="hh_vacancies_to_postgres",
    )
    ods_employers = _trigger(
        task_id="ods_employers_to_postgres",
        dag_id="hh_employers_to_postgres",
    )

    # DDS
    dds_salary_industries = _trigger(
        task_id="dds_salary_industries_postgres",
        dag_id="hh_salary_industries_dds_postgres",
    )
    dds_vacancies = _trigger(
        task_id="dds_vacancies_postgres",
        dag_id="hh_vacancies_dds_postgres",
    )
    dds_employers = _trigger(
        task_id="dds_employers_postgres",
        dag_id="hh_employers_dds_postgres",
    )

    # DM
    dm_salary = _trigger(
        task_id="dm_salary_by_industry_region_currency",
        dag_id="hh_dm_salary_by_industry_region_currency",
    )
    dm_regions = _trigger(
        task_id="dm_regions_by_industry",
        dag_id="hh_dm_regions_by_industry",
    )
    dm_trends = _trigger(
        task_id="dm_industry_demand_trends",
        dag_id="hh_dm_industry_demand_trends",
    )
    dm_top_employers = _trigger(
        task_id="dm_top_employers_by_industry_region",
        dag_id="hh_dm_top_employers_by_industry_region",
    )
    dm_currency = _trigger(
        task_id="dm_currency_structure_by_industry_region",
        dag_id="hh_dm_currency_structure_by_industry_region",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> ods_salary_industries
        >> ods_vacancies
        >> ods_employers
        >> dds_salary_industries
        >> dds_vacancies
        >> dds_employers
        >> dm_salary
        >> dm_regions
        >> dm_trends
        >> dm_top_employers
        >> dm_currency
        >> end
    )


