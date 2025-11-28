from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DDS_SCHEMA = "dds"


def _get_postgres_conn() -> PostgresHook:
    return PostgresHook(postgres_conn_id="postgres_ods")


def check_row_counts(**_context):
    """
    Сравнение количества строк в ODS и DDS (фактовая таблица).
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    # 1. Общее количество строк в ODS
    cur.execute(
        "SELECT COUNT(*) FROM ods_hh_vacancies_pg;"
    )
    ods_total = cur.fetchone()[0]

    # 2. Количество "валидных" строк ODS, которые вообще могут попасть в DDS:
    #    - у вакансии есть employer.id (именно по нему строится связь с dim_employer)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM ods_hh_vacancies_pg v
        WHERE v.raw->'employer'->>'id' IS NOT NULL;
        """
    )
    ods_eligible = cur.fetchone()[0]

    # 3. Количество строк в фактовой таблице DDS
    cur.execute(f"SELECT COUNT(*) FROM {DDS_SCHEMA}.fact_vacancy;")
    dds_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    logging.info(
        "ODS total=%s, ODS eligible=%s (with employer.id), "
        "DDS fact_vacancy=%s",
        ods_total,
        ods_eligible,
        dds_count,
    )

    if ods_eligible == 0:
        logging.info("ODS has no vacancies with employer.id, skipping comparison")
        return

    if ods_eligible != dds_count:
        raise AirflowException(
            "Row count mismatch between ODS and DDS fact_vacancy "
            f"for eligible rows: ODS_eligible={ods_eligible}, "
            f"DDS={dds_count}, ODS_total={ods_total}"
        )


def check_null_foreign_keys(**_context):
    """
    Проверка отсутствия NULL в ключевых измерениях фактовой таблицы.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {DDS_SCHEMA}.fact_vacancy
        WHERE employer_sk IS NULL
           OR published_date IS NULL;
        """
    )
    invalid_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    logging.info("Rows with NULL foreign keys in fact_vacancy: %s", invalid_count)

    if invalid_count > 0:
        raise AirflowException(
            f"Found {invalid_count} rows in fact_vacancy with NULL foreign keys"
        )


def check_published_date_range(**_context):
    """
    Проверка, что даты публикации находятся в разумном диапазоне:
    не в будущем и не ранее 2000-01-01.
    """
    hook = _get_postgres_conn()
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT
            MIN(published_date),
            MAX(published_date),
            COUNT(*) FILTER (WHERE published_date < DATE '2000-01-01') AS too_old,
            COUNT(*) FILTER (WHERE published_date > CURRENT_DATE)     AS in_future
        FROM {DDS_SCHEMA}.fact_vacancy;
        """
    )
    min_date, max_date, too_old, in_future = cur.fetchone()

    cur.close()
    conn.close()

    logging.info(
        "fact_vacancy published_date: min=%s, max=%s, too_old=%s, in_future=%s",
        min_date,
        max_date,
        too_old,
        in_future,
    )

    if too_old > 0 or in_future > 0:
        raise AirflowException(
            f"Found {too_old} rows with published_date < 2000-01-01 "
            f"and {in_future} rows with published_date in the future"
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_vacancies_dds_data_quality_postgres",
    default_args=default_args,
    description="Проверки качества данных для DDS-слоя по вакансиям HH (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="45 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dds", "data_quality", "postgres"],
) as dq_dag:
    # Учебный режим: проверки качества можно запускать вручную
    # без строгой привязки к успешному ран-у DDS DAG.
    wait_dds = EmptyOperator(
        task_id="wait_for_dds_vacancies_postgres",
    )

    row_counts_task = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    fk_task = PythonOperator(
        task_id="check_null_foreign_keys",
        python_callable=check_null_foreign_keys,
    )

    date_range_task = PythonOperator(
        task_id="check_published_date_range",
        python_callable=check_published_date_range,
    )

    wait_dds >> [row_counts_task, fk_task, date_range_task]


