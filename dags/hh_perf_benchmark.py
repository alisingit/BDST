from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from perf_benchmark_module import run_all_benchmarks, print_results


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def run_benchmark_task(**context):
    """
    Запускает бенчмарк PostgreSQL, MySQL и MongoDB и логирует результаты.

    Для подключения используются параметры из docker-compose:
      - PostgreSQL ODS: localhost:5433, db=ods_hh, user=airflow, password=airflow
      - MySQL ODS:     localhost:3307, db=ods_hh, user=airflow, password=airflow
      - MongoDB ODS:   localhost:27018, db=ods_hh

    Вся логика замеров находится в модуле performance_benchmark.
    """
    ti = context["ti"]

    # Количество прогонов каждого запроса можно при необходимости
    # сделать параметром DAG (conf) или Variable; для простоты фиксируем 5.
    runs = 5
    results = run_all_benchmarks(runs=runs)

    # Логируем в стандартный лог Airflow
    logging.info("Benchmark completed, total result rows: %d", len(results))
    for r in results:
        logging.info(
            "DB=%s query=%s runs=%d min=%.2fms avg=%.2fms max=%.2fms std=%.2fms",
            r.db,
            r.query_name,
            r.runs,
            r.min_ms,
            r.avg_ms,
            r.max_ms,
            r.std_ms,
        )

    # Пушим агрегированные результаты в XCom как список словарей
    ti.xcom_push(
        key="benchmark_results",
        value=[
            {
                "db": r.db,
                "query_name": r.query_name,
                "runs": r.runs,
                "min_ms": r.min_ms,
                "avg_ms": r.avg_ms,
                "max_ms": r.max_ms,
                "std_ms": r.std_ms,
            }
            for r in results
        ],
    )

    # Заодно печатаем таблицу в stdout (попадёт в лог задачи)
    print_results(results)


with DAG(
    dag_id="hh_perf_benchmark",
    default_args=default_args,
    description="Бенчмарк производительности PostgreSQL, MySQL и MongoDB на ODS-данных HH",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # запуск по требованию
    catchup=False,
    max_active_runs=1,
    tags=["hh", "perf", "benchmark"],
) as dag_perf_benchmark:
    run_benchmark = PythonOperator(
        task_id="run_perf_benchmark",
        python_callable=run_benchmark_task,
    )


