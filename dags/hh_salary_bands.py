from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


def build_salary_bands_postgres(**context):
    """
    Строит витрину зарплатных вилок на основе ODS-таблицы ods_hh_vacancies_pg.

    Гранулярность: месяц публикации × название вакансии × регион × валюта.
    Агрегаты: количество вакансий и min/avg/max по средней точке вилки.
    """
    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    logging.info("Creating salary bands mart table in Postgres if not exists")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dm_hh_salary_bands_pg (
            month           DATE,
            vacancy_name    TEXT,
            area_name       TEXT,
            currency        TEXT,
            vacancies_cnt   INTEGER,
            salary_min      NUMERIC,
            salary_avg      NUMERIC,
            salary_max      NUMERIC,
            PRIMARY KEY (month, vacancy_name, area_name, currency)
        );
        """
    )

    logging.info("Refreshing salary bands data in Postgres (full reload)")
    cur.execute("TRUNCATE TABLE dm_hh_salary_bands_pg;")

    # В salary HH может быть только from, только to или оба.
    # Берём среднюю точку вилки как:
    #   (from + to) / 2
    #   или from, если to NULL
    #   или to, если from NULL.
    cur.execute(
        """
        INSERT INTO dm_hh_salary_bands_pg (
            month,
            vacancy_name,
            area_name,
            currency,
            vacancies_cnt,
            salary_min,
            salary_avg,
            salary_max
        )
        SELECT
            date_trunc('month', v.published_at)::date          AS month,
            v.name                                             AS vacancy_name,
            v.area_name                                        AS area_name,
            (v.raw -> 'salary' ->> 'currency')                 AS currency,
            count(*)                                           AS vacancies_cnt,
            MIN(salary_point)                                  AS salary_min,
            AVG(salary_point)                                  AS salary_avg,
            MAX(salary_point)                                  AS salary_max
        FROM ods_hh_vacancies_pg v
        CROSS JOIN LATERAL (
            SELECT
                CASE
                    WHEN (v.raw -> 'salary' ->> 'from') IS NOT NULL
                         AND (v.raw -> 'salary' ->> 'to') IS NOT NULL
                        THEN ((v.raw -> 'salary' ->> 'from')::numeric
                              + (v.raw -> 'salary' ->> 'to')::numeric) / 2
                    WHEN (v.raw -> 'salary' ->> 'from') IS NOT NULL
                        THEN (v.raw -> 'salary' ->> 'from')::numeric
                    WHEN (v.raw -> 'salary' ->> 'to') IS NOT NULL
                        THEN (v.raw -> 'salary' ->> 'to')::numeric
                    ELSE NULL
                END AS salary_point
        ) s
        WHERE v.raw -> 'salary' IS NOT NULL
          AND salary_point IS NOT NULL
        GROUP BY
            date_trunc('month', v.published_at)::date,
            v.name,
            v.area_name,
            (v.raw -> 'salary' ->> 'currency');
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Salary bands mart in Postgres has been rebuilt")


def build_salary_bands_mysql(**context):
    """
    Строит витрину зарплатных вилок на основе ODS-таблицы ods_hh_vacancies_mysql.

    Для простоты агрегируем в Python:
    - читаем нужные поля и salary из JSON,
    - считаем среднюю точку вилки,
    - потом группируем и пишем в MySQL как dm_hh_salary_bands_mysql.
    """
    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor(dictionary=True)

    logging.info("Selecting raw vacancies from MySQL for salary bands")
    cur.execute(
        """
        SELECT
            id,
            name,
            area_name,
            published_at,
            raw
        FROM ods_hh_vacancies_mysql
        WHERE raw IS NOT NULL;
        """
    )
    rows = cur.fetchall()

    import json
    from collections import defaultdict
    from datetime import datetime as dt

    agg = defaultdict(lambda: {"cnt": 0, "sum": 0.0, "min": None, "max": None})

    for r in rows:
        raw = r["raw"]
        if isinstance(raw, str):
            try:
                raw_json = json.loads(raw)
            except json.JSONDecodeError:
                continue
        else:
            raw_json = raw

        salary = raw_json.get("salary") or {}
        s_from = salary.get("from")
        s_to = salary.get("to")
        currency = salary.get("currency")

        if s_from is None and s_to is None:
            continue

        try:
            if s_from is not None and s_to is not None:
                salary_point = (float(s_from) + float(s_to)) / 2.0
            elif s_from is not None:
                salary_point = float(s_from)
            else:
                salary_point = float(s_to)
        except (TypeError, ValueError):
            continue

        published_at = r["published_at"]
        if isinstance(published_at, str):
            try:
                published_dt = dt.fromisoformat(published_at)
            except ValueError:
                try:
                    published_dt = dt.strptime(published_at, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
        else:
            published_dt = published_at

        month = published_dt.date().replace(day=1)
        key = (month, r["name"], r["area_name"], currency)

        bucket = agg[key]
        bucket["cnt"] += 1
        bucket["sum"] += salary_point
        bucket["min"] = salary_point if bucket["min"] is None else min(
            bucket["min"], salary_point
        )
        bucket["max"] = salary_point if bucket["max"] is None else max(
            bucket["max"], salary_point
        )

    logging.info("Aggregated %d (month, vacancy, area, currency) buckets", len(agg))

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dm_hh_salary_bands_mysql (
            month DATE,
            vacancy_name VARCHAR(500),
            area_name VARCHAR(255),
            currency VARCHAR(10),
            vacancies_cnt INT,
            salary_min DOUBLE,
            salary_avg DOUBLE,
            salary_max DOUBLE,
            PRIMARY KEY (month, vacancy_name, area_name, currency)
        );
        """
    )

    logging.info("Truncating dm_hh_salary_bands_mysql before reload")
    cur.execute("TRUNCATE TABLE dm_hh_salary_bands_mysql;")

    insert_sql = """
        INSERT INTO dm_hh_salary_bands_mysql (
            month,
            vacancy_name,
            area_name,
            currency,
            vacancies_cnt,
            salary_min,
            salary_avg,
            salary_max
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    for (month, vacancy_name, area_name, currency), vals in agg.items():
        avg = vals["sum"] / vals["cnt"] if vals["cnt"] > 0 else None
        cur.execute(
            insert_sql,
            (
                month,
                vacancy_name,
                area_name,
                currency,
                vals["cnt"],
                vals["min"],
                avg,
                vals["max"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Salary bands mart in MySQL has been rebuilt")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_salary_bands_to_postgres",
    default_args=default_args,
    description="Витрина зарплатных вилок HH на основе ods_hh_vacancies_pg",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dm", "salary", "postgres"],
) as dag_salary_pg:
    build_pg = PythonOperator(
        task_id="build_salary_bands_postgres",
        python_callable=build_salary_bands_postgres,
    )


with DAG(
    dag_id="hh_salary_bands_to_mysql",
    default_args=default_args,
    description="Витрина зарплатных вилок HH на основе ods_hh_vacancies_mysql",
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "dm", "salary", "mysql"],
) as dag_salary_mysql:
    build_mysql = PythonOperator(
        task_id="build_salary_bands_mysql",
        python_callable=build_salary_bands_mysql,
    )



