from datetime import datetime, timedelta
import json
import logging

import requests

from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable


HH_EMPLOYERS_API_URL = "https://api.hh.ru/employers"


def same_execution_date(execution_date):
    return execution_date


def _get_hh_headers() -> dict:
    """
    Возвращает заголовки для запросов к HH API,
    используя client_id и client_secret из переменных Airflow.
    """
    client_id = Variable.get("HH_CLIENT_ID")
    client_secret = Variable.get("HH_CLIENT_SECRET")

    return {
        "User-Agent": "airflow-hh-ods-employers/1.0",
        "X-HH-Client-Id": client_id,
        "X-HH-Client-Secret": client_secret,
    }


def extract_employers(pages: int = 1, **context):
    """
    Извлекает работодателей с hh.ru.
    Для простоты берём первые N страниц общего списка работодателей.
    """
    all_items = []
    for page in range(pages):
        params = {
            "page": page,
            "per_page": 100,
        }
        resp = requests.get(
            HH_EMPLOYERS_API_URL,
            params=params,
            headers=_get_hh_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        logging.info("Fetched %d employers from page %d", len(items), page)
        all_items.extend(items)

    return all_items


def transform_employers(**context):
    """
    Минимальная нормализация полей работодателей.
    """
    ti = context["ti"]
    raw_employers = ti.xcom_pull(task_ids="extract_employers") or []

    transformed = []
    for e in raw_employers:
        area = e.get("area") or {}
        transformed.append(
            {
                "id": int(e["id"]),
                "name": e.get("name"),
                "area_name": area.get("name"),
                "open_vacancies": e.get("open_vacancies"),
                "site_url": e.get("site_url"),
                "raw": e,
            }
        )

    logging.info("Transformed %d employers", len(transformed))
    return transformed


def load_employers_to_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_employers") or []
    if not rows:
        logging.info("No rows to load into Postgres (employers)")
        return

    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_employers_pg (
            id BIGINT PRIMARY KEY,
            name TEXT,
            area_name TEXT,
            open_vacancies INT,
            site_url TEXT,
            raw JSONB
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_employers_pg (id, name, area_name, open_vacancies, site_url, raw)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name,
            area_name = EXCLUDED.area_name,
            open_vacancies = EXCLUDED.open_vacancies,
            site_url = EXCLUDED.site_url,
            raw = EXCLUDED.raw;
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["id"],
                r["name"],
                r["area_name"],
                r["open_vacancies"],
                r["site_url"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d employers into Postgres", len(rows))


def load_employers_to_mysql(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_employers") or []
    if not rows:
        logging.info("No rows to load into MySQL (employers)")
        return

    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_employers_mysql (
            id BIGINT PRIMARY KEY,
            name VARCHAR(500),
            area_name VARCHAR(255),
            open_vacancies INT,
            site_url VARCHAR(500),
            raw JSON
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_employers_mysql
            (id, name, area_name, open_vacancies, site_url, raw)
        VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSON))
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            area_name = VALUES(area_name),
            open_vacancies = VALUES(open_vacancies),
            site_url = VALUES(site_url),
            raw = VALUES(raw);
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["id"],
                r["name"],
                r["area_name"],
                r["open_vacancies"],
                r["site_url"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d employers into MySQL", len(rows))


def load_employers_to_mongo(**context):
    """
    Загрузка работодателей в MongoDB без TLS (локальный учебный стенд),
    по аналогии с загрузкой вакансий.
    """
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_employers") or []
    if not rows:
        logging.info("No rows to load into MongoDB (employers)")
        return

    conn = BaseHook.get_connection("mongo_ods")
    extras = conn.extra_dejson or {}

    host = conn.host or "mongo_ods"
    port = conn.port or 27017

    auth = ""
    if conn.login:
        auth = conn.login
        if conn.password:
            auth += f":{conn.password}"
        auth += "@"

    uri = f"mongodb://{auth}{host}:{port}"

    client = MongoClient(uri)
    db_name = extras.get("database", "ods_hh")
    db = client[db_name]
    collection = db["ods_hh_employers"]

    for r in rows:
        doc = r["raw"]
        doc["_id"] = r["id"]
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    logging.info("Loaded %d employers into MongoDB", len(rows))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_employers_to_postgres",
    default_args=default_args,
    description="Загрузка работодателей hh.ru в ODS (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "employers", "postgres"],
) as dag_employers_postgres:
    wait_vacancies = ExternalTaskSensor(
        task_id="wait_for_vacancies_postgres",
        external_dag_id="hh_vacancies_to_postgres",
        external_task_id="load_to_postgres",
        execution_date_fn=same_execution_date,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    extract_pg = PythonOperator(
        task_id="extract_employers",
        python_callable=extract_employers,
        op_kwargs={"pages": 2},
    )

    transform_pg = PythonOperator(
        task_id="transform_employers",
        python_callable=transform_employers,
    )

    load_pg = PythonOperator(
        task_id="load_employers_to_postgres",
        python_callable=load_employers_to_postgres,
    )

    wait_vacancies >> extract_pg >> transform_pg >> load_pg


with DAG(
    dag_id="hh_employers_to_mysql",
    default_args=default_args,
    description="Загрузка работодателей hh.ru в ODS (MySQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "employers", "mysql"],
) as dag_employers_mysql:
    extract_mysql = PythonOperator(
        task_id="extract_employers",
        python_callable=extract_employers,
        op_kwargs={"pages": 2},
    )

    transform_mysql = PythonOperator(
        task_id="transform_employers",
        python_callable=transform_employers,
    )

    load_mysql = PythonOperator(
        task_id="load_employers_to_mysql",
        python_callable=load_employers_to_mysql,
    )

    extract_mysql >> transform_mysql >> load_mysql


with DAG(
    dag_id="hh_employers_to_mongo",
    default_args=default_args,
    description="Загрузка работодателей hh.ru в ODS (MongoDB)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "employers", "mongo"],
) as dag_employers_mongo:
    extract_mongo = PythonOperator(
        task_id="extract_employers",
        python_callable=extract_employers,
        op_kwargs={"pages": 2},
    )

    transform_mongo = PythonOperator(
        task_id="transform_employers",
        python_callable=transform_employers,
    )

    load_mongo = PythonOperator(
        task_id="load_employers_to_mongo",
        python_callable=load_employers_to_mongo,
    )

    extract_mongo >> transform_mongo >> load_mongo




