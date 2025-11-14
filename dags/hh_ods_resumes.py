from datetime import datetime, timedelta
import json
import logging

import requests

from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable


HH_RESUMES_API_URL = "https://api.hh.ru/resumes"


def _get_hh_headers() -> dict:
    """
    Возвращает заголовки для запросов к HH API,
    используя client_id и client_secret из переменных Airflow.
    """
    client_id = Variable.get("HH_CLIENT_ID")
    client_secret = Variable.get("HH_CLIENT_SECRET")

    return {
        "User-Agent": "airflow-hh-ods-resumes/1.0",
        "X-HH-Client-Id": client_id,
        "X-HH-Client-Secret": client_secret,
    }


def extract_resumes(pages: int = 1, **context):
    """
    Извлекает резюме с hh.ru.
    Для простоты берём первые N страниц общего списка резюме.
    """
    all_items = []
    for page in range(pages):
        params = {
            "page": page,
            "per_page": 100,
        }
        resp = requests.get(
            HH_RESUMES_API_URL,
            params=params,
            headers=_get_hh_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        logging.info("Fetched %d resumes from page %d", len(items), page)
        all_items.extend(items)

    return all_items


def transform_resumes(**context):
    """
    Минимальная нормализация полей резюме.
    Структура объекта резюме может отличаться, поэтому аккуратно берём поля через get().
    """
    ti = context["ti"]
    raw_resumes = ti.xcom_pull(task_ids="extract_resumes") or []

    transformed = []
    for r in raw_resumes:
        area = r.get("area") or {}
        gender = r.get("gender")
        # gender на практике может быть как строкой, так и объектом; аккуратно вытаскиваем name, если это dict
        if isinstance(gender, dict):
            gender_value = gender.get("name")
        else:
            gender_value = gender

        transformed.append(
            {
                "id": str(r.get("id")),
                "title": r.get("title"),
                "area_name": area.get("name"),
                "age": r.get("age"),
                "gender": gender_value,
                "updated_at": r.get("updated_at") or r.get("created_at"),
                "raw": r,
            }
        )

    logging.info("Transformed %d resumes", len(transformed))
    return transformed


def load_resumes_to_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_resumes") or []
    if not rows:
        logging.info("No rows to load into Postgres (resumes)")
        return

    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_resumes_pg (
            id TEXT PRIMARY KEY,
            title TEXT,
            area_name TEXT,
            age INT,
            gender TEXT,
            updated_at TIMESTAMPTZ,
            raw JSONB
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_resumes_pg (id, title, area_name, age, gender, updated_at, raw)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET title = EXCLUDED.title,
            area_name = EXCLUDED.area_name,
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            updated_at = EXCLUDED.updated_at,
            raw = EXCLUDED.raw;
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["id"],
                r["title"],
                r["area_name"],
                r["age"],
                r["gender"],
                r["updated_at"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d resumes into Postgres", len(rows))


def load_resumes_to_mysql(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_resumes") or []
    if not rows:
        logging.info("No rows to load into MySQL (resumes)")
        return

    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_resumes_mysql (
            id VARCHAR(64) PRIMARY KEY,
            title VARCHAR(500),
            area_name VARCHAR(255),
            age INT,
            gender VARCHAR(50),
            updated_at DATETIME,
            raw JSON
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_resumes_mysql
            (id, title, area_name, age, gender, updated_at, raw)
        VALUES (%s, %s, %s, %s, %s, %s, CAST(%s AS JSON))
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            area_name = VALUES(area_name),
            age = VALUES(age),
            gender = VALUES(gender),
            updated_at = VALUES(updated_at),
            raw = VALUES(raw);
    """

    for r in rows:
        updated_at_raw = r.get("updated_at")
        if updated_at_raw:
            try:
                # Аналогичная логика, как в загрузке вакансий:
                # приводим ISO 8601 к строке без таймзоны для MySQL DATETIME
                dt = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
                updated_at_mysql = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                updated_at_mysql = (
                    updated_at_raw.split("+")[0].split("Z")[0].replace("T", " ")
                )
        else:
            updated_at_mysql = None

        cur.execute(
            insert_sql,
            (
                r["id"],
                r["title"],
                r["area_name"],
                r["age"],
                r["gender"],
                updated_at_mysql,
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d resumes into MySQL", len(rows))


def load_resumes_to_mongo(**context):
    """
    Загрузка резюме в MongoDB без TLS (локальный учебный стенд),
    по аналогии с загрузкой вакансий.
    """
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_resumes") or []
    if not rows:
        logging.info("No rows to load into MongoDB (resumes)")
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
    collection = db["ods_hh_resumes"]

    for r in rows:
        doc = r["raw"]
        doc["_id"] = r["id"]
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    logging.info("Loaded %d resumes into MongoDB", len(rows))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_resumes_to_postgres",
    default_args=default_args,
    description="Загрузка резюме hh.ru в ODS (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "resumes", "postgres"],
) as dag_resumes_postgres:
    extract_pg = PythonOperator(
        task_id="extract_resumes",
        python_callable=extract_resumes,
        op_kwargs={"pages": 2},
    )

    transform_pg = PythonOperator(
        task_id="transform_resumes",
        python_callable=transform_resumes,
    )

    load_pg = PythonOperator(
        task_id="load_resumes_to_postgres",
        python_callable=load_resumes_to_postgres,
    )

    extract_pg >> transform_pg >> load_pg


with DAG(
    dag_id="hh_resumes_to_mysql",
    default_args=default_args,
    description="Загрузка резюме hh.ru в ODS (MySQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "resumes", "mysql"],
) as dag_resumes_mysql:
    extract_mysql = PythonOperator(
        task_id="extract_resumes",
        python_callable=extract_resumes,
        op_kwargs={"pages": 2},
    )

    transform_mysql = PythonOperator(
        task_id="transform_resumes",
        python_callable=transform_resumes,
    )

    load_mysql = PythonOperator(
        task_id="load_resumes_to_mysql",
        python_callable=load_resumes_to_mysql,
    )

    extract_mysql >> transform_mysql >> load_mysql


with DAG(
    dag_id="hh_resumes_to_mongo",
    default_args=default_args,
    description="Загрузка резюме hh.ru в ODS (MongoDB)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "resumes", "mongo"],
) as dag_resumes_mongo:
    extract_mongo = PythonOperator(
        task_id="extract_resumes",
        python_callable=extract_resumes,
        op_kwargs={"pages": 2},
    )

    transform_mongo = PythonOperator(
        task_id="transform_resumes",
        python_callable=transform_resumes,
    )

    load_mongo = PythonOperator(
        task_id="load_resumes_to_mongo",
        python_callable=load_resumes_to_mongo,
    )

    extract_mongo >> transform_mongo >> load_mongo




