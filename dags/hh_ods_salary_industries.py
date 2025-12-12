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
from airflow.models import Variable


# Для учебной задачи URL вынесен в константу.
# Фактический справочник отраслей/сфер деятельности берём
# из открытого эндпоинта HH `/industries`.
HH_SALARY_INDUSTRIES_API_URL = "https://api.hh.ru/industries"


def _get_salary_headers() -> dict:
    """
    Заголовки для запросов к справочникам Банка данных заработных плат.

    Основной вариант — использовать OAuth-токен, если он задан
    в переменной Airflow `HH_SALARY_OAUTH_TOKEN`.

    Если переменная отсутствует (учебный стенд), пробуем
    использовать тот же механизм, что и в ODS-вакансиях/работодателях:
    `HH_CLIENT_ID` и `HH_CLIENT_SECRET`.
    """
    headers: dict = {
        # В реальном проекте укажите свой app name и рабочий контакт
        "HH-User-Agent": "airflow-hh-salary-ods/1.0 (your-email@example.com)",
        "User-Agent": "airflow-hh-salary-ods/1.0 (your-email@example.com)",
    }

    oauth_token = Variable.get("HH_SALARY_OAUTH_TOKEN", default_var=None)
    if oauth_token:
        headers["Authorization"] = f"Bearer {oauth_token}"
        return headers

    # Фоллбэк для учебного стенда — те же ключи, что в других ODS-ДАГах
    client_id = Variable.get("HH_CLIENT_ID", default_var=None)
    client_secret = Variable.get("HH_CLIENT_SECRET", default_var=None)
    if client_id and client_secret:
        headers["X-HH-Client-Id"] = client_id
        headers["X-HH-Client-Secret"] = client_secret
    else:
        logging.warning(
            "Neither HH_SALARY_OAUTH_TOKEN nor HH_CLIENT_ID/HH_CLIENT_SECRET are set. "
            "Requests to salary dictionaries API may fail with 401/403."
        )

    return headers


def extract_salary_industries(host: str = "hh.ru", locale: str = "RU", **context):
    """
    Извлекает двухуровневый список отраслей и сфер деятельности
    из справочников Банка данных заработных плат HH.
    """
    params = {
        "host": host,
        "locale": locale,
    }

    logging.info(
        "Requesting salary industries dictionary from %s with locale=%s",
        host,
        locale,
    )
    resp = requests.get(
        HH_SALARY_INDUSTRIES_API_URL,
        params=params,
        headers=_get_salary_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # Ожидаем список отраслей, каждая содержит список сфер/подотраслей
    if not isinstance(data, list):
        logging.warning(
            "Unexpected response type for salary industries: %s", type(data)
        )

    return data


def transform_salary_industries(**context):
    """
    Превращает двухуровневый список отраслей/сфер деятельности
    в плоский список записей для загрузки в ODS.

    На выходе каждая строка соответствует паре (отрасль, сфера деятельности).
    """
    ti = context["ti"]
    raw_items = ti.xcom_pull(task_ids="extract_salary_industries") or []

    transformed = []
    for industry in raw_items:
        industry_id = str(industry.get("id"))
        industry_name = industry.get("name")
        spheres = (
            industry.get("industries")
            or industry.get("spheres")
            or industry.get("areas")
            or []
        )

        # Если у отрасли нет подчинённых сфер, всё равно сохраняем запись,
        # но с NULL в полях сферы.
        if not spheres:
            transformed.append(
                {
                    "industry_id": industry_id,
                    "industry_name": industry_name,
                    "sphere_id": None,
                    "sphere_name": None,
                    "raw": industry,
                }
            )
            continue

        for sphere in spheres:
            sphere_id = str(sphere.get("id")) if sphere.get("id") is not None else None
            sphere_name = sphere.get("name")

            transformed.append(
                {
                    "industry_id": industry_id,
                    "industry_name": industry_name,
                    "sphere_id": sphere_id,
                    "sphere_name": sphere_name,
                    "raw": sphere,
                }
            )

    logging.info("Transformed %d salary industry/sphere rows", len(transformed))
    return transformed


def load_salary_industries_to_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_salary_industries") or []
    if not rows:
        logging.info("No rows to load into Postgres (salary industries)")
        return

    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_salary_industries_pg (
            industry_id   TEXT,
            industry_name TEXT,
            sphere_id     TEXT,
            sphere_name   TEXT,
            raw           JSONB,
            PRIMARY KEY (industry_id, sphere_id)
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_salary_industries_pg (
            industry_id,
            industry_name,
            sphere_id,
            sphere_name,
            raw
        )
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (industry_id, sphere_id) DO UPDATE
        SET industry_name = EXCLUDED.industry_name,
            sphere_name   = EXCLUDED.sphere_name,
            raw           = EXCLUDED.raw;
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["industry_id"],
                r["industry_name"],
                r["sphere_id"],
                r["sphere_name"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d salary industry/sphere rows into Postgres", len(rows))


def load_salary_industries_to_mysql(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_salary_industries") or []
    if not rows:
        logging.info("No rows to load into MySQL (salary industries)")
        return

    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_salary_industries_mysql (
            industry_id   VARCHAR(64),
            industry_name VARCHAR(500),
            sphere_id     VARCHAR(64),
            sphere_name   VARCHAR(500),
            raw           JSON,
            PRIMARY KEY (industry_id, sphere_id)
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_salary_industries_mysql (
            industry_id,
            industry_name,
            sphere_id,
            sphere_name,
            raw
        )
        VALUES (%s, %s, %s, %s, CAST(%s AS JSON))
        ON DUPLICATE KEY UPDATE
            industry_name = VALUES(industry_name),
            sphere_name   = VALUES(sphere_name),
            raw           = VALUES(raw);
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["industry_id"],
                r["industry_name"],
                r["sphere_id"],
                r["sphere_name"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d salary industry/sphere rows into MySQL", len(rows))


def load_salary_industries_to_mongo(**context):
    """
    Загрузка отраслей/сфер деятельности в MongoDB без TLS
    (локальный учебный стенд, по аналогии с остальными ODS-дагами).
    """
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_salary_industries") or []
    if not rows:
        logging.info("No rows to load into MongoDB (salary industries)")
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
    collection = db["ods_hh_salary_industries"]

    for r in rows:
        # Формируем составной _id, чтобы не было дублей по отрасли/сфере
        ind_id = r["industry_id"]
        sph_id = r["sphere_id"] or ""
        doc = r["raw"]
        doc["_id"] = f"{ind_id}:{sph_id}"
        doc["industry_id"] = ind_id
        doc["sphere_id"] = sph_id or None

        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    logging.info("Loaded %d salary industry/sphere docs into MongoDB", len(rows))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_salary_industries_to_postgres",
    default_args=default_args,
    description=(
        "Загрузка справочника отраслей и сфер деятельности Банка данных "
        "заработных плат HH в ODS (PostgreSQL)"
    ),
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "salary", "industries", "postgres"],
) as dag_salary_industries_pg:
    extract_pg = PythonOperator(
        task_id="extract_salary_industries",
        python_callable=extract_salary_industries,
        op_kwargs={"host": "hh.ru", "locale": "RU"},
    )

    transform_pg = PythonOperator(
        task_id="transform_salary_industries",
        python_callable=transform_salary_industries,
    )

    load_pg = PythonOperator(
        task_id="load_salary_industries_to_postgres",
        python_callable=load_salary_industries_to_postgres,
    )

    extract_pg >> transform_pg >> load_pg


with DAG(
    dag_id="hh_salary_industries_to_mysql",
    default_args=default_args,
    description=(
        "Загрузка справочника отраслей и сфер деятельности Банка данных "
        "заработных плат HH в ODS (MySQL)"
    ),
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "salary", "industries", "mysql"],
) as dag_salary_industries_mysql:
    extract_mysql = PythonOperator(
        task_id="extract_salary_industries",
        python_callable=extract_salary_industries,
        op_kwargs={"host": "hh.ru", "locale": "RU"},
    )

    transform_mysql = PythonOperator(
        task_id="transform_salary_industries",
        python_callable=transform_salary_industries,
    )

    load_mysql = PythonOperator(
        task_id="load_salary_industries_to_mysql",
        python_callable=load_salary_industries_to_mysql,
    )

    extract_mysql >> transform_mysql >> load_mysql


with DAG(
    dag_id="hh_salary_industries_to_mongo",
    default_args=default_args,
    description=(
        "Загрузка справочника отраслей и сфер деятельности Банка данных "
        "заработных плат HH в ODS (MongoDB)"
    ),
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "salary", "industries", "mongo"],
) as dag_salary_industries_mongo:
    extract_mongo = PythonOperator(
        task_id="extract_salary_industries",
        python_callable=extract_salary_industries,
        op_kwargs={"host": "hh.ru", "locale": "RU"},
    )

    transform_mongo = PythonOperator(
        task_id="transform_salary_industries",
        python_callable=transform_salary_industries,
    )

    load_mongo = PythonOperator(
        task_id="load_salary_industries_to_mongo",
        python_callable=load_salary_industries_to_mongo,
    )

    extract_mongo >> transform_mongo >> load_mongo


