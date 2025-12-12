import json
import logging
import time
from datetime import datetime, timedelta

import requests

from pymongo import MongoClient

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable


HH_API_URL = "https://api.hh.ru/vacancies"



def _get_hh_headers() -> dict:
    """
    Возвращает заголовки для запросов к HH API,
    используя client_id и client_secret из переменных Airflow.
    Сами значения ключей в коде не хранятся.
    """
    client_id = Variable.get("HH_CLIENT_ID")
    client_secret = Variable.get("HH_CLIENT_SECRET")

    # Для учебной задачи достаточно передать их в заголовках + корректный User-Agent.
    # При необходимости можно доработать до полноценного OAuth-потока.
    return {
        "User-Agent": "airflow-hh-ods/1.0",
        "X-HH-Client-Id": client_id,
        "X-HH-Client-Secret": client_secret,
    }


def extract_vacancies(search_text: str = "data engineer", pages: int = 1, **context):
    """
    Извлекает вакансии с hh.ru по запросу search_text.
    Возвращает список "сырых" вакансий (JSON-объекты).
    """
    all_items = []
    for page in range(pages):
        params = {
            "text": search_text,
            "page": page,
            "per_page": 100,
        }
        resp = requests.get(
            HH_API_URL,
            params=params,
            headers=_get_hh_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        logging.info("Fetched %d items from page %d", len(items), page)
        all_items.extend(items)

    return all_items


def enrich_vacancies_details(
    max_items: int = 200,
    sleep_seconds: float = 0.2,
    max_retries: int = 5,
    **context,
):
    """
    Для каждой вакансии из поиска запрашивает детальную карточку `GET /vacancies/{id}`.

    Это нужно, чтобы получить `specializations` (привязку к сферам/индустриям),
    которой обычно нет в поисковой выдаче.
    """
    ti = context["ti"]
    raw_vacancies = ti.xcom_pull(task_ids="extract_vacancies") or []
    if not raw_vacancies:
        logging.info("No vacancies to enrich")
        return []

    headers = _get_hh_headers()
    enriched = []

    for idx, v in enumerate(raw_vacancies[:max_items], start=1):
        vacancy_id = v.get("id")
        if vacancy_id is None:
            continue

        url = f"{HH_API_URL}/{vacancy_id}"
        last_exc: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 429:
                    backoff = min(10.0, sleep_seconds * attempt * 2)
                    logging.warning(
                        "Rate limited (429) on vacancy_id=%s attempt=%s, sleeping %.2fs",
                        vacancy_id,
                        attempt,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue
                resp.raise_for_status()
                enriched.append(resp.json())
                break
            except Exception as exc:  # noqa: BLE001 - airflow task should not crash on single vacancy
                last_exc = exc
                backoff = min(10.0, sleep_seconds * attempt * 2)
                logging.warning(
                    "Failed to fetch vacancy details vacancy_id=%s attempt=%s/%s: %s. Sleeping %.2fs",
                    vacancy_id,
                    attempt,
                    max_retries,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
        else:
            logging.error(
                "Skip vacancy_id=%s after %s retries. Last error: %s",
                vacancy_id,
                max_retries,
                last_exc,
            )

        if idx % 25 == 0:
            logging.info("Enriched %d/%d vacancies", idx, min(max_items, len(raw_vacancies)))

        time.sleep(sleep_seconds)

    logging.info("Enriched %d vacancies with details", len(enriched))
    return enriched


def transform_vacancies(**context):
    """
    Минимальная нормализация: выделяем несколько полей + сохраняем сырой JSON.
    """
    ti = context["ti"]
    raw_vacancies = (
        ti.xcom_pull(task_ids="enrich_vacancies_details")
        or ti.xcom_pull(task_ids="extract_vacancies")
        or []
    )

    transformed = []
    for v in raw_vacancies:
        transformed.append(
            {
                "id": int(v["id"]),
                "name": v.get("name"),
                "area_name": (v.get("area") or {}).get("name"),
                "employer_name": (v.get("employer") or {}).get("name"),
                "published_at": v.get("published_at"),
                "raw": v,
            }
        )

    logging.info("Transformed %d vacancies", len(transformed))
    return transformed


def load_to_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_vacancies") or []
    if not rows:
        logging.info("No rows to load into Postgres")
        return

    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_vacancies_pg (
            id BIGINT PRIMARY KEY,
            name TEXT,
            area_name TEXT,
            employer_name TEXT,
            published_at TIMESTAMPTZ,
            raw JSONB
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_vacancies_pg (id, name, area_name, employer_name, published_at, raw)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name,
            area_name = EXCLUDED.area_name,
            employer_name = EXCLUDED.employer_name,
            published_at = EXCLUDED.published_at,
            raw = EXCLUDED.raw;
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["id"],
                r["name"],
                r["area_name"],
                r["employer_name"],
                r["published_at"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d vacancies into Postgres", len(rows))


def load_to_mysql(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_vacancies") or []
    if not rows:
        logging.info("No rows to load into MySQL")
        return

    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_hh_vacancies_mysql (
            id BIGINT PRIMARY KEY,
            name VARCHAR(500),
            area_name VARCHAR(255),
            employer_name VARCHAR(255),
            published_at DATETIME,
            raw JSON
        );
        """
    )

    insert_sql = """
        INSERT INTO ods_hh_vacancies_mysql
            (id, name, area_name, employer_name, published_at, raw)
        VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSON))
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            area_name = VALUES(area_name),
            employer_name = VALUES(employer_name),
            published_at = VALUES(published_at),
            raw = VALUES(raw);
    """

    for r in rows:
        # HH API возвращает published_at в ISO 8601, например:
        # 2025-11-11T19:03:31+0300 или 2025-11-11T19:03:31Z.
        # MySQL DATETIME не принимает суффикс часового пояса (+0300/Z),
        # поэтому конвертируем в строку без таймзоны.
        published_at_raw = r.get("published_at")
        if published_at_raw:
            try:
                # Заменяем Z на +00:00, чтобы fromisoformat мог корректно разобрать строку
                dt = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
                published_at_mysql = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                # На всякий случай fallback: просто отрезаем часть с часовым поясом и заменяем T на пробел
                published_at_mysql = (
                    published_at_raw.split("+")[0].split("Z")[0].replace("T", " ")
                )
        else:
            published_at_mysql = None

        cur.execute(
            insert_sql,
            (
                r["id"],
                r["name"],
                r["area_name"],
                r["employer_name"],
                published_at_mysql,
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d vacancies into MySQL", len(rows))


# def load_to_mongo(**context):
#     ti = context["ti"]
#     rows = ti.xcom_pull(task_ids="transform_vacancies") or []
#     if not rows:
#         logging.info("No rows to load into MongoDB")
#         return

#     hook = MongoHook(mongo_conn_id="mongo_ods")
#     db = hook.get_conn()[hook.conn.extra_dejson.get("database", "ods_hh")]
#     collection = db["ods_hh_vacancies"]

#     for r in rows:
#         doc = r["raw"]
#         doc["_id"] = r["id"]
#         collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

#     logging.info("Loaded %d vacancies into MongoDB", len(rows))


def load_to_mongo(**context):
    """
    Загрузка в MongoDB без использования TLS/SSL (локальный учебный стенд).
    Игнорируем ssl/allow_insecure из Extras, подключаемся по обычному mongodb://.
    """
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_vacancies") or []
    if not rows:
        logging.info("No rows to load into MongoDB")
        return

    # Получаем подключение Airflow и extras с именем базы
    conn = BaseHook.get_connection("mongo_ods")
    extras = conn.extra_dejson or {}

    host = conn.host or "mongo_ods"
    port = conn.port or 27017

    # Логин/пароль, если заданы
    auth = ""
    if conn.login:
        auth = conn.login
        if conn.password:
            auth += f":{conn.password}"
        auth += "@"

    # Строим обычный URI без TLS‑параметров
    uri = f"mongodb://{auth}{host}:{port}"

    client = MongoClient(uri)
    db_name = extras.get("database", "ods_hh")
    db = client[db_name]
    collection = db["ods_hh_vacancies"]

    for r in rows:
        doc = r["raw"]
        doc["_id"] = r["id"]
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    logging.info("Loaded %d vacancies into MongoDB", len(rows))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hh_vacancies_to_postgres",
    default_args=default_args,
    description="Загрузка вакансий hh.ru в ODS (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "postgres"],
) as dag_postgres:
    extract_pg = PythonOperator(
        task_id="extract_vacancies",
        python_callable=extract_vacancies,
        op_kwargs={"search_text": "data engineer", "pages": 2},
    )

    enrich_pg = PythonOperator(
        task_id="enrich_vacancies_details",
        python_callable=enrich_vacancies_details,
        op_kwargs={"max_items": 200, "sleep_seconds": 0.2, "max_retries": 5},
    )

    transform_pg = PythonOperator(
        task_id="transform_vacancies",
        python_callable=transform_vacancies,
    )

    load_pg = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    extract_pg >> enrich_pg >> transform_pg >> load_pg


with DAG(
    dag_id="hh_vacancies_to_mysql",
    default_args=default_args,
    description="Загрузка вакансий hh.ru в ODS (MySQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "mysql"],
) as dag_mysql:
    extract_mysql = PythonOperator(
        task_id="extract_vacancies",
        python_callable=extract_vacancies,
        op_kwargs={"search_text": "data engineer", "pages": 2},
    )

    enrich_mysql = PythonOperator(
        task_id="enrich_vacancies_details",
        python_callable=enrich_vacancies_details,
        op_kwargs={"max_items": 200, "sleep_seconds": 0.2, "max_retries": 5},
    )

    transform_mysql = PythonOperator(
        task_id="transform_vacancies",
        python_callable=transform_vacancies,
    )

    load_mysql = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql,
    )

    extract_mysql >> enrich_mysql >> transform_mysql >> load_mysql


with DAG(
    dag_id="hh_vacancies_to_mongo",
    default_args=default_args,
    description="Загрузка вакансий hh.ru в ODS (MongoDB)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hh", "ods", "mongo"],
) as dag_mongo:
    extract_mongo = PythonOperator(
        task_id="extract_vacancies",
        python_callable=extract_vacancies,
        op_kwargs={"search_text": "data engineer", "pages": 2},
    )

    enrich_mongo = PythonOperator(
        task_id="enrich_vacancies_details",
        python_callable=enrich_vacancies_details,
        op_kwargs={"max_items": 200, "sleep_seconds": 0.2, "max_retries": 5},
    )

    transform_mongo = PythonOperator(
        task_id="transform_vacancies",
        python_callable=transform_vacancies,
    )

    load_mongo = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo,
    )

    extract_mongo >> enrich_mongo >> transform_mongo >> load_mongo


