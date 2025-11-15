import time
import statistics as stats
import logging
from dataclasses import dataclass
from typing import Callable, Dict, List

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pymongo import MongoClient


@dataclass
class QueryBenchmarkResult:
    db: str
    query_name: str
    runs: int
    durations: List[float]

    @property
    def min_ms(self) -> float:
        return min(self.durations) * 1000 if self.durations else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.durations) * 1000 if self.durations else 0.0

    @property
    def avg_ms(self) -> float:
        return stats.mean(self.durations) * 1000 if self.durations else 0.0

    @property
    def std_ms(self) -> float:
        return stats.pstdev(self.durations) * 1000 if len(self.durations) > 1 else 0.0


def time_callable(fn: Callable[[], None], runs: int = 5) -> List[float]:
    durations: List[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        end = time.perf_counter()
        durations.append(end - start)
    return durations


def benchmark_postgres(runs: int = 5) -> List[QueryBenchmarkResult]:
    """
    Бенчмарк PostgreSQL на основе Airflow Connection postgres_ods.
    """
    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Анализируем только ODS-слой, витрина dm_hh_salary_bands_* в анализ не входит.
    queries: Dict[str, str] = {
        "vacancies_count": "SELECT COUNT(*) FROM ods_hh_vacancies_pg;",
        "employers_count": "SELECT COUNT(*) FROM ods_hh_employers_pg;",
        "salary_industries_count": "SELECT COUNT(*) FROM ods_hh_salary_industries_pg;",
        "vacancies_by_area": """
            SELECT area_name, COUNT(*)
            FROM ods_hh_vacancies_pg
            GROUP BY area_name;
        """,
        "vacancies_by_employer": """
            SELECT employer_name, COUNT(*)
            FROM ods_hh_vacancies_pg
            GROUP BY employer_name;
        """,
    }

    results: List[QueryBenchmarkResult] = []

    for name, sql in queries.items():
        def run_query() -> None:
            cur.execute(sql)
            _ = cur.fetchall()

        durations = time_callable(run_query, runs=runs)
        results.append(
            QueryBenchmarkResult(
                db="PostgreSQL",
                query_name=name,
                runs=runs,
                durations=durations,
            )
        )

    cur.close()
    conn.close()
    return results


def benchmark_mysql(runs: int = 5) -> List[QueryBenchmarkResult]:
    """
    Бенчмарк MySQL на основе Airflow Connection mysql_ods.
    """
    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Анализируем только ODS-слой, витрина dm_hh_salary_bands_* в анализ не входит.
    queries: Dict[str, str] = {
        "vacancies_count": "SELECT COUNT(*) FROM ods_hh_vacancies_mysql;",
        "employers_count": "SELECT COUNT(*) FROM ods_hh_employers_mysql;",
        "salary_industries_count": "SELECT COUNT(*) FROM ods_hh_salary_industries_mysql;",
        "vacancies_by_area": """
            SELECT area_name, COUNT(*)
            FROM ods_hh_vacancies_mysql
            GROUP BY area_name;
        """,
        "vacancies_by_employer": """
            SELECT employer_name, COUNT(*)
            FROM ods_hh_vacancies_mysql
            GROUP BY employer_name;
        """,
    }

    results: List[QueryBenchmarkResult] = []

    for name, sql in queries.items():
        def run_query() -> None:
            cur.execute(sql)
            _ = cur.fetchall()

        try:
            durations = time_callable(run_query, runs=runs)
        except Exception as exc:
            # Например, витрина dm_hh_salary_bands_mysql может ещё не быть построена.
            logging.warning(
                "Skipping MySQL benchmark query '%s' due to error: %s", name, exc
            )
            continue

        results.append(
            QueryBenchmarkResult(
                db="MySQL",
                query_name=name,
                runs=runs,
                durations=durations,
            )
        )

    cur.close()
    conn.close()
    return results


def benchmark_mongo(runs: int = 5) -> List[QueryBenchmarkResult]:
    """
    Бенчмарк MongoDB на основе Airflow Connection mongo_ods.
    """
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

    vacancies = db["ods_hh_vacancies"]
    employers = db["ods_hh_employers"]
    salary_industries = db["ods_hh_salary_industries"]

    def run_many(fn: Callable[[], None]) -> List[float]:
        return time_callable(fn, runs=runs)

    results: List[QueryBenchmarkResult] = []

    results.append(
        QueryBenchmarkResult(
            db="MongoDB",
            query_name="vacancies_count",
            runs=runs,
            durations=run_many(lambda: vacancies.count_documents({})),
        )
    )
    results.append(
        QueryBenchmarkResult(
            db="MongoDB",
            query_name="employers_count",
            runs=runs,
            durations=run_many(lambda: employers.count_documents({})),
        )
    )
    results.append(
        QueryBenchmarkResult(
            db="MongoDB",
            query_name="salary_industries_count",
            runs=runs,
            durations=run_many(lambda: salary_industries.count_documents({})),
        )
    )

    results.append(
        QueryBenchmarkResult(
            db="MongoDB",
            query_name="vacancies_by_area",
            runs=runs,
            durations=run_many(
                lambda: list(
                    vacancies.aggregate(
                        [
                            {"$group": {"_id": "$area.name", "cnt": {"$sum": 1}}},
                        ]
                    )
                )
            ),
        )
    )
    results.append(
        QueryBenchmarkResult(
            db="MongoDB",
            query_name="vacancies_by_employer",
            runs=runs,
            durations=run_many(
                lambda: list(
                    vacancies.aggregate(
                        [
                            {"$group": {"_id": "$employer.name", "cnt": {"$sum": 1}}},
                        ]
                    )
                )
            ),
        )
    )

    client.close()
    return results


def print_results(results: List[QueryBenchmarkResult]) -> None:
    header = f"{'DB':<10} {'QUERY':<25} {'RUNS':<4} {'MIN ms':>10} {'AVG ms':>10} {'MAX ms':>10} {'STD ms':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.db:<10} {r.query_name:<25} {r.runs:<4} "
            f"{r.min_ms:>10.2f} {r.avg_ms:>10.2f} {r.max_ms:>10.2f} {r.std_ms:>10.2f}"
        )


def run_all_benchmarks(runs: int = 5) -> List[QueryBenchmarkResult]:
    all_results: List[QueryBenchmarkResult] = []
    all_results.extend(benchmark_postgres(runs=runs))
    all_results.extend(benchmark_mysql(runs=runs))
    all_results.extend(benchmark_mongo(runs=runs))
    return all_results


