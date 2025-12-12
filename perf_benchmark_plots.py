"""
Скрипт для построения графиков по результатам бенчмарка ODS-хранилищ.

Использует зашитые в код значения (из лога Airflow), поэтому
графики будут полностью соответствовать таблице в отчёте.

Запуск:
    python perf_benchmark_plots.py

Будут показаны два окна с графиками:
  1) Сгруппированные столбики AVG ms по каждому запросу и СУБД.
  2) Сравнение СУБД по среднему AVG ms (усреднение по всем запросам).
"""

from collections import defaultdict

import matplotlib.pyplot as plt


def get_benchmark_rows():
    """
    Данные взяты из лога бенчмарка (AVG ms).
    Формат строки: (DB, QUERY, AVG_ms).
    """
    return [
        ("PostgreSQL", "vacancies_count", 0.11),
        ("PostgreSQL", "employers_count", 0.05),
        ("PostgreSQL", "salary_industries_count", 0.05),
        ("PostgreSQL", "vacancies_by_area", 0.11),
        ("PostgreSQL", "vacancies_by_employer", 0.12),
        ("MySQL", "vacancies_count", 0.44),
        ("MySQL", "employers_count", 0.19),
        ("MySQL", "salary_industries_count", 0.20),
        ("MySQL", "vacancies_by_area", 0.14),
        ("MySQL", "vacancies_by_employer", 0.17),
        ("MongoDB", "vacancies_count", 1.07),
        ("MongoDB", "employers_count", 0.42),
        ("MongoDB", "salary_industries_count", 0.38),
        ("MongoDB", "vacancies_by_area", 0.35),
        ("MongoDB", "vacancies_by_employer", 0.59),
    ]


def plot_queries_by_db(rows):
    """
    Сгруппированные столбики AVG ms по каждому запросу и СУБД.
    """
    dbs = ["PostgreSQL", "MySQL", "MongoDB"]
    queries = [
        "vacancies_count",
        "employers_count",
        "salary_industries_count",
        "vacancies_by_area",
        "vacancies_by_employer",
    ]

    # data[query][db] = avg_ms
    data = {q: {db: 0.0 for db in dbs} for q in queries}
    for db, q, avg_ms in rows:
        if q in data and db in data[q]:
            data[q][db] = avg_ms

    x = list(range(len(queries)))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 6))

    for i, db in enumerate(dbs):
        offsets = [xi + (i - 1) * width for xi in x]  # центруем группы вокруг x
        values = [data[q][db] for q in queries]
        ax.bar(offsets, values, width, label=db)

    ax.set_xticks(x)
    ax.set_xticklabels(queries, rotation=30, ha="right")
    ax.set_ylabel("Среднее время, ms")
    ax.set_title("Сравнение AVG ms по запросам и СУБД (ODS-слой)")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.show()


def plot_overall_by_db(rows):
    """
    Сравнение СУБД по среднему AVG ms (усреднение по всем запросам).
    """
    avg_by_db = defaultdict(list)
    for db, q, avg_ms in rows:
        avg_by_db[db].append(avg_ms)

    db_names = sorted(avg_by_db.keys())
    db_avgs = [
        sum(avg_by_db[db]) / len(avg_by_db[db]) if avg_by_db[db] else 0.0
        for db in db_names
    ]

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(db_names, db_avgs, color=["tab:blue", "tab:orange", "tab:green"])

    ax.set_ylabel("Средний AVG ms (по всем запросам)")
    ax.set_title("Сравнение СУБД по среднему времени выполнения запросов (ODS-слой)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.show()


def main():
    rows = get_benchmark_rows()
    plot_queries_by_db(rows)
    plot_overall_by_db(rows)


if __name__ == "__main__":
    main()










