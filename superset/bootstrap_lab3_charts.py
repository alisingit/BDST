import json
import os
from pathlib import Path

CHARTS_DIR = Path("/app/pythonpath/charts")


def _get_or_create_database(db, Database, database_name: str, sqlalchemy_uri: str):
    existing = db.session.query(Database).filter(Database.database_name == database_name).one_or_none()
    if existing:
        existing.sqlalchemy_uri = sqlalchemy_uri
        db.session.commit()
        return existing

    database = Database(database_name=database_name, sqlalchemy_uri=sqlalchemy_uri)
    db.session.add(database)
    db.session.commit()
    return database


def _get_or_create_dataset(db, SqlaTable, database, schema: str, table_name: str):
    existing = (
        db.session.query(SqlaTable)
        .filter(SqlaTable.database_id == database.id)
        .filter(SqlaTable.schema == schema)
        .filter(SqlaTable.table_name == table_name)
        .one_or_none()
    )
    if existing:
        return existing

    ds = SqlaTable(database=database, schema=schema, table_name=table_name)
    db.session.add(ds)
    db.session.commit()

    # fetch columns/metadata
    ds.fetch_metadata()
    db.session.commit()
    return ds


def _upsert_chart(db, SqlaTable, Slice, database, chart_json_path: Path):
    payload = json.loads(chart_json_path.read_text(encoding="utf-8"))
    slice_name = payload["slice_name"]
    viz_type = payload["viz_type"]

    datasource = payload["datasource"]
    schema, table_name = datasource.split(".", 1)
    dataset = _get_or_create_dataset(db, SqlaTable, database, schema=schema, table_name=table_name)

    existing = (
        db.session.query(Slice)
        .filter(Slice.slice_name == slice_name)
        .filter(Slice.datasource_id == dataset.id)
        .filter(Slice.datasource_type == "table")
        .one_or_none()
    )

    params = json.dumps(payload["form_data"], ensure_ascii=False)

    if existing:
        existing.viz_type = viz_type
        existing.params = params
        db.session.commit()
        return existing

    slc = Slice(
        slice_name=slice_name,
        viz_type=viz_type,
        params=params,
        datasource_id=dataset.id,
        datasource_type="table",
    )
    db.session.add(slc)
    db.session.commit()
    return slc


def main() -> None:
    # IMPORTANT: все импорты Superset, требующие app context, делаем внутри main().
    from superset import db  # pylint: disable=import-outside-toplevel
    from superset.connectors.sqla.models import SqlaTable  # pylint: disable=import-outside-toplevel
    from superset.models.core import Database  # pylint: disable=import-outside-toplevel
    from superset.models.slice import Slice  # pylint: disable=import-outside-toplevel

    database_name = os.environ.get("LAB3_DATABASE_NAME", "PostgreSQL")
    sqlalchemy_uri = os.environ.get(
        "LAB3_DM_SQLALCHEMY_URI",
        "postgresql+psycopg2://airflow:airflow@postgres_dm:5432/dm_hh",
    )

    if not CHARTS_DIR.exists():
        raise RuntimeError(f"Charts directory not found: {CHARTS_DIR}")

    database = _get_or_create_database(db, Database, database_name=database_name, sqlalchemy_uri=sqlalchemy_uri)

    for chart_path in sorted(CHARTS_DIR.glob("lab3_*.json")):
        _upsert_chart(db, SqlaTable, Slice, database, chart_path)


if __name__ == "__main__":
    from superset.app import create_app  # pylint: disable=import-outside-toplevel

    app = create_app()
    with app.app_context():
        main()


