import os


# Superset читает параметры из python-конфига. В docker-compose мы прокидываем
# SUPERSET_METADATA_DB_URI и SUPERSET_SECRET_KEY, чтобы не хардкодить в файле.
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_METADATA_DB_URI")

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me_in_prod")

# Отключаем примеры для “чистого” стенда
SUPERSET_LOAD_EXAMPLES = False


