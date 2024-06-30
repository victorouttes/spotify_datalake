# Celery configuration
class CeleryConfig(object):
    broker_url = 'redis://redis:6379/0'
    result_backend = 'redis://redis:6379/0'
    worker_log_server = False


CELERY_CONFIG = CeleryConfig

SQLALCHEMY_TRACK_MODIFICATIONS = False

# Superset metadata database configuration
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset:superset@superset-db:5432/superset'

# Additional configurations for Superset
SECRET_KEY = 'ahyeha9182aNah98A'
CSRF_ENABLED = True

# Configuration to support large queries and dashboards
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}
