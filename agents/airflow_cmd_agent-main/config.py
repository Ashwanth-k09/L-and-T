AIRFLOW_VERSION = "2.10.5"
PYTHON_VERSION = "3.12"

AIRFLOW_HOME = "~/airflow"
VENV_PATH = "~/airflow_venv"

DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"

DEFAULT_PORT = "8080"
MAX_RETRIES = 5

REQUIRED_APT_PACKAGES = [
    "python3-pip",
    "python3-venv",
    "build-essential",
    "curl",
    "libssl-dev",
    "libffi-dev",
    "libpq-dev",
    "postgresql",
    "postgresql-contrib",
    "libsasl2-dev",
    "libsasl2-modules",
    "libxmlsec1",
    "libxmlsec1-dev",
    "pkg-config",
    "unixodbc",
    "unixodbc-dev",
    "freetds-bin",
    "freetds-dev",
    "ldap-utils",
    "libldap2-dev",
    "sqlite3",
]
