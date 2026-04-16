import json
from pathlib import Path

from runner import run_command
from config import AIRFLOW_VERSION, PYTHON_VERSION

RULE_FILE = Path.home() / "airflow_cmd_agent" / "rules" / "error_rules.json"

with open(RULE_FILE, "r") as f:
    RULES = json.load(f)


def detect_fix(error_text: str):
    for pattern, data in RULES.items():
        if pattern.lower() in error_text.lower():
            return data["fix"], data["message"]
    return None, None


def apply_fix(fix_name: str):
    if fix_name == "use_venv":
        return run_command("python3 -m venv ~/airflow_venv")

    if fix_name == "install_libpq_dev":
        return run_command("sudo apt update && sudo apt install -y libpq-dev")

    if fix_name == "install_psycopg2":
        return run_command(
            "bash -lc 'source ~/airflow_venv/bin/activate && python -m pip install psycopg2-binary'"
        )

    if fix_name == "reinstall_airflow":
        return run_command(
            "bash -lc 'source ~/airflow_venv/bin/activate && "
            f"python -m pip install \"apache-airflow[postgres]=={AIRFLOW_VERSION}\" "
            f"--constraint \"https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt\"'"
        )

    if fix_name == "start_postgres":
        return run_command("sudo systemctl start postgresql")

    if fix_name == "change_port":
        return {
            "returncode": 0,
            "stdout": "Selected port is busy. Re-run agent with another port.",
            "stderr": "",
            "cmd": "change-port",
        }

    if fix_name == "fix_public_schema_privileges":
        return run_command(
            "sudo -u postgres psql -d airflow -c \"GRANT USAGE, CREATE ON SCHEMA public TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"ALTER SCHEMA public OWNER TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;\""
        )

    if fix_name == "reset_airflow_db":
        return run_command(
            "sudo -u postgres psql -c \"DROP DATABASE IF EXISTS airflow;\" && "
            "sudo -u postgres psql -c \"DROP USER IF EXISTS airflow;\" && "
            "sudo -u postgres psql -c \"CREATE USER airflow WITH PASSWORD 'airflow';\" && "
            "sudo -u postgres psql -c \"CREATE DATABASE airflow OWNER airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"GRANT USAGE, CREATE ON SCHEMA public TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"ALTER SCHEMA public OWNER TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;\" && "
            "sudo -u postgres psql -d airflow -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;\""
        )

    return {
        "returncode": 1,
        "stdout": "",
        "stderr": f"Unknown fix: {fix_name}",
        "cmd": "unknown-fix",
    }
