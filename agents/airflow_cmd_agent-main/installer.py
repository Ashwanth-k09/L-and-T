from runner import run_command
from config import AIRFLOW_VERSION, PYTHON_VERSION

def install_missing_apt(packages):
    if not packages:
        return {"returncode": 0, "stdout": "No missing apt packages.", "stderr": "", "cmd": "apt-skip"}
    pkg_str = " ".join(packages)
    return run_command(f"sudo apt update && sudo apt install -y {pkg_str}")

def ensure_postgres():
    cmds = [
        "sudo systemctl enable --now postgresql",
        "sudo -u postgres psql -tc \"SELECT 1 FROM pg_database WHERE datname='airflow'\" | grep -q 1 || sudo -u postgres psql -c \"CREATE DATABASE airflow;\"",
        "sudo -u postgres psql -tc \"SELECT 1 FROM pg_roles WHERE rolname='airflow'\" | grep -q 1 || sudo -u postgres psql -c \"CREATE USER airflow WITH PASSWORD 'airflow';\"",
        "sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;\"",
    ]
    for cmd in cmds:
        res = run_command(cmd)
        if res["returncode"] != 0:
            return res
    return {"returncode": 0, "stdout": "PostgreSQL ready.", "stderr": "", "cmd": "postgres-ready"}

def create_venv():
    cmds = [
        "python3 -m venv ~/airflow_venv",
        "bash -lc 'source ~/airflow_venv/bin/activate && python -m pip install --upgrade pip setuptools wheel'",
    ]
    for cmd in cmds:
        res = run_command(cmd)
        if res["returncode"] != 0:
            return res
    return {"returncode": 0, "stdout": "Virtualenv ready.", "stderr": "", "cmd": "venv-ready"}

def install_airflow():
    cmd = (
        "bash -lc 'source ~/airflow_venv/bin/activate && "
        f"python -m pip install \"apache-airflow[postgres]=={AIRFLOW_VERSION}\" "
        f"--constraint \"https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt\"'"
    )
    return run_command(cmd)
