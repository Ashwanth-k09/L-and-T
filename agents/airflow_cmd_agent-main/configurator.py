from pathlib import Path
import os
import subprocess
import time

from runner import run_command


def configure_airflow(username: str, password: str):
    cmds = [
        "mkdir -p ~/airflow",

        "sudo -u postgres psql -d airflow -c \"GRANT USAGE, CREATE ON SCHEMA public TO airflow;\"",
        "sudo -u postgres psql -d airflow -c \"ALTER SCHEMA public OWNER TO airflow;\"",
        "sudo -u postgres psql -d airflow -c \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;\"",
        "sudo -u postgres psql -d airflow -c \"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;\"",
        "sudo -u postgres psql -d airflow -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;\"",
        "sudo -u postgres psql -d airflow -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;\"",

        "bash -lc 'export AIRFLOW_HOME=~/airflow && "
        "export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=\"postgresql+psycopg2://airflow:airflow@localhost:5432/airflow\" && "
        "export AIRFLOW__CORE__EXECUTOR=\"LocalExecutor\" && "
        "source ~/airflow_venv/bin/activate && yes y | airflow db migrate'",

        f"bash -lc 'export AIRFLOW_HOME=~/airflow && "
        f"export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=\"postgresql+psycopg2://airflow:airflow@localhost:5432/airflow\" && "
        f"export AIRFLOW__CORE__EXECUTOR=\"LocalExecutor\" && "
        f"source ~/airflow_venv/bin/activate && "
        f"airflow users list | grep -q \"^{username}[[:space:]]\" || "
        f"airflow users create --username \"{username}\" --firstname Admin --lastname User --role Admin --email {username}@example.com --password \"{password}\"'"
    ]

    for cmd in cmds:
        res = run_command(cmd)
        if res["returncode"] != 0:
            return res

    return {
        "returncode": 0,
        "stdout": "Airflow configured with LocalExecutor.",
        "stderr": "",
        "cmd": "configure-ready"
    }


def _start_bg(command: str, logfile: str):
    env = os.environ.copy()
    env["AIRFLOW_HOME"] = str(Path.home() / "airflow")
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
    env["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"

    log_path = Path(logfile).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    log_file = open(log_path, "a")

    subprocess.Popen(
        ["/bin/bash", "-lc", f"source ~/airflow_venv/bin/activate && exec {command}"],
        stdin=subprocess.DEVNULL,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
        close_fds=True,
    )


def start_airflow(port: str):
    try:
        _start_bg(f"airflow webserver --port {port}", "~/airflow/webserver.log")
        _start_bg("airflow scheduler", "~/airflow/scheduler.log")
        _start_bg("airflow triggerer", "~/airflow/triggerer.log")

        time.sleep(8)

        port_check = run_command(f"bash -lc 'ss -ltnp | grep :{port}'")
        proc_check = run_command(
            "bash -lc 'ps aux | grep -E \"airflow webserver|airflow scheduler|airflow triggerer\" | grep -v grep'"
        )

        if port_check["returncode"] != 0:
            return {
                "returncode": 1,
                "stdout": "",
                "stderr": f"Airflow webserver is not listening on port {port}. Check ~/airflow/webserver.log",
                "cmd": "start-check-port"
            }

        if proc_check["returncode"] != 0:
            return {
                "returncode": 1,
                "stdout": "",
                "stderr": "Airflow processes are not running correctly. Check logs in ~/airflow/",
                "cmd": "start-check-process"
            }

        return {
            "returncode": 0,
            "stdout": f"Airflow started on port {port} with LocalExecutor.",
            "stderr": "",
            "cmd": "start-ready"
        }

    except Exception as e:
        return {
            "returncode": 1,
            "stdout": "",
            "stderr": str(e),
            "cmd": "start-exception"
        }
