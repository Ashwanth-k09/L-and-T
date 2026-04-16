from runner import run_command


def verify_airflow(port: str):
    commands = [
        "bash -lc 'source ~/airflow_venv/bin/activate && airflow version'",
        "bash -lc 'export AIRFLOW_HOME=~/airflow && export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=\"postgresql+psycopg2://airflow:airflow@localhost:5432/airflow\" && source ~/airflow_venv/bin/activate && airflow users list'",
        f"bash -lc 'ss -ltnp | grep :{port}'",
        "bash -lc 'ps aux | grep -E \"airflow webserver|airflow scheduler|airflow triggerer\" | grep -v grep'"
    ]

    results = []
    overall_ok = True

    for cmd in commands:
        result = run_command(cmd)
        results.append(result)
        if result["returncode"] != 0:
            overall_ok = False

    return overall_ok, results
