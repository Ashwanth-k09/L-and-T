from runner import run_command
from config import REQUIRED_APT_PACKAGES

def check_airflow_installed():
    result = run_command("bash -lc 'source ~/airflow_venv/bin/activate 2>/dev/null && airflow version'")
    if result["returncode"] == 0:
        return True, (result["stdout"] or "").strip()
    result2 = run_command("airflow version")
    if result2["returncode"] == 0:
        return True, (result2["stdout"] or "").strip()
    return False, ""

def run_prechecks():
    commands = [
        "python3 --version",
        "pip3 --version",
        "psql --version",
        "systemctl is-active postgresql || true",
        "which airflow || true",
    ]
    results = [run_command(cmd) for cmd in commands]
    return results

def missing_packages():
    missing = []
    for pkg in REQUIRED_APT_PACKAGES:
        res = run_command(f"dpkg -s {pkg} >/dev/null 2>&1")
        if res["returncode"] != 0:
            missing.append(pkg)
    return missing
