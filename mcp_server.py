import paramiko
import time
from fastmcp import FastMCP

mcp = FastMCP("AI Agent Server")

DEFAULT_USER = "vboxuser"
DEFAULT_PASSWORD = "009"
AIRFLOW_VERSION = "2.10.2"


# ================= SSH HELPER =================
def ssh_run(host, user, password, cmd):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(host, username=user, password=password, timeout=15)

        stdin, stdout, stderr = client.exec_command(cmd, timeout=300)

        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        code = stdout.channel.recv_exit_status()

        client.close()
        return code, out, err

    except Exception as e:
        return 1, "", str(e)


# ================= INSTALL AIRFLOW =================
@mcp.tool()
def install_airflow(target_ip: str, port: int, username: str, password: str) -> str:
    host = target_ip
    user = DEFAULT_USER
    pwd = DEFAULT_PASSWORD

    # Test SSH
    code, _, err = ssh_run(host, user, pwd, "echo connected")
    if code != 0:
        return f"❌ SSH failed: {err}"

    # Kill old airflow
    ssh_run(host, user, pwd, "pkill -f airflow || true")

    # Install dependencies
    code, _, err = ssh_run(
        host,
        user,
        pwd,
        "sudo apt update && sudo apt install -y postgresql postgresql-contrib python3-venv python3-dev libpq-dev"
    )
    if code != 0:
        return f"❌ Dependency install failed: {err}"

    ssh_run(host, user, pwd, "sudo systemctl start postgresql")

    # PostgreSQL setup
    ssh_run(host, user, pwd, "sudo -u postgres psql -c \"CREATE USER airflow WITH PASSWORD 'airflow';\" || true")
    ssh_run(host, user, pwd, "sudo -u postgres psql -c \"CREATE DATABASE airflow OWNER airflow;\" || true")

    # Create venv
    ssh_run(host, user, pwd, "rm -rf ~/airflow_venv")
    code, _, err = ssh_run(host, user, pwd, "python3 -m venv ~/airflow_venv")
    if code != 0:
        return f"❌ Virtualenv failed: {err}"

    # Install Airflow
    code, _, err = ssh_run(
        host,
        user,
        pwd,
        f"~/airflow_venv/bin/pip install --upgrade pip && "
        f"~/airflow_venv/bin/pip install apache-airflow=={AIRFLOW_VERSION} psycopg2-binary "
        f"--constraint https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-3.12.txt"
    )
    if code != 0:
        return f"❌ Airflow install failed: {err}"

    # Setup Airflow
    ssh_run(host, user, pwd, "mkdir -p ~/airflow_home")

    ssh_run(
        host,
        user,
        pwd,
        "AIRFLOW_HOME=~/airflow_home "
        "AIRFLOW_DATABASE_SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow "
        "~/airflow_venv/bin/airflow db migrate"
    )

    time.sleep(2)

    # Create user
    code, _, err = ssh_run(
        host,
        user,
        pwd,
        f"AIRFLOW_HOME=~/airflow_home "
        f"~/airflow_venv/bin/airflow users create "
        f"--username {username} --password {password} "
        f"--firstname Admin --lastname User --role Admin --email admin@example.com"
    )

    if code != 0:
        return f"❌ User creation failed: {err}"

    # Start services
    ssh_run(
        host,
        user,
        pwd,
        f"setsid env AIRFLOW_HOME=~/airflow_home "
        f"~/airflow_venv/bin/airflow webserver --port {port} "
        f"> ~/web.log 2>&1 < /dev/null &"
    )

    ssh_run(
        host,
        user,
        pwd,
        "setsid env AIRFLOW_HOME=~/airflow_home "
        "~/airflow_venv/bin/airflow scheduler "
        "> ~/scheduler.log 2>&1 < /dev/null &"
    )

    time.sleep(10)

    # Verify
    code, _, _ = ssh_run(host, user, pwd, f"ss -ltn | grep :{port}")
    if code != 0:
        _, log, _ = ssh_run(host, user, pwd, "tail -20 ~/web.log")
        return f"❌ Airflow failed to start:\n{log}"

    return f"✅ Airflow running at http://{host}:{port}"


# ================= VERIFY =================
@mcp.tool()
def verify_airflow(target_ip: str, port: int) -> str:
    user = DEFAULT_USER
    pwd = DEFAULT_PASSWORD

    code, _, _ = ssh_run(target_ip, user, pwd, f"ss -ltn | grep :{port}")
    if code == 0:
        return f"✅ Airflow running at http://{target_ip}:{port}"
    return "❌ Airflow not running"


# ================= DELETE =================
@mcp.tool()
def delete_airflow(target_ip: str) -> str:
    user = DEFAULT_USER
    pwd = DEFAULT_PASSWORD

    ssh_run(target_ip, user, pwd, "pkill -f airflow || true")
    time.sleep(2)

    ssh_run(target_ip, user, pwd, "rm -rf ~/airflow_venv ~/airflow_home ~/web.log ~/scheduler.log")
    ssh_run(target_ip, user, pwd, "sudo -u postgres psql -c \"DROP DATABASE IF EXISTS airflow;\"")
    ssh_run(target_ip, user, pwd, "sudo -u postgres psql -c \"DROP USER IF EXISTS airflow;\"")

    return "✅ Airflow deleted"


# ================= RUN SERVER =================
if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
