import subprocess
from datetime import datetime
from pathlib import Path

LOG_DIR = Path.home() / "airflow_cmd_agent" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "agent.log"

def run_command(cmd: str):
    result = subprocess.run(
        cmd,
        shell=True,
        executable="/bin/bash",
        text=True,
        capture_output=True
    )

    with open(LOG_FILE, "a") as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"{datetime.now().isoformat()}\n")
        f.write(f"CMD: {cmd}\n")
        f.write(f"RETURN CODE: {result.returncode}\n")
        f.write("\nSTDOUT:\n")
        f.write(result.stdout or "")
        f.write("\nSTDERR:\n")
        f.write(result.stderr or "")
        f.write("\n")

    return {
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
