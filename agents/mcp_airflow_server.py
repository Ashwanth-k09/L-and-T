#!/usr/bin/env python3
import sys
import subprocess
import json
from pathlib import Path

def send(obj):
    line = json.dumps(obj)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()

def recv():
    line = sys.stdin.readline()
    if not line:
        return None
    return json.loads(line.strip())

AGENT_DIR = Path(__file__).parent / "airflow_cmd_agent-main"

def run_checks():
    code = f"""
import sys
sys.path.insert(0, r'{AGENT_DIR}')
from checks import run_prechecks, check_airflow_installed, missing_packages
results = run_prechecks()
installed, version = check_airflow_installed()
pkgs = missing_packages()
import json
print(json.dumps({{"prechecks": results, "airflow_installed": installed, "airflow_version": version, "missing_packages": pkgs}}))
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    return proc.stdout.strip() or proc.stderr.strip() or "(no output)"

def run_install_airflow(port, username, password):
    code = f"""
import sys
sys.path.insert(0, r'{AGENT_DIR}')
from installer import install_missing_apt, ensure_postgres, create_venv, install_airflow
from checks import missing_packages
from configurator import configure_airflow, start_airflow
import json
log = []
pkgs = missing_packages()
if pkgs:
    r = install_missing_apt(pkgs)
    log.append({{"step": "install_apt", "result": r}})
    if r["returncode"] != 0:
        print(json.dumps({{"success": False, "log": log}}))
        sys.exit(0)
r = ensure_postgres()
log.append({{"step": "ensure_postgres", "result": r}})
if r["returncode"] != 0:
    print(json.dumps({{"success": False, "log": log}}))
    sys.exit(0)
r = create_venv()
log.append({{"step": "create_venv", "result": r}})
if r["returncode"] != 0:
    print(json.dumps({{"success": False, "log": log}}))
    sys.exit(0)
r = install_airflow()
log.append({{"step": "install_airflow", "result": r}})
if r["returncode"] != 0:
    print(json.dumps({{"success": False, "log": log}}))
    sys.exit(0)
r = configure_airflow({repr(username)}, {repr(password)})
log.append({{"step": "configure_airflow", "result": r}})
if r["returncode"] != 0:
    print(json.dumps({{"success": False, "log": log}}))
    sys.exit(0)
r = start_airflow({repr(port)})
log.append({{"step": "start_airflow", "result": r}})
print(json.dumps({{"success": r["returncode"] == 0, "log": log}}))
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600)
    return proc.stdout.strip() or proc.stderr.strip() or "(no output)"

def verify_airflow(port):
    code = f"""
import sys
sys.path.insert(0, r'{AGENT_DIR}')
from verifier import verify_airflow
import json
ok, results = verify_airflow({repr(port)})
print(json.dumps({{"ok": ok, "results": results}}))
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    return proc.stdout.strip() or proc.stderr.strip() or "(no output)"

TOOLS = [
    {"name": "airflow_check_system", "description": "Check system status for Airflow: prechecks, installed version, missing packages.", "inputSchema": {"type": "object", "properties": {}, "required": []}},
    {"name": "airflow_install", "description": "Install and configure Apache Airflow on this machine.", "inputSchema": {"type": "object", "properties": {"port": {"type": "string", "description": "Port for Airflow webserver (default: 8080)"}, "username": {"type": "string"}, "password": {"type": "string"}}, "required": ["username", "password"]}},
    {"name": "airflow_verify", "description": "Verify Airflow is running correctly.", "inputSchema": {"type": "object", "properties": {"port": {"type": "string"}}, "required": []}}
]

def handle(req):
    method = req.get("method")
    req_id = req.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "airflow-agent-mcp", "version": "1.0.0"}}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        name = req["params"]["name"]
        args = req["params"].get("arguments", {})
        if name == "airflow_check_system":
            result = run_checks()
        elif name == "airflow_install":
            result = run_install_airflow(args.get("port", "8080"), args["username"], args["password"])
        elif name == "airflow_verify":
            result = verify_airflow(args.get("port", "8080"))
        else:
            result = f"Unknown tool: {name}"
        return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "text", "text": result}]}}
    if method == "notifications/initialized":
        return None
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}

def main():
    while True:
        req = recv()
        if req is None:
            break
        resp = handle(req)
        if resp is not None:
            send(resp)

if __name__ == "__main__":
    main()
