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

AGENT_DIR = Path(__file__).parent / "-hadoop-agent--main"

def run_phase(phase_module, phase_func, cluster):
    code = f"""
import sys
sys.path.insert(0, r'{AGENT_DIR}')
from agent.phases.{phase_module} import {phase_func}
import json
result = {phase_func}({cluster!r})
print(json.dumps({{"success": bool(result)}}))
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600)
    return proc.stdout.strip() or proc.stderr.strip() or "(no output)"

def hadoop_full_install_docker():
    code = f"""
import sys
sys.path.insert(0, r'{AGENT_DIR}')
from agent.phases.phase0_docker import run_phase0
from agent.phases.phase2_prereqs import run_phase2
from agent.phases.phase3_ssh import run_phase3
from agent.phases.phase4_install import run_phase4
from agent.phases.phase5_master_config import run_phase5
from agent.phases.phase6_worker_config import run_phase6
from agent.phases.phase7_start import run_phase7
import json
log = []
cluster = run_phase0()
if not cluster:
    print(json.dumps({{"success": False, "step": "docker_setup", "log": log}}))
    sys.exit(0)
log.append("phase0_docker: OK")
for phase_name, phase_fn in [("phase2_prereqs", run_phase2), ("phase3_ssh", run_phase3), ("phase4_install", run_phase4), ("phase5_master", run_phase5)]:
    ok = phase_fn(cluster)
    log.append(f"{{phase_name}}: {{'OK' if ok else 'FAILED'}}")
    if not ok:
        print(json.dumps({{"success": False, "step": phase_name, "log": log}}))
        sys.exit(0)
run_phase6(cluster)
log.append("phase6_workers: OK")
run_phase7(cluster)
log.append("phase7_start: OK")
master_ip = cluster["master"]["ip"]
print(json.dumps({{"success": True, "log": log, "urls": {{"hdfs_ui": "http://localhost:9870", "yarn_ui": "http://localhost:8088"}}, "cluster_summary": {{"master": master_ip, "workers": len(cluster["workers"])}}}}))
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=1200)
    return proc.stdout.strip() or proc.stderr.strip() or "(no output)"

def hadoop_full_install_real(cluster):
    code = f"""
import sys
sys.path.insert(0, r'{AGENT_DIR}')
from agent.phases.phase2_prereqs import run_phase2
from agent.phases.phase3_ssh import run_phase3
from agent.phases.phase4_install import run_phase4
from agent.phases.phase5_master_config import run_phase5
from agent.phases.phase6_worker_config import run_phase6
from agent.phases.phase7_start import run_phase7
import json
cluster = {cluster!r}
log = []
for phase_name, phase_fn in [("phase2_prereqs", run_phase2), ("phase3_ssh", run_phase3), ("phase4_install", run_phase4), ("phase5_master", run_phase5)]:
    ok = phase_fn(cluster)
    log.append(f"{{phase_name}}: {{'OK' if ok else 'FAILED'}}")
    if not ok:
        print(json.dumps({{"success": False, "step": phase_name, "log": log}}))
        sys.exit(0)
run_phase6(cluster)
log.append("phase6_workers: OK")
run_phase7(cluster)
log.append("phase7_start: OK")
master_ip = cluster["master"]["ip"]
print(json.dumps({{"success": True, "log": log, "urls": {{"hdfs_ui": f"http://{{master_ip}}:9870", "yarn_ui": f"http://{{master_ip}}:8088"}}, "cluster_summary": {{"master": master_ip, "workers": len(cluster["workers"])}}}}))
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=1200)
    return proc.stdout.strip() or proc.stderr.strip() or "(no output)"

TOOLS = [
    {"name": "hadoop_install_docker", "description": "Install Hadoop 3.4.2 cluster on a single machine using Docker containers.", "inputSchema": {"type": "object", "properties": {"worker_count": {"type": "integer"}}, "required": []}},
    {"name": "hadoop_install_real_machines", "description": "Install Hadoop on real machines or VMs via SSH.", "inputSchema": {"type": "object", "properties": {"master_ip": {"type": "string"}, "master_username": {"type": "string"}, "master_password": {"type": "string"}, "workers": {"type": "array", "items": {"type": "object", "properties": {"ip": {"type": "string"}, "username": {"type": "string"}, "password": {"type": "string"}}}}}, "required": ["master_ip", "master_username", "master_password", "workers"]}},
    {"name": "hadoop_check_prerequisites", "description": "Check prerequisites on all cluster nodes.", "inputSchema": {"type": "object", "properties": {"master_ip": {"type": "string"}, "master_username": {"type": "string"}, "master_password": {"type": "string"}, "workers": {"type": "array", "items": {"type": "object"}}}, "required": ["master_ip", "master_username", "master_password", "workers"]}}
]

def handle(req):
    method = req.get("method")
    req_id = req.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "hadoop-agent-mcp", "version": "1.0.0"}}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        name = req["params"]["name"]
        args = req["params"].get("arguments", {})
        if name == "hadoop_install_docker":
            result = hadoop_full_install_docker()
        elif name == "hadoop_install_real_machines":
            cluster = {"master": {"ip": args["master_ip"], "username": args["master_username"], "password": args["master_password"]}, "workers": [{"id": i+1, "ip": w["ip"], "username": w["username"], "password": w["password"]} for i, w in enumerate(args.get("workers", []))]}
            result = hadoop_full_install_real(cluster)
        elif name == "hadoop_check_prerequisites":
            cluster = {"master": {"ip": args["master_ip"], "username": args["master_username"], "password": args["master_password"]}, "workers": [{"id": i+1, "ip": w["ip"], "username": w["username"], "password": w["password"]} for i, w in enumerate(args.get("workers", []))]}
            result = run_phase("phase2_prereqs", "run_phase2", cluster)
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
